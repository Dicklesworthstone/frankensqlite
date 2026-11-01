"""Protocol/fixture tests; these do not claim FrankenSQLite SQL qualification."""

import copy
import contextlib
import io
import json
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import benchmark_issue_531 as bench


def row_memory_cli(work, values):
    """A real child verifies the supplied database and emits multirow results."""
    path = work / "row-memory-cli"
    database = work / "existing.db"
    path.write_text(f"""#!{sys.executable}
import sys
assert sys.argv[-1] == {str(database)!r}, 'wrong database'
for line in sys.stdin:
    if line.startswith('.print '):
        print(line.split(maxsplit=1)[1].strip())
    elif line.startswith('SELECT '):
        for value in {values!r}:
            print(value)
""", encoding="utf-8")
    path.chmod(0o700)
    return {"path": str(path)}, database


class Issue531ProtocolTests(unittest.TestCase):
    def result(self, executions, timings=(0.01, 0.02), stderr=""):
        output = []
        for index, values in enumerate(executions):
            output.extend([f"marker_{index}", *values])
        output.append("marker_end")
        timers = "".join(f"Run Time: real {value:.6f} user 0.000001 sys 0.000000\n" for value in timings)
        return subprocess.CompletedProcess(["protocol-fixture"], 0, "\n".join(output) + "\n", timers + stderr)

    def test_every_warmup_and_measurement_is_checked(self):
        expected = ["1,'one'", "2,'two'"]
        case = bench.Case("SELECT ...", True)
        sample = bench.validate_sample(self.result([expected] * 3), expected, case, 1, 2, "marker_")
        self.assertEqual(sample["warmups_checked"], 1)
        self.assertEqual(sample["measured_results_checked"], 2)
        self.assertAlmostEqual(sample["seconds"], 0.015)
        for bad_index in range(3):
            executions = [expected.copy() for _ in range(3)]
            executions[bad_index][0] = "0,'wrong'"
            with self.assertRaisesRegex(ValueError, "result mismatch"):
                bench.validate_sample(self.result(executions), expected, case, 1, 2, "marker_")

    def test_ordered_rows_are_not_normalized(self):
        expected = ["1", "2"]
        with self.assertRaisesRegex(ValueError, "result mismatch"):
            bench.validate_sample(self.result([expected, expected[::-1], expected]), expected,
                                  bench.Case("SELECT ... ORDER BY x", True), 1, 2, "marker_")

    def test_unordered_comparison_preserves_duplicate_multiplicity(self):
        expected = ["1", "1", "2"]
        bench.validate_sample(self.result([expected[::-1]] * 3), expected,
                              bench.Case("SELECT ...", False), 1, 2, "marker_")
        with self.assertRaisesRegex(ValueError, "result mismatch"):
            bench.validate_sample(self.result([["1", "2", "2"]] * 3), expected,
                                  bench.Case("SELECT ...", False), 1, 2, "marker_")

    def test_empty_results_still_require_all_markers(self):
        bench.validate_sample(self.result([[], [], []]), [], bench.Case("SELECT ...", False), 1, 2, "marker_")
        broken = self.result([[], [], []])
        broken.stdout = broken.stdout.replace("marker_1\n", "")
        with self.assertRaisesRegex(ValueError, "missing result marker"):
            bench.validate_sample(broken, [], bench.Case("SELECT ...", False), 1, 2, "marker_")

    def test_stderr_and_missing_timers_cannot_pass(self):
        case = bench.Case("SELECT ...", True)
        for result in (self.result([["1"]] * 3, stderr="Error: failed query\n"),
                       self.result([["1"]] * 3, timings=(0.01,))):
            with self.assertRaises(ValueError):
                bench.validate_sample(result, ["1"], case, 1, 2, "marker_")

    def test_zero_resolution_timers_produce_no_candidate_speedup(self):
        value = bench.validate_sample(self.result([["1"]], timings=(0.0,)), ["1"],
                                      bench.Case("SELECT ...", True), 0, 1, "marker_")
        self.assertTrue(value["below_timer_resolution"])
        result = bench.summarize([{"engine": "baseline", "seconds": 1.0},
                                  {"engine": "candidate", "seconds": 0.0},
                                  {"engine": "stock", "seconds": 0.01}])
        self.assertTrue(result["unresolved_timer_resolution"])
        self.assertNotIn("baseline_over_candidate", result["ratios"])
        self.assertNotIn("candidate_over_stock", result["ratios"])

    def test_oracle_only_summary_has_no_comparison_labels(self):
        result = bench.summarize([{"engine": "stock", "seconds": 0.0001}])
        self.assertEqual(set(result["engines"]), {"stock"})
        self.assertEqual(result["ratios"], {})

    def test_fixture_counts_are_independent_of_insert_attempts(self):
        _, default_count = bench.fixture_sql(50_000, 500_000, "cli")
        self.assertEqual(default_count, 500_000)
        for mode, expected in (("cli", 12), ("four-per-post", 336)):
            # The 84-post scale shares a divisor with 16807; this catches an
            # incorrect assumption that every scale produces one like per post.
            sql, count = bench.fixture_sql(12, 84, mode)
            self.assertEqual(count, expected)
            connection = sqlite3.connect(":memory:")
            try:
                connection.executescript(sql)
                actual = connection.execute("SELECT (SELECT count(*) FROM users), (SELECT count(*) FROM posts), (SELECT count(*) FROM likes)").fetchone()
                self.assertEqual(actual, (12, 84, count))
                if mode == "four-per-post":
                    self.assertEqual(connection.execute("SELECT min(n), max(n) FROM (SELECT count(*) n FROM likes GROUP BY post_id)").fetchone(), (4, 4))
            finally:
                connection.close()

    def test_reported_queries_keep_ordering_and_limits(self):
        cases = bench.cases(500_000)
        self.assertIn("post_id = 400000 GROUP BY post_id", cases["group_eq"].sql)
        self.assertIn("p.created_at DESC, p.id DESC LIMIT 20", cases["join_correlated_newest20"].sql)
        self.assertIn("WHERE l.post_id = p.id", cases["join_correlated_newest20"].sql)
        self.assertTrue(cases["join_correlated_newest20"].ordered)
        self.assertFalse(cases["group_eq"].ordered)
        self.assertFalse(cases["join_filtered_offset_control"].reported_shape)

    def test_reported_cli_wrappers_keep_the_inner_query(self):
        connection = sqlite3.connect(":memory:")
        try:
            connection.executescript(bench.fixture_sql(10, 100, "cli")[0])
            for name in ("newest20_count_wrapper", "join_newest20_count_wrapper"):
                case = bench.cases(100)[name]
                self.assertTrue(case.sql.startswith("SELECT count(*) FROM (SELECT p.id"))
                self.assertIn("ORDER BY p.created_at DESC, p.id DESC LIMIT 20)", case.sql)
                self.assertEqual(connection.execute(case.sql).fetchall(), [(20,)])
                self.assertFalse(case.ordered)
        finally:
            connection.close()

    def test_build_manifest_requires_exact_binary_and_dependency_identity(self):
        source = {"Cargo.lock": "a" * 64, "crates/fsqlite-core/src/connection.rs": "b" * 64,
                  "crates/fsqlite-vdbe/src/codegen.rs": "c" * 64}
        builds = {engine: {"source_revision": engine, "source_sha256": source.copy(),
                           "binary_sha256": digit * 64, "command": ["cargo", "build"],
                           "toolchain": "pinned", "profile": "release-perf", "features": ["default"],
                           "rustflags": []}
                  for engine, digit in (("baseline", "1"), ("candidate", "2"))}
        binaries = {engine: {"sha256": digit * 64}
                    for engine, digit in (("baseline", "1"), ("candidate", "2"), ("stock", "3"))}
        bench.validate_builds(builds, binaries)
        for key, replacement in (("binary_sha256", "9" * 64), ("toolchain", "different"),
                                 ("profile", "different"), ("features", ["changed"]),
                                 ("rustflags", ["-Ctarget-cpu=native"])):
            changed = copy.deepcopy(builds)
            changed["candidate"][key] = replacement
            with self.assertRaises(ValueError):
                bench.validate_builds(changed, binaries)
        changed = copy.deepcopy(builds)
        changed["candidate"]["source_sha256"]["Cargo.lock"] = "d" * 64
        with self.assertRaisesRegex(ValueError, "dependency locks"):
            bench.validate_builds(changed, binaries)

    @unittest.skipUnless(sys.platform.startswith("linux"), "Linux /proc memory protocol")
    def test_memory_collector_checks_multirow_results_in_same_live_process(self):
        work = Path(tempfile.mkdtemp(prefix="issue531-memory-rows-"))
        expected = ["1,'one'", "1,'one'", "2,'two'"]
        binary, database = row_memory_cli(work, expected)
        sample = bench.run_memory_cli(
            binary, "SELECT id, name FROM items;", expected, [".mode quote"],
            3, work / "init", work, 5, "rows", database=database)
        self.assertEqual(sample["queries"], 3)
        self.assertEqual([point["query_count"] for point in sample["checkpoints"]], [0, 1, 2, 3])
        self.assertTrue(all(point["rss_kib"] > 0 and point["hwm_kib"] > 0
                            for point in sample["checkpoints"]))
        receipt = json.loads((work / "rows.checkpoints.json").read_text())
        self.assertEqual(receipt["status"], "ok")
        self.assertEqual(receipt["pid"], sample["pid"])
        self.assertEqual(receipt["checkpoints"], sample["checkpoints"])
        self.assertNotIn("seconds", sample)

    @unittest.skipUnless(sys.platform.startswith("linux"), "Linux /proc memory protocol")
    def test_memory_collector_preserves_order_and_duplicate_multiplicity(self):
        expected = ["1,'one'", "1,'one'", "2,'two'"]
        for values, ordered, succeeds in (
                (expected[::-1], False, True), (expected[::-1], True, False),
                (["1,'one'", "2,'two'", "2,'two'"], False, False),
                ([], False, False), (expected + ["3,'extra'"], False, False)):
            work = Path(tempfile.mkdtemp(prefix="issue531-memory-order-"))
            binary, database = row_memory_cli(work, values)
            with self.subTest(values=values, ordered=ordered):
                arguments = (binary, "SELECT id, name FROM items;", expected, [],
                             2, work / "init", work, 5, "result")
                if succeeds:
                    bench.run_memory_cli(*arguments, database=database, ordered=ordered)
                else:
                    with self.assertRaisesRegex(ValueError, "wrong memory result"):
                        bench.run_memory_cli(*arguments, database=database, ordered=ordered)
                    receipt = json.loads((work / "result.checkpoints.json").read_text())
                    self.assertEqual(receipt["status"], "incomplete")
                    self.assertEqual(len(receipt["checkpoints"]), 1)

    @unittest.skipUnless(sys.platform.startswith("linux"), "Linux /proc memory protocol")
    def test_memory_empty_result_is_still_sampled_and_checked(self):
        work = Path(tempfile.mkdtemp(prefix="issue531-memory-empty-"))
        binary, database = row_memory_cli(work, [])
        sample = bench.run_memory_cli(binary, "SELECT 1 WHERE 0;", [], [],
                                     2, work / "init", work, 5, "empty", database=database)
        self.assertEqual(len(sample["checkpoints"]), 3)

    def test_memory_cli_rejects_timing_gates_and_invalid_query_counts(self):
        work = Path(tempfile.mkdtemp(prefix="issue531-memory-options-"))
        for arguments in (["--memory-queries", "2"], ["--memory", "--memory-queries", "0"],
                          ["--memory", "--max-candidate-over-stock", "10"],
                          ["--memory", "--warmups", "2"], ["--memory", "--iterations", "2"]):
            argv = ["benchmark", "--oracle-only", "--sqlite", "unused",
                    "--work-dir", str(work), *arguments]
            with self.subTest(arguments=arguments), mock.patch.object(sys, "argv", argv), \
                    mock.patch.object(bench, "run_cli") as execute, \
                    contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as error:
                bench.main()
            self.assertEqual(error.exception.code, 2)
            execute.assert_not_called()

    @unittest.skipUnless(sys.platform.startswith("linux"), "Linux /proc memory protocol")
    def test_memory_driver_has_no_timing_claim_and_preserves_collector_failures(self):
        for fails in (False, True):
            work = Path(tempfile.mkdtemp(prefix="issue531-memory-driver-"))
            output = work / "result.json"
            argv = ["benchmark", "--oracle-only", "--sqlite", "unused", "--memory",
                    "--memory-queries", "2", "--trials", "1", "--case", "group_eq",
                    "--users", "10", "--posts", "100", "--work-dir", str(work),
                    "--output", str(output)]

            def run_cli(_binary, database, _sql, _init, _directory, stem, _timeout):
                if stem == "seed":
                    Path(database).write_bytes(b"protocol fixture")
                    values = ["off", "wal"]
                elif stem.startswith("verify-"):
                    values = ["'ok'", "10,100,100"]
                elif stem == "stock-identity":
                    values = ["'stock','source'"]
                elif stem.endswith("-oracle"):
                    values = ["80,1"]
                else:
                    values = ["SEARCH likes USING INDEX likes_post_id_idx"]
                return subprocess.CompletedProcess(["protocol"], 0, "\n".join(values) + "\n", "")

            measurement = {"pid": 1234, "queries": 2, "checkpoints": [
                {"query_count": 0, "rss_kib": 100, "hwm_kib": 150},
                {"query_count": 1, "rss_kib": 200, "hwm_kib": 350},
                {"query_count": 2, "rss_kib": 175, "hwm_kib": 350},
            ]}
            with self.subTest(fails=fails), mock.patch.object(sys, "argv", argv), \
                    mock.patch.object(bench, "binary_info", return_value={"path": "unused"}), \
                    mock.patch.object(bench, "run_cli", side_effect=run_cli), \
                    mock.patch.object(bench, "run_memory_cli", return_value=measurement,
                                      side_effect=ValueError("missing /proc child") if fails else None) as collect, \
                    mock.patch.object(bench, "timing_script") as timers, \
                    contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(bench.main(), 1 if fails else 0)
            timers.assert_not_called()
            self.assertEqual(collect.call_count, 1)
            self.assertEqual(collect.call_args.kwargs["ordered"], False)
            self.assertTrue(str(collect.call_args.kwargs["database"]).endswith("stock.db"))
            report = json.loads(output.read_text())
            self.assertEqual(report["measurement_mode"], "memory")
            self.assertEqual(report["mode"], "oracle_only")
            for key in ("warmups_per_sample", "iterations_per_sample", "ratio_limits"):
                self.assertNotIn(key, report)
            case = report["cases"]["group_eq"]
            if fails:
                self.assertEqual(report["status"], "failed")
                self.assertEqual(case["samples"], [])
                self.assertIsNone(case["summary"])
                self.assertIn("missing /proc child", case["errors"][0]["message"])
            else:
                self.assertEqual(report["status"], "passed")
                self.assertEqual(case["samples"][0]["measured_results_checked"], 2)
                self.assertNotIn("seconds", case["samples"][0])
                self.assertNotIn("ratios", case["summary"])
                summary = case["summary"]["engines"]["stock"]
                self.assertEqual(summary["median_first_query_hwm_kib"], 350)
                self.assertEqual(summary["median_final_minus_setup_rss_kib"], 75)


if __name__ == "__main__":
    unittest.main()
