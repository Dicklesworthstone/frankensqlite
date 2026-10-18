"""Tests for the GH#518 runner, NOT evidence about FrankenSQLite memory use.

Run: python3 -m unittest discover -s scripts/tests -p 'test_verify_wal_commit_memory.py' -v
The subprocess fixtures are explicitly synthetic. Real SQLite files exercise
independent readback, while injected numbers exercise the comparison policy.
Test artifacts are retained rather than deleted.
"""
from __future__ import annotations

import contextlib
import copy
import importlib.util
import io
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest
from unittest import mock

MODULE_PATH = Path(__file__).resolve().parents[1] / "verify_wal_commit_memory.py"
SPEC = importlib.util.spec_from_file_location("verify_wal_commit_memory", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
runner = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(runner)


def checkpoint(phase, rows=0, payload_bytes=0, elapsed_ms=0, rss_kib=100, hwm_kib=200):
    return dict(phase=phase, rows=rows, payload_bytes=payload_bytes, elapsed_ms=elapsed_ms,
                rss_kib=rss_kib, hwm_kib=hwm_kib)


def writer_records(total=2, transaction=2, row=32):
    total_rows, tx_rows, row_bytes = runner.dimensions(total, transaction, row)
    records = [checkpoint("start")]
    for first in range(0, total_rows, tx_rows):
        last = min(first + tx_rows, total_rows)
        payload = (last - first) * row_bytes
        records += [checkpoint("commit_begin", last, payload, len(records)),
                    checkpoint("commit_end", last, payload, len(records) + 1, rss_kib=150)]
    records += [checkpoint("all_transactions_committed", total_rows, total * runner.MIB, len(records)),
                checkpoint("closed_without_checkpoint", total_rows, total * runner.MIB, len(records) + 1, rss_kib=80)]
    return records


def comparison_runs(repetitions=3):
    return [dict(implementation=implementation, case=case, repetition=repetition,
                 fnv1a64="0000000000000518", stock_readback={"sha256": "oracle"},
                 metrics=dict(peak_rss_kib=1000 if implementation == "baseline" or case == "small" else 600,
                              retained_after_close_kib=500 if implementation == "baseline" else 250,
                              max_commit_endpoint_delta_kib=100, commit_elapsed_ms=10, writer_elapsed_ms=20))
            for repetition in range(repetitions) for implementation in ("baseline", "candidate")
            for case in ("large", "small")]


class ProtocolTests(unittest.TestCase):
    def test_single_transaction_and_sampled_peak(self):
        metrics = runner.writer_metrics(writer_records(), 2, 2, 32, 250)
        self.assertEqual(metrics["transactions"], 1)
        self.assertEqual(metrics["peak_rss_kib"], 250)
        self.assertEqual(metrics["retained_after_close_kib"], 80)
        self.assertEqual(metrics["max_commit_endpoint_delta_kib"], 50)
        self.assertEqual(metrics["commit_elapsed_ms"], 1)

    def test_split_transactions_are_not_misidentified_as_one(self):
        metrics = runner.writer_metrics(writer_records(transaction=1), 2, 1, 32)
        self.assertEqual(metrics["transactions"], 2)
        with self.assertRaises(runner.VerificationError):
            runner.writer_metrics(writer_records(), 2, 1, 32)

    def test_final_short_transaction(self):
        metrics = runner.writer_metrics(writer_records(total=3, transaction=2), 3, 2, 32)
        self.assertEqual([item["payload_bytes"] for item in metrics["commits"]], [2 * runner.MIB, runner.MIB])

    def test_missing_start_close_or_commit_end_fails(self):
        valid = writer_records()
        for invalid in ([], valid[1:], valid[:-1], [record for record in valid if record["phase"] != "commit_end"]):
            with self.subTest(invalid=invalid), self.assertRaises(runner.VerificationError):
                runner.writer_metrics(invalid, 2, 2, 32)

    def test_duplicated_and_unknown_phases_fail(self):
        for index in range(len(writer_records())):
            records = writer_records()
            records.insert(index, records[index].copy())
            with self.subTest(index=index), self.assertRaises(runner.VerificationError):
                runner.writer_metrics(records, 2, 2, 32)
        records = writer_records()
        records[1]["phase"] = "ignored_commit_error"
        with self.assertRaises(runner.VerificationError):
            runner.writer_metrics(records, 2, 2, 32)

    def test_incorrect_totals_fail(self):
        for index in range(1, len(writer_records())):
            for field in ("rows", "payload_bytes"):
                records = writer_records()
                records[index][field] += 1
                with self.subTest(index=index, field=field), self.assertRaises(runner.VerificationError):
                    runner.writer_metrics(records, 2, 2, 32)

    def test_non_monotonic_elapsed_time_fails(self):
        records = writer_records()
        records[-1]["elapsed_ms"] = 0
        with self.assertRaises(runner.VerificationError):
            runner.writer_metrics(records, 2, 2, 32)

    def test_missing_zero_negative_boolean_and_string_memory_fail(self):
        for value in (None, 0, -1, True, "100"):
            for field in ("rss_kib", "hwm_kib"):
                records = writer_records()
                records[1][field] = value
                with self.subTest(value=value, field=field), self.assertRaises(runner.VerificationError):
                    runner.writer_metrics(records, 2, 2, 32)

    def test_negative_endpoint_delta_is_preserved(self):
        records = writer_records()
        records[2]["rss_kib"] = 50
        self.assertEqual(runner.writer_metrics(records, 2, 2, 32)["max_commit_endpoint_delta_kib"], -50)

    def test_inserting_checkpoints_validate_cumulative_payload(self):
        records = writer_records()
        records.insert(1, checkpoint("inserting", 32, runner.MIB))
        self.assertEqual(runner.writer_metrics(records, 2, 2, 32)["transactions"], 1)
        records[1]["payload_bytes"] = 1
        with self.assertRaises(runner.VerificationError):
            runner.writer_metrics(records, 2, 2, 32)

    def test_inserting_cannot_run_past_commit_boundary(self):
        records = writer_records()
        records.insert(2, checkpoint("inserting", 32, runner.MIB, elapsed_ms=1))
        with self.assertRaises(runner.VerificationError):
            runner.writer_metrics(records, 2, 2, 32)

    def test_inserting_cannot_skip_a_split_transaction_boundary(self):
        records = writer_records(transaction=1)
        records.insert(1, checkpoint("inserting", 64, 2 * runner.MIB))
        with self.assertRaises(runner.VerificationError):
            runner.writer_metrics(records, 2, 1, 32)

    def test_workload_dimensions_fail_closed(self):
        for values in ((0, 1, 32), (1, -1, 32), (1, 1, 0), (1, 1, 3), (True, 1, 32)):
            with self.subTest(values=values), self.assertRaises(runner.VerificationError):
                runner.dimensions(*values)

    def test_fresh_process_verifier_protocol(self):
        records = [checkpoint("start"), dict(phase="all_bytes_verified", rows=64,
                   payload_bytes=2 * runner.MIB, fnv1a64="0123456789abcdef")]
        self.assertEqual(runner.verifier_digest(records, 2, 32), "0123456789abcdef")
        for field, value in (("rows", 63), ("payload_bytes", 0), ("phase", "partial"),
                             ("fnv1a64", "not a digest"), ("fnv1a64", "F" * 16)):
            invalid = copy.deepcopy(records)
            invalid[1][field] = value
            with self.subTest(field=field), self.assertRaises(runner.VerificationError):
                runner.verifier_digest(invalid, 2, 32)
        with self.assertRaises(runner.VerificationError):
            runner.verifier_digest(records[1:], 2, 32)


class ComparisonTests(unittest.TestCase):
    def test_complete_repeated_comparison_passes(self):
        result = runner.compare_runs(comparison_runs(), 3, 0.75, 0.75, 1.10)
        self.assertTrue(result["passed"])
        self.assertEqual(result["gates"][0]["candidate_over_baseline"], 0.6)

    def test_medians_not_best_run_choose_the_result(self):
        runs = comparison_runs()
        for run in runs:
            if run["implementation"] == "candidate" and run["case"] == "large" and run["repetition"] != 0:
                run["metrics"]["peak_rss_kib"] = 2000
        self.assertFalse(runner.compare_runs(runs, 3, 0.75, 0.75, 1.10)["passed"])

    def test_small_transaction_regression_fails(self):
        runs = comparison_runs()
        for run in runs:
            if run["implementation"] == "candidate" and run["case"] == "small":
                run["metrics"]["peak_rss_kib"] = 2000
        self.assertFalse(runner.compare_runs(runs, 3, 0.75, 0.75, 1.10)["passed"])

    def test_retained_memory_regression_fails(self):
        runs = comparison_runs()
        for run in runs:
            if run["implementation"] == "candidate":
                run["metrics"]["retained_after_close_kib"] = 1000
        self.assertFalse(runner.compare_runs(runs, 3, 0.75, 0.75, 1.10)["passed"])

    def test_incomplete_and_duplicate_runs_fail(self):
        runs = comparison_runs()
        for invalid in (runs[:-1], runs + [runs[0]], runs[1:] + [runs[1]]):
            with self.subTest(length=len(invalid)), self.assertRaises(runner.VerificationError):
                runner.compare_runs(invalid, 3, 0.75, 0.75, 1.10)

    def test_digest_mismatches_fail(self):
        for digest_kind in ("native", "stock"):
            runs = comparison_runs()
            if digest_kind == "native":
                runs[-1]["fnv1a64"] = "different"
            else:
                runs[-1]["stock_readback"]["sha256"] = "different"
            with self.subTest(kind=digest_kind), self.assertRaises(runner.VerificationError):
                runner.compare_runs(runs, 3, 0.75, 0.75, 1.10)

    def test_missing_memory_or_non_finite_thresholds_fail(self):
        for value in (float("nan"), float("inf"), 0, -1):
            with self.subTest(value=value), self.assertRaises(runner.VerificationError):
                runner.compare_runs(comparison_runs(), 3, value, 0.75, 1.10)
        runs = comparison_runs()
        runs[0]["metrics"]["peak_rss_kib"] = 0
        with self.assertRaises(runner.VerificationError):
            runner.compare_runs(runs, 3, 0.75, 0.75, 1.10)


class FilesAndProcessTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.directory = Path(tempfile.mkdtemp(prefix="gh518-runner-selftest-"))

    def fixture(self, name):
        path = self.directory / self.id().split(".")[-1] / name
        path.parent.mkdir(parents=True, exist_ok=True)
        return path

    def database(self):
        path = self.fixture("records.sqlite")
        connection = sqlite3.connect(path)
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA wal_autocheckpoint=0")
        connection.execute("CREATE TABLE records(id INTEGER PRIMARY KEY,payload BLOB NOT NULL)")
        connection.commit()
        connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        payload = bytes(index % 251 for index in range(32 * runner.KIB))
        connection.executemany("INSERT INTO records VALUES(?,?)", ((index, payload) for index in range(1, 33)))
        connection.commit()
        self.addCleanup(connection.close)
        return path, connection

    def test_stock_oracle_reads_uncheckpointed_wal_without_mutation(self):
        path, _ = self.database()
        wal = Path(str(path) + "-wal")
        before = (path.read_bytes(), wal.read_bytes())
        self.assertGreater(len(before[1]), 32)
        self.assertEqual(runner.stock_readback(path, 1, 32)["rows"], 32)
        self.assertEqual(before, (path.read_bytes(), wal.read_bytes()))

    def test_stock_oracle_rejects_incorrect_payload(self):
        path, connection = self.database()
        connection.execute("UPDATE records SET payload=? WHERE id=7", (b"wrong",))
        connection.commit()
        with self.assertRaises(runner.VerificationError):
            runner.stock_readback(path, 1, 32)

    def test_stock_oracle_rejects_missing_row(self):
        path, connection = self.database()
        connection.execute("DELETE FROM records WHERE id=32")
        connection.commit()
        with self.assertRaises(runner.VerificationError):
            runner.stock_readback(path, 1, 32)

    def test_stock_oracle_rejects_wrong_id_with_correct_row_count(self):
        path, connection = self.database()
        connection.execute("UPDATE records SET id=33 WHERE id=32")
        connection.commit()
        with self.assertRaises(runner.VerificationError):
            runner.stock_readback(path, 1, 32)

    def test_malformed_json_and_array_records_fail(self):
        for index, text in enumerate(("not-json\n", "[]\n")):
            path = self.fixture(f"invalid-{index}.jsonl")
            path.write_text(text)
            with self.assertRaises(runner.VerificationError):
                runner.records_from(path)

    def test_nonzero_process_exit_is_not_a_measurement(self):
        directory = self.fixture("placeholder").parent
        with self.assertRaises(runner.VerificationError):
            runner.run_process([sys.executable, "-c", "raise SystemExit(7)"], directory, "bad", 5)
        self.assertTrue((directory / "bad.stderr.log").is_file())

    def test_timeout_reaps_child_and_preserves_logs(self):
        directory = self.fixture("placeholder").parent
        children = []
        real_popen = runner.subprocess.Popen

        def capture_child(*args, **kwargs):
            child = real_popen(*args, **kwargs)
            children.append(child)
            return child

        with mock.patch.object(runner.subprocess, "Popen", side_effect=capture_child):
            with self.assertRaisesRegex(runner.VerificationError, "timed out"):
                runner.run_process([sys.executable, "-c", "import time; time.sleep(60)"], directory, "timeout", 0.3)
        self.assertEqual(len(children), 1)
        self.assertIsNotNone(children[0].returncode)
        self.assertFalse(Path(f"/proc/{children[0].pid}").exists())
        self.assertTrue((directory / "timeout.stdout.jsonl").is_file())

    def test_existing_logs_are_not_overwritten(self):
        directory = self.fixture("placeholder").parent
        path = directory / "existing.stdout.jsonl"
        path.write_text("preserve me")
        with self.assertRaises(FileExistsError):
            runner.run_process([sys.executable, "-c", "pass"], directory, "existing", 5)
        self.assertEqual(path.read_text(), "preserve me")

    def make_synthetic_reproducer(self, corrupt=False):
        path = self.fixture("synthetic-reproducer")
        source = f'''#!{sys.executable}
# Test fixture only: these RSS numbers and FNV digests are synthetic.
import json, sqlite3, sys
from pathlib import Path
mode, name, total, transaction, row = sys.argv[1:]
total, transaction, row = int(total), int(transaction), int(row)
rows, tx_rows, row_bytes = total * 1024 // row, transaction * 1024 // row, row * 1024
clock = 0
def checkpoint(phase, n=0, payload_bytes=0):
    global clock
    clock += 1
    print(json.dumps(dict(phase=phase, rows=n, payload_bytes=payload_bytes,
                          elapsed_ms=clock, rss_kib=1000000, hwm_kib=1000000)))
checkpoint("start")
if mode == "write":
    c = sqlite3.connect(name)
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA wal_autocheckpoint=0")
    c.execute("CREATE TABLE records(id INTEGER PRIMARY KEY,payload BLOB NOT NULL)")
    c.commit()
    payload = bytes(i % 251 for i in range(row_bytes))
    for first in range(0, rows, tx_rows):
        last = min(first + tx_rows, rows)
        c.executemany("INSERT INTO records VALUES(?,?)", ((i + 1, payload) for i in range(first, last)))
        checkpoint("commit_begin", last, (last-first)*row_bytes)
        c.commit()
        checkpoint("commit_end", last, (last-first)*row_bytes)
    if {corrupt!r}:
        c.execute("UPDATE records SET payload=? WHERE id=1", (b"corrupt",))
        c.commit()
    checkpoint("all_transactions_committed", rows, rows*row_bytes)
    c.close()
    checkpoint("closed_without_checkpoint", rows, rows*row_bytes)
else:
    print(json.dumps(dict(phase="all_bytes_verified", rows=rows,
                          payload_bytes=rows*row_bytes, fnv1a64="0000000000000518")))
'''
        path.write_text(source)
        path.chmod(0o700)
        return path

    def arguments(self, binary, output):
        return ["--baseline", str(binary), "--candidate", str(binary), "--output", str(output),
                "--total-mib", "2", "--small-transaction-mib", "1", "--repetitions", "1",
                "--max-large-peak-ratio", "1", "--max-large-retained-ratio", "1", "--max-small-peak-ratio", "1"]

    def test_full_cli_with_synthetic_subprocesses_and_real_sqlite_readback(self):
        binary = self.make_synthetic_reproducer()
        output = self.fixture("comparison")
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            code = runner.main(self.arguments(binary, output))
        self.assertEqual(code, 0)
        report = json.loads((output / "report.json").read_text())
        self.assertEqual(report["status"], "passed")
        self.assertEqual(len(report["runs"]), 4)
        self.assertEqual({item["metrics"]["transactions"] for item in report["runs"]}, {1, 2})
        self.assertEqual(len({item["stock_readback"]["sha256"] for item in report["runs"]}), 1)
        self.assertEqual(report["binaries"]["baseline"]["sha256"], runner.file_sha256(binary))
        original = (output / "report.json").read_bytes()
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(runner.main(self.arguments(binary, output)), 2)
        self.assertEqual(original, (output / "report.json").read_bytes())

    def test_forged_native_success_does_not_bypass_stock_readback(self):
        binary = self.make_synthetic_reproducer(corrupt=True)
        output = self.fixture("comparison")
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            code = runner.main(self.arguments(binary, output))
        self.assertEqual(code, 2)
        report = json.loads((output / "report.json").read_text())
        self.assertEqual(report["status"], "error")
        self.assertIn("stock SQLite readback mismatch", report["error"])
        self.assertNotIn("verify", report["runs"][0])


if __name__ == "__main__":
    unittest.main()
