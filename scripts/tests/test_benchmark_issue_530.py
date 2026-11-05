"""Guard GH#530/533 report integrity using synthetic CLI output, not performance data.

Run: python3 -m unittest discover -s scripts/tests -p 'test_benchmark_issue_530.py' -v
Temporary evidence is retained rather than deleted.
"""
from __future__ import annotations

import contextlib
import importlib.util
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock

SPEC = importlib.util.spec_from_file_location(
    "benchmark_issue_530", Path(__file__).resolve().parents[1] / "benchmark_issue_530.py")
assert SPEC is not None and SPEC.loader is not None
runner = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(runner)


def result(stdout="42\n", stderr="Run Time: real 0.25\n", returncode=0):
    return subprocess.CompletedProcess(["synthetic-cli"], returncode, stdout, stderr)


def memory_cli(work: Path, behavior: str = "ok") -> dict:
    """A real child process exercising pipes, PTY flushing and checkpoints."""
    path = work / f"synthetic-memory-{behavior}"
    path.write_text(f"""#!{sys.executable}
import sys
import time
behavior = {behavior!r}
if behavior == 'blocked_input':
    time.sleep(60)
for line in sys.stdin:
    if line.startswith('.print '):
        marker = line.split(maxsplit=1)[1].strip()
        print('wrong-marker' if behavior == 'bad_marker' else marker)
    elif line.startswith('SELECT '):
        if behavior == 'stderr':
            print('Error: synthetic query failure', file=sys.stderr)
        if behavior != 'missing':
            print('41' if behavior == 'wrong' else '42')
        if behavior == 'extra':
            print('42')
        if behavior == 'timeout':
            time.sleep(60)
        if behavior == 'exit':
            sys.exit(7)
""", encoding="utf-8")
    path.chmod(0o700)
    return {"path": str(path)}


class BenchmarkProtocolTests(unittest.TestCase):
    def test_native_units_cpu_and_timer_streams(self):
        for line, seconds, cpu in (
            ("Run Time: real 0.25 user 0.20 sys 0.03", 0.25, 0.23),
            ("Run Time: 250 ms user 0.20 sys 0.03", 0.25, 0.23),
            ("Run Time: 0.25 s", 0.25, None),
            ("Run Time: 250000 us", 0.25, None),
            ("Run Time: 250000 µs", 0.25, None),
            ("Run Time: 250000000 ns", 0.25, None),
        ):
            for output in (result("42\n42\n", line), result("42\n42\n" + line, "")):
                with self.subTest(timer=line, stderr=output.stderr):
                    sample = runner.validate_sample(output, expected=42, warmups=1)
                    self.assertAlmostEqual(sample["seconds"], seconds)
                    if cpu is None:
                        self.assertIsNone(sample["cpu_seconds"])
                    else:
                        self.assertAlmostEqual(sample["cpu_seconds"], cpu)

    def test_wrong_or_missing_answers_and_cli_errors_fail_closed(self):
        for output in (result("41\n42\n"), result("42\n41\n"), result("42\n"),
                       result("42\n42\n42\n"), result("42.0\n42\n"),
                       result("42\n42\n", "Run Time: real 0.25\nError: failed"),
                       result("42\n42\n", returncode=1)):
            with self.subTest(output=output), self.assertRaises(ValueError):
                runner.validate_sample(output, expected=42, warmups=1)

    def test_missing_negative_nonfinite_or_extra_timers_fail_closed(self):
        for timer in ("", "Run Time: -0.25 s", "Run Time: nan s",
                      "Run Time: " + "9" * 309 + " s",
                      "Run Time: real 0.25\nRun Time: 250 ms"):
            with self.subTest(timer=timer), self.assertRaises(ValueError):
                runner.validate_sample(result(stderr=timer), expected=42, warmups=0)
        with self.assertRaises(ValueError):
            runner.validate_sample(result("42\nRun Time: 0.25 s"), expected=42, warmups=0)

    def test_zero_native_timer_is_censored_but_answers_remain_required(self):
        # StatementTimer::report formats real seconds with {:.3}; a correctly
        # executed empty CTE can therefore report 0.000. This is not zero work.
        timer = "Run Time: real 0.000 user 0.000020 sys 0.000003"
        sample = runner.validate_sample(result("0\n0\n", timer), expected=0, warmups=1)
        self.assertEqual(sample["seconds"], 0)
        self.assertAlmostEqual(sample["cpu_seconds"], 0.000023)
        self.assertTrue(sample["below_timer_resolution"])
        for output in (result("0\n", timer), result("1\n0\n", timer),
                       result("0\n0\n", timer + "\nError: failed")):
            with self.subTest(output=output), self.assertRaises(ValueError):
                runner.validate_sample(output, expected=0, warmups=1)

    def test_zero_samples_suppress_only_affected_ratios_even_with_nonzero_median(self):
        samples = [{"engine": engine, "seconds": seconds, "cpu_seconds": None}
                   for engine, times in (("baseline", (2, 2, 2)),
                                         ("candidate", (0, 1, 1)), ("stock", (0.5, 0.5, 0.5)))
                   for seconds in times]
        summary = runner.summarize(samples)
        self.assertEqual(summary["candidate"]["median_seconds"], 1)
        self.assertEqual(summary["candidate"]["zero_timer_samples"], 1)
        self.assertEqual(summary["baseline_over_stock"], 4)
        self.assertIsNone(summary["candidate_over_stock"])
        self.assertIsNone(summary["baseline_over_candidate"])
        self.assertIn("below-resolution", summary["ratio_note"])
        zeros = [{"engine": engine, "seconds": 0, "cpu_seconds": None}
                 for engine in ("released", "stock")]
        self.assertIsNone(runner.summarize(zeros)["released_over_stock"])

    def test_release_summary_cannot_claim_candidate_speedup(self):
        samples = [{"engine": engine, "seconds": seconds, "cpu_seconds": None}
                   for engine, times in (("released", (1, 9, 2)), ("stock", (0.5, 2, 1)))
                   for seconds in times]
        summary = runner.summarize(samples)
        self.assertEqual(set(summary), {"released", "stock", "released_over_stock"})
        self.assertEqual(summary["released"]["median_seconds"], 2)
        self.assertEqual(summary["released_over_stock"], 2)
        self.assertNotIn("median_cpu_seconds", summary["released"])

    def test_invalid_cli_modes_and_duplicate_cases_reject_before_execution(self):
        work = Path(tempfile.mkdtemp(prefix="issue530-cli-test-"))
        invalid = (["--fsqlite", "candidate"],
                   ["--fsqlite", "candidate", "--release-fsqlite", "release"],
                   ["--release-fsqlite", "release", "--baseline-fsqlite", "baseline"],
                   ["--release-fsqlite", "release", "--candidate-revision", "sha"],
                   ["--fsqlite", "candidate", "--baseline-fsqlite", "baseline",
                    "--release-revision", "sha"],
                   ["--release-fsqlite", "release", "--case", "single_count",
                    "--case", "single_count"],
                   ["--release-fsqlite", "release", "--memory", "--memory-queries", "0"],
                   ["--release-fsqlite", "release", "--memory-queries", "2"],
                   ["--release-fsqlite", "release", "--issue", "533", "--case", "cartesian_count"],
                   ["--release-fsqlite", "release", "--case", "cte_count"])
        for arguments in invalid:
            argv = ["benchmark", "--work-dir", str(work), "--output", str(work / "report.json"),
                    *arguments]
            with self.subTest(arguments=arguments), mock.patch.object(sys, "argv", argv), \
                    mock.patch.object(runner.subprocess, "run") as execute, \
                    contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as error:
                runner.main()
            self.assertEqual(error.exception.code, 2)
            execute.assert_not_called()
        self.assertFalse((work / "report.json").exists())

    def test_cte_suite_selects_exact_reported_sql_and_retains_three_engine_evidence(self):
        work = Path(tempfile.mkdtemp(prefix="issue533-timing-mode-"))
        argv = ["benchmark", "--fsqlite", "candidate", "--baseline-fsqlite", "baseline",
                "--sqlite", "stock", "--issue", "533", "--case", "cte_count",
                "--case", "cte_empty_count", "--repeats", "2", "--warmups", "1",
                "--work-dir", str(work), "--output", str(work / "report.json")]
        measured = []

        def native(binary, script, init, directory, timeout, stem):
            if ".timer on" not in script:
                return result("SCAN synthetic\n", "")
            measured.append((binary["path"], script))
            if "generate_series(2,1)" in script:
                return result("0\n0\n", "Run Time: real 0.000")
            return result("1000000\n1000000\n", "Run Time: real 0.25")

        with mock.patch.object(sys, "argv", argv), \
                mock.patch.object(runner, "binary_info", side_effect=lambda path, *args: {"path": path}), \
                mock.patch.object(runner, "run_cli", side_effect=native), \
                contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(runner.main(), 0)
        report = json.loads((work / "report.json").read_text())
        self.assertEqual(report["issue"], 533)
        self.assertEqual(report["status"], "ok")
        self.assertEqual(set(report["results"]), {"cte_count", "cte_empty_count"})
        count = report["results"]["cte_count"]
        self.assertEqual(count["sql"], "WITH t AS (SELECT value FROM generate_series(1,1000000)) "
                                      "SELECT count(*) FROM t;")
        self.assertEqual(set(count["plans"]), {"baseline", "candidate", "stock"})
        self.assertEqual(len(count["samples"]), 6)
        self.assertEqual(count["summary"]["baseline_over_candidate"], 1)
        self.assertIsNone(report["results"]["cte_empty_count"]["summary"]["baseline_over_candidate"])
        self.assertEqual([engine for engine, _ in measured[:6]],
                         ["baseline", "candidate", "stock", "baseline", "stock", "candidate"])
        for _, script in measured[:6]:
            self.assertEqual(script.count(count["sql"]), 2)
            self.assertEqual(script.count(".timer on"), 1)
        profile = Path(report["work_directory"]) / "profile-cte_count.sql"
        self.assertIn(count["sql"], profile.read_text())
        self.assertNotIn(".timer on", profile.read_text())

    def test_timeout_preserves_partial_stdout_and_stderr(self):
        work = Path(tempfile.mkdtemp(prefix="issue530-timeout-test-"))
        for index, partial in enumerate((b"42\npartial\xff", "42\npartial", None)):
            error = subprocess.TimeoutExpired("synthetic-cli", 1, output=partial, stderr=b"interrupted")
            with mock.patch.object(runner.subprocess, "run", side_effect=error), \
                    self.assertRaises(subprocess.TimeoutExpired):
                runner.run_cli({"path": "synthetic-cli"}, "SELECT 42;", work / "init", work, 1, str(index))
            expected = partial.decode("utf-8", errors="replace") if isinstance(partial, bytes) else partial
            self.assertEqual((work / f"{index}.stdout").read_text(), expected or "")
            self.assertEqual((work / f"{index}.stderr").read_text(), "interrupted")

    def test_process_memory_requires_positive_counters_with_linux_units(self):
        good = "Name:\tsqlite\nVmRSS:\t 1240 kB\nVmHWM:\t 4321 kB\nThreads:\t2\n"
        self.assertEqual(runner.parse_process_memory(good), {"rss_kib": 1240, "hwm_kib": 4321})
        for malformed in ("", "VmRSS: 123 kB", "VmHWM: 123 kB",
                          good.replace("1240", "0"), good.replace("1240", "-1"),
                          good.replace("4321", "unknown"), good.replace("kB", "MB"),
                          good + "VmRSS: 10 kB\n"):
            with self.subTest(status=malformed), self.assertRaises(ValueError):
                runner.parse_process_memory(malformed)

    @unittest.skipUnless(sys.platform.startswith("linux"), "Linux /proc memory protocol")
    def test_memory_protocol_samples_same_live_child_and_checks_every_answer(self):
        work = Path(tempfile.mkdtemp(prefix="issue530-memory-protocol-"))
        sample = runner.run_memory_cli(memory_cli(work), "SELECT 42;", 42, [], 3,
                                       work / "init", work, 5, "success")
        self.assertEqual(sample["queries"], 3)
        self.assertEqual([point["query_count"] for point in sample["checkpoints"]], [0, 1, 2, 3])
        for point in sample["checkpoints"]:
            self.assertGreater(point["rss_kib"], 0)
            self.assertGreater(point["hwm_kib"], 0)
        self.assertEqual((work / "success.stdout").read_text().splitlines().count("42"), 3)
        self.assertEqual((work / "success.stderr").read_text(), "")
        evidence = json.loads((work / "success.checkpoints.json").read_text())
        self.assertEqual(evidence["status"], "ok")
        self.assertEqual(evidence["pid"], sample["pid"])
        self.assertEqual(evidence["proc_pid"], sample["proc_pid"])
        self.assertGreater(sample["proc_pid"], 0)
        self.assertEqual((work / "success.proc_pid").read_text(), f"{sample['proc_pid']}\n")
        self.assertEqual(evidence["checkpoints"], sample["checkpoints"])

    @unittest.skipUnless(sys.platform.startswith("linux"), "Linux /proc memory protocol")
    def test_pid_handshake_supports_high_descriptors_and_keeps_sql_output_separate(self):
        work = Path(tempfile.mkdtemp(prefix="issue533-pid-fds-"))
        held = []
        try:
            held.extend(os.open(os.devnull, os.O_RDONLY) for _ in range(16))
            self.assertGreater(held[-1], 9)
            sample = runner.run_memory_cli(memory_cli(work), "SELECT 42;", 42, [], 1,
                                          work / "init", work, 5, "high-fds")
        finally:
            for descriptor in held:
                os.close(descriptor)
        output = (work / "high-fds.stdout").read_text().splitlines()
        self.assertNotIn(str(sample["proc_pid"]), output)
        self.assertEqual(output.count("42"), 1)
        self.assertEqual(len(sample["checkpoints"]), 2)

    @unittest.skipUnless(sys.platform.startswith("linux"), "Linux /proc memory protocol")
    def test_malformed_or_missing_pid_handshake_fails_before_any_checkpoint(self):
        work = Path(tempfile.mkdtemp(prefix="issue533-bad-pid-"))
        for index, pid in enumerate(("0\\n", "-1\\n", "12x\\n", "\\n", "12\\n13\\n",
                                     "2147483648\\n", "9" * 33, "12", "")):
            launcher = f"printf '{pid}' >\"/proc/self/fd/$1\"; exit 0"
            with self.subTest(pid=pid), mock.patch.object(runner, "PROC_PID_LAUNCHER", launcher), \
                    self.assertRaises(ValueError):
                runner.run_memory_cli(memory_cli(work), "SELECT 42;", 42, [], 1,
                                      work / "init", work, 5, f"invalid-{index}")
            evidence = json.loads((work / f"invalid-{index}.checkpoints.json").read_text())
            self.assertEqual(evidence["status"], "incomplete")
            self.assertEqual(evidence["checkpoints"], [])
            self.assertEqual((work / f"invalid-{index}.stdout").read_text(), "")

    @unittest.skipUnless(sys.platform.startswith("linux"), "Linux /proc memory protocol")
    def test_pid_handshake_uses_the_same_deadline_and_reaps_stalled_child(self):
        work = Path(tempfile.mkdtemp(prefix="issue533-pid-timeout-"))
        started = time.monotonic()
        # A shell builtin waits for input. The collector must not submit SQL
        # until it has a PID, and must kill/reap this child at the same deadline.
        with mock.patch.object(runner, "PROC_PID_LAUNCHER", "read -r ignored"), \
                self.assertRaises(subprocess.TimeoutExpired):
            runner.run_memory_cli(memory_cli(work), "SELECT 42;", 42, [], 1,
                                  work / "init", work, 0.3, "stalled")
        self.assertLess(time.monotonic() - started, 5)
        evidence = json.loads((work / "stalled.checkpoints.json").read_text())
        self.assertEqual(evidence["status"], "incomplete")
        self.assertIsNone(evidence["proc_pid"])
        self.assertEqual(evidence["checkpoints"], [])
        with self.assertRaises(ChildProcessError):
            os.waitpid(evidence["pid"], os.WNOHANG)

    @unittest.skipUnless(sys.platform.startswith("linux"), "Linux /proc memory protocol")
    def test_memory_protocol_rejects_bad_results_stderr_and_early_exit(self):
        work = Path(tempfile.mkdtemp(prefix="issue530-memory-errors-"))
        for behavior in ("wrong", "missing", "extra", "stderr", "exit"):
            with self.subTest(behavior=behavior), self.assertRaises(ValueError):
                runner.run_memory_cli(memory_cli(work, behavior), "SELECT 42;", 42, [], 2,
                                      work / "init", work, 5, behavior)
            evidence = json.loads((work / f"{behavior}.checkpoints.json").read_text())
            self.assertEqual(evidence["status"], "incomplete")
            self.assertEqual(len(evidence["checkpoints"]), 1)
            self.assertTrue((work / f"{behavior}.stdout").exists())
            self.assertTrue((work / f"{behavior}.stderr").exists())

    @unittest.skipUnless(sys.platform.startswith("linux"), "Linux /proc memory protocol")
    def test_memory_timeout_bounds_reads_and_writes_and_preserves_checkpoints(self):
        work = Path(tempfile.mkdtemp(prefix="issue530-memory-timeout-"))
        for behavior in ("timeout", "bad_marker", "blocked_input"):
            setup = ["x" * 100_000] if behavior == "blocked_input" else []
            started = time.monotonic()
            with self.subTest(behavior=behavior), self.assertRaises(subprocess.TimeoutExpired):
                runner.run_memory_cli(memory_cli(work, behavior), "SELECT 42;", 42, setup, 2,
                                      work / "init", work, 0.3, behavior)
            self.assertLess(time.monotonic() - started, 5)
            evidence = json.loads((work / f"{behavior}.checkpoints.json").read_text())
            self.assertEqual(evidence["status"], "incomplete")
            if behavior == "timeout":
                self.assertEqual(len(evidence["checkpoints"]), 1)
                self.assertIn("42", (work / f"{behavior}.stdout").read_text())

    @unittest.skipUnless(sys.platform.startswith("linux"), "Linux /proc memory protocol")
    def test_missing_proc_counters_are_errors_with_retained_protocol_logs(self):
        work = Path(tempfile.mkdtemp(prefix="issue530-memory-counters-"))
        with mock.patch.object(runner, "parse_process_memory", side_effect=ValueError("missing VmRSS")), \
                self.assertRaisesRegex(ValueError, "missing VmRSS"):
            runner.run_memory_cli(memory_cli(work), "SELECT 42;", 42, [], 2,
                                  work / "init", work, 5, "missing-proc")
        evidence = json.loads((work / "missing-proc.checkpoints.json").read_text())
        self.assertEqual(evidence["status"], "incomplete")
        self.assertIn("__issue530_", (work / "missing-proc.stdout").read_text())

    def test_memory_summary_distinguishes_first_peak_from_retained_growth(self):
        sample = {"engine": "candidate", "checkpoints": [
            {"query_count": 0, "rss_kib": 100, "hwm_kib": 120},
            {"query_count": 1, "rss_kib": 300, "hwm_kib": 1000},
            {"query_count": 2, "rss_kib": 250, "hwm_kib": 1200},
        ]}
        summary = runner.summarize_memory([sample])["candidate"]
        self.assertEqual(summary["median_first_query_hwm_kib"], 1000)
        self.assertEqual(summary["median_final_query_hwm_kib"], 1200)
        self.assertEqual(summary["median_final_query_rss_kib"], 250)
        self.assertEqual(summary["median_final_minus_setup_rss_kib"], 150)
        self.assertEqual(summary["median_final_minus_first_rss_kib"], -50)

    @unittest.skipUnless(sys.platform.startswith("linux"), "Linux /proc memory protocol")
    def test_memory_mode_never_collects_or_claims_timing_samples(self):
        work = Path(tempfile.mkdtemp(prefix="issue530-memory-mode-"))
        argv = ["benchmark", "--release-fsqlite", "release", "--sqlite", "stock",
                "--memory", "--memory-queries", "2", "--repeats", "1",
                "--case", "single_count", "--work-dir", str(work),
                "--output", str(work / "report.json")]
        memory = {"queries": 2, "checkpoints": [
            {"query_count": 0, "rss_kib": 100, "hwm_kib": 120},
            {"query_count": 1, "rss_kib": 300, "hwm_kib": 1000},
            {"query_count": 2, "rss_kib": 250, "hwm_kib": 1200},
        ]}
        with mock.patch.object(sys, "argv", argv), \
                mock.patch.object(runner, "binary_info", return_value={"path": "synthetic"}), \
                mock.patch.object(runner, "run_memory_cli", return_value=memory) as memory_run, \
                mock.patch.object(runner, "run_cli") as timer_run, \
                contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(runner.main(), 0)
        timer_run.assert_not_called()
        self.assertEqual(memory_run.call_count, 2)
        report = json.loads((work / "report.json").read_text())
        self.assertEqual(report["measurement_mode"], "memory")
        self.assertEqual(report["issue"], 530)
        self.assertEqual(report["status"], "ok")
        self.assertNotIn("timing_scope", report)
        self.assertNotIn("warmups_per_sample", report)
        self.assertNotIn("seconds", report["results"]["single_count"]["samples"][0])

    @unittest.skipUnless(sys.platform.startswith("linux"), "Linux /proc memory protocol")
    def test_failed_memory_collection_writes_error_report(self):
        work = Path(tempfile.mkdtemp(prefix="issue530-memory-failed-report-"))
        argv = ["benchmark", "--release-fsqlite", "release", "--sqlite", "stock",
                "--memory", "--repeats", "1", "--case", "single_count",
                "--work-dir", str(work), "--output", str(work / "report.json")]
        with mock.patch.object(sys, "argv", argv), \
                mock.patch.object(runner, "binary_info", return_value={"path": "synthetic"}), \
                mock.patch.object(runner, "run_memory_cli", side_effect=OSError("missing /proc child")), \
                contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(runner.main(), 1)
        report = json.loads((work / "report.json").read_text())
        self.assertEqual(report["status"], "error")
        self.assertEqual(report["error"], "missing /proc child")
        self.assertNotIn("summary", report["results"]["single_count"])


if __name__ == "__main__":
    unittest.main()
