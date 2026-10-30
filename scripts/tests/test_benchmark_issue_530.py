"""Guard GH#530 report integrity using synthetic CLI output, not performance data.

Run: python3 -m unittest discover -s scripts/tests -p 'test_benchmark_issue_530.py' -v
Temporary evidence is retained rather than deleted.
"""
from __future__ import annotations

import contextlib
import importlib.util
import io
import json
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

    def test_missing_nonpositive_nonfinite_or_extra_timers_fail_closed(self):
        for timer in ("", "Run Time: real 0", "Run Time: -0.25 s", "Run Time: nan s",
                      "Run Time: " + "9" * 309 + " s",
                      "Run Time: real 0.25\nRun Time: 250 ms"):
            with self.subTest(timer=timer), self.assertRaises(ValueError):
                runner.validate_sample(result(stderr=timer), expected=42, warmups=0)
        with self.assertRaises(ValueError):
            runner.validate_sample(result("42\nRun Time: 0.25 s"), expected=42, warmups=0)

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
                   ["--release-fsqlite", "release", "--memory-queries", "2"])
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
        self.assertEqual(evidence["checkpoints"], sample["checkpoints"])

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
