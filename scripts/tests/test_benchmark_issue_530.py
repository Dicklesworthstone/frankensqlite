"""Guard GH#530 report integrity using synthetic CLI output, not performance data.

Run: python3 -m unittest discover -s scripts/tests -p 'test_benchmark_issue_530.py' -v
Temporary evidence is retained rather than deleted.
"""
from __future__ import annotations

import contextlib
import importlib.util
import io
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

SPEC = importlib.util.spec_from_file_location(
    "benchmark_issue_530", Path(__file__).resolve().parents[1] / "benchmark_issue_530.py")
assert SPEC is not None and SPEC.loader is not None
runner = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(runner)


def result(stdout="42\n", stderr="Run Time: real 0.25\n", returncode=0):
    return subprocess.CompletedProcess(["synthetic-cli"], returncode, stdout, stderr)


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
                    "--case", "single_count"])
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


if __name__ == "__main__":
    unittest.main()
