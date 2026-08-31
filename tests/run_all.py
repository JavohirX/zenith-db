"""
ZenithDB Unified Test Runner
Zero-dependency test discovery, execution, and ANSI reporting.
"""

import os
import sys
import time
import unittest

# Ensure project root is in sys.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from zenith.cli.terminal import Colors, format_table, print_banner


class ZenithTestResult(unittest.TextTestResult):
    def __init__(self, stream, descriptions, verbosity):
        super().__init__(stream, descriptions, verbosity)
        self.results_data = []

    def startTest(self, test):
        self._test_start = time.perf_counter()
        super().startTest(test)

    def addSuccess(self, test):
        elapsed = (time.perf_counter() - self._test_start) * 1000.0
        self.results_data.append((test.id(), "PASS", elapsed, ""))
        super().addSuccess(test)

    def addFailure(self, test, err):
        elapsed = (time.perf_counter() - self._test_start) * 1000.0
        self.results_data.append((test.id(), "FAIL", elapsed, str(err[1])))
        super().addFailure(test, err)

    def addError(self, test, err):
        elapsed = (time.perf_counter() - self._test_start) * 1000.0
        self.results_data.append((test.id(), "ERROR", elapsed, str(err[1])))
        super().addError(test, err)


def run_tests() -> bool:
    print_banner()
    print(f"{Colors.BOLD}⚡ Running ZenithDB Comprehensive Test Suite{Colors.RESET}\n")

    loader = unittest.TestLoader()
    start_dir = os.path.dirname(os.path.abspath(__file__))
    suite = loader.discover(start_dir, pattern="test_*.py")

    start_time = time.time()
    runner = unittest.TextTestRunner(resultclass=ZenithTestResult, verbosity=1)
    result = runner.run(suite)
    duration = time.time() - start_time

    # Render summary table
    headers = ["Test Module / Case", "Status", "Duration"]
    rows = []
    for test_id, status, elapsed, _ in result.results_data:
        short_name = test_id.split(".")[-2] + "." + test_id.split(".")[-1]
        status_colored = (
            f"{Colors.GREEN}PASS ✓{Colors.RESET}"
            if status == "PASS"
            else f"{Colors.RED}{status} ✗{Colors.RESET}"
        )
        rows.append([short_name, status_colored, f"{elapsed:.2f} ms"])

    print("\n" + format_table(headers, rows))

    print(
        f"\n{Colors.BOLD}Results:{Colors.RESET} {result.testsRun} tests executed in {duration:.2f}s "
        f"({Colors.GREEN}{result.testsRun - len(result.failures) - len(result.errors)} passed{Colors.RESET}, "
        f"{Colors.RED}{len(result.failures)} failed{Colors.RESET}, "
        f"{Colors.YELLOW}{len(result.errors)} errors{Colors.RESET})\n"
    )

    return result.wasSuccessful()


if __name__ == "__main__":
    success = run_tests()
    sys.exit(0 if success else 1)
