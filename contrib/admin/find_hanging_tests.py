#!/usr/bin/env python3
"""
Find tests that were expected to run in a CI job but did not complete.

Useful for identifying tests that may be hanging and causing a CI job to time out.

Usage:
    python contrib/admin/find_hanging_tests.py https://ucsc-ci.com/databiosphere/toil/-/jobs/105237

We turn all the pytest commands in the log into collect-only commands to get
the names of tests that should have run, and compare that to the names of tests
that actually finish.

Assumes the correct git commit is already checked out locally.
"""

import argparse
import os
import re
import shlex
import subprocess
import sys
import urllib.error
import urllib.request
from typing import Optional

from strip_ansi import strip_ansi

def fetch_url(url: str) -> str:
    """
    Fetch a URL and return its decoded text content.
    """
    req = urllib.request.Request(url)
    # We could add a Gitlab token here, but this script is Toil-specific and
    # the Toil logs are public.
    try:
        with urllib.request.urlopen(req) as response:
            return response.read().decode('utf-8', errors='replace')
    except urllib.error.HTTPError as e:
        print(f"HTTP {e.code} fetching {url}: {e.reason}", file=sys.stderr)
        sys.exit(1)
    except urllib.error.URLError as e:
        print(f"Failed to fetch {url}: {e.reason}", file=sys.stderr)
        sys.exit(1)


def extract_pytest_commands_from_log(log_text: str) -> list[list[str]]:
    """
    Find `python -m pytest` invocations echoed by make in the CI log.

    Returns a list of arg lists, where each list contains the
    arguments that appeared after 'pytest' on that line.
    """
    clean_log = strip_ansi(log_text)
    results = []

    for line in clean_log.splitlines():
        m = re.search(r'\bpython\d*(?:\.\d+)?\s+-m\s+pytest\b(.*)', line)
        if not m:
            continue
        rest = m.group(1)
        try:
            args = shlex.split(rest)
        except ValueError:
            args = rest.split()

        results.append(args)

    return results


# Matches verbose flags (-v, -vv, --verbose) that conflict with -q.
_VERBOSE_RE = re.compile(r'^-v+$|^--verbose$')

def build_collect_from_pytest_args(pytest_args: list[str]) -> list[str]:
    """
    Build a collect-only command from a pytest arg list found in the log.

    Uses the args as-is, except verbose flags (-v, -vv, --verbose) are removed
    because they conflict with -q. --collect-only and -q are appended.
    """
    filtered = [arg for arg in pytest_args if not _VERBOSE_RE.match(arg)]
    return [sys.executable, '-m', 'pytest'] + filtered + ['--collect-only', '-q']


def collect_expected_tests(cmd: list[str], verbose: bool = False) -> set[str]:
    """
    Run `pytest --collect-only -q` and return the set of collected test IDs.
    """
    if verbose:
        print(f"  Running: {' '.join(cmd)}", file=sys.stderr)

    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        cwd=os.getcwd(),
    )

    if result.returncode not in (0, 5):  # 5 = no tests collected
        if verbose and result.stderr.strip():
            print(f"  pytest stderr: {result.stderr.strip()}", file=sys.stderr)

    # pytest --collect-only -q outputs test IDs one per line, e.g.:
    #   src/toil/test/wdl/wdltoil_test.py::TestWDL::test_MD5sum
    # Skip summary lines like "3 tests collected in 1.23s", errors,
    # coverage reports, anything after each test name, and other stuff in
    # the log that isn't test names.
    return {line.strip().split()[0] for line in result.stdout.splitlines() if '::' in line and not line.startswith('=') and not line.startswith('ERROR')}


def parse_completed_tests(log_text: str) -> set[str]:
    """
    Scan CI log output for test results and return the set of completed test IDs.

    Handles both standard pytest output and pytest-xdist parallel output:

    pytest-xdist (-n N):
        [gw0] [  5%] PASSED src/toil/test/foo.py::Bar::test_baz
        [gw1] [ 10%] FAILED src/toil/test/foo.py::Bar::test_qux - AssertionError

    Standard (no xdist or -n 1):
        src/toil/test/foo.py::Bar::test_baz PASSED
        src/toil/test/foo.py::Bar::test_qux FAILED
    """
    log_text = strip_ansi(log_text)

    STATUS = r'(?:PASSED|FAILED|ERROR|SKIPPED|XFAIL|XPASS)'
    TEST_ID = r'(\S+::\S+)'
    # All these match the test name as group 1.
    regexes = (
        # pytest-xdist: [gw0] [XX%] STATUS test_id  (possibly "- reason" after)
        re.compile(
            r'\[gw\d+\]\s+(?:\[\s*\d+%\]\s+)?' + STATUS + r'\s+' + TEST_ID
        ),
        # Standard verbose: test_id STATUS
        re.compile(TEST_ID + r'\s+' + STATUS + r'(?:\s|$)'),
        # Reversed (some reporters): STATUS test_id
        re.compile(STATUS + r'\s+' + TEST_ID),
    )

    completed: set[str] = set()
    for line in log_text.splitlines():
        for option in regexes:
            # Try each regex against the line
            m = option.search(line)
            if m:
                # It's this one
                completed.add(m.group(1))
                break

    return completed


def main() -> int:
    parser = argparse.ArgumentParser(
        description='Find tests expected to run in a CI job that did not complete.',
        epilog='Assumes the correct git commit is already checked out locally.',
    )
    parser.add_argument(
        'job_url',
        help='GitLab job URL, e.g. https://ucsc-ci.com/databiosphere/toil/-/jobs/105237',
    )
    parser.add_argument(
        '--show-extra', action='store_true',
        help='Also report tests that appear completed in the log but were not expected',
    )
    parser.add_argument(
        '--verbose', '-v', action='store_true',
        help='Show more detail about collection commands and counts',
    )
    args = parser.parse_args()

    job_url = args.job_url.rstrip('/')

    # --- Fetch raw log ---
    raw_url = job_url + '/raw'
    print(f"Fetching raw log ...", file=sys.stderr)
    log_text = fetch_url(raw_url)
    print(f"Log: {len(log_text):,} bytes", file=sys.stderr)

    # --- Find pytest invocations from make's command echo in the log ---
    pytest_arg_lists = extract_pytest_commands_from_log(log_text)

    if not pytest_arg_lists:
        print(
            "No 'python -m pytest' invocations found in the log.\n"
            "The job may have timed out before any tests started, or the log\n"
            "format may differ from what was expected.",
            file=sys.stderr,
        )
        return 1

    print(
        f"Found {len(pytest_arg_lists)} pytest invocation(s) in log.",
        file=sys.stderr,
    )

    # --- Collect expected tests ---
    expected_tests: set[str] = set()
    for i, pytest_args in enumerate(pytest_arg_lists):
            print(
            f"Collecting exp[ected tests locally for invocation {i}...",
            file=sys.stderr,
        )
        cmd = build_collect_from_pytest_args(pytest_args)
        if args.verbose:
            print(f"  Collecting: {' '.join(cmd)}", file=sys.stderr)
        collected = collect_expected_tests(cmd, verbose=args.verbose)
        if args.verbose:
            print(f"  Collected {len(collected)} test(s)", file=sys.stderr)
        expected_tests.update(collected)

    if not expected_tests:
        print(
            "Warning: No tests were collected. "
            "Check that the test paths exist and dependencies are installed.",
            file=sys.stderr,
        )
        return 1

    print(f"Total expected tests: {len(expected_tests)}", file=sys.stderr)

    # --- Parse completed tests from the log ---
    completed_tests = parse_completed_tests(log_text)
    print(f"Tests completed in log: {len(completed_tests)}", file=sys.stderr)

    # --- Report ---
    missing = sorted(expected_tests - completed_tests)
    print()
    if missing:
        print(f"{len(missing)} test(s) expected but not completed:")
        for t in missing:
            print(f"  {t}")
    else:
        print("All expected tests completed.")

    if args.show_extra:
        extra = sorted(completed_tests - expected_tests)
        if extra:
            print(
                f"\n{len(extra)} test(s) completed in log but not in expected set "
                f"(may be from a different invocation or parametrized differently):"
            )
            for t in extra:
                print(f"  {t}")

    return 0


if __name__ == '__main__':
    sys.exit(main())
