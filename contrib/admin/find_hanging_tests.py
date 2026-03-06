#!/usr/bin/env python3
"""
Find tests that were expected to run in a CI job but did not complete.

Useful for identifying tests that may be hanging and causing a CI job to time out.

Usage:
    python contrib/admin/find_hanging_tests.py https://ucsc-ci.com/databiosphere/toil/-/jobs/105237

The script:
1. Retrieves the raw job log from the GitLab job URL.
2. Scans the log for `python -m pytest` commands echoed by make before executing
   them. All arguments are already fully substituted at this point.
3. Runs 'pytest --collect-only -q' using the same test-selection arguments
   (paths, -m marker, -k filter, --ignore) to enumerate the expected tests.
4. Compares those against tests that appear completed in the log.
5. Prints tests that were expected but did not complete.

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


# Matches ANSI CSI escape sequences (colors, cursor movement, erase-in-line, etc.)
_ANSI_CSI_RE = re.compile(r'\x1b\[[0-9;]*[A-Za-z]')
# Matches GitLab CI section markers embedded in the log
_GITLAB_SECTION_RE = re.compile(r'\x1b\[0Ksection_\w+:[^\r\n]*\r?')
# Matches other standalone ESC sequences (e.g. \x1b(B)
_ANSI_OTHER_RE = re.compile(r'\x1b[^[]')


def strip_ansi(text: str) -> str:
    """Remove ANSI escape codes and GitLab CI section markers from text."""
    text = _GITLAB_SECTION_RE.sub('', text)
    text = _ANSI_CSI_RE.sub('', text)
    text = _ANSI_OTHER_RE.sub('', text)
    return text


def fetch_url(url: str, token: Optional[str] = None) -> str:
    """Fetch a URL and return its decoded text content."""
    req = urllib.request.Request(url)
    if token:
        req.add_header('PRIVATE-TOKEN', token)
    try:
        with urllib.request.urlopen(req) as response:
            return response.read().decode('utf-8', errors='replace')
    except urllib.error.HTTPError as e:
        print(f"HTTP {e.code} fetching {url}: {e.reason}", file=sys.stderr)
        if e.code == 401:
            print("Authentication required. Provide a token with --token.", file=sys.stderr)
        elif e.code == 403:
            print("Access denied. Try providing a token with --token.", file=sys.stderr)
        sys.exit(1)
    except urllib.error.URLError as e:
        print(f"Failed to fetch {url}: {e.reason}", file=sys.stderr)
        sys.exit(1)


# pytest flags that consume the next token as their value.
# Needed so we can correctly skip over non-collection arguments.
_PYTEST_FLAGS_WITH_VALUE = frozenset([
    '-m', '-k', '-n', '--numprocesses', '--dist',
    '--timeout', '--timeout-method', '--maxfail',
    '--log-level', '--log-cli-level', '--log-format', '--log-cli-format',
    '--log-date-format', '--log-cli-date-format',
    '--log-file', '--log-file-level', '--log-file-format',
    '--cov', '--cov-report', '--cov-config', '--cov-source',
    '--junit-xml', '--junit-prefix', '--junit-logging',
    '--ignore', '--ignore-glob', '--rootdir', '--confcutdir',
    '-o', '--override-ini', '-p', '--capture', '--tb',
    '--durations', '--durations-min', '-c', '--color', '--import-mode',
    '--cwl-badgedir', '--cwl-args', '--cwl-runner',
    '--randomly-seed', '--deselect',
    '-W', '--pythonwarnings', '--basetemp',
])

# The subset of the above that are relevant to test collection and should be
# forwarded to `pytest --collect-only`.
_PYTEST_COLLECT_FLAGS = frozenset(['-m', '-k', '--ignore', '--ignore-glob'])


def extract_pytest_commands_from_log(log_text: str) -> list[list[str]]:
    """
    Find `python -m pytest` invocations echoed by make in the CI log.

    make prints each recipe command before executing it. After ANSI stripping,
    these appear as lines containing 'python -m pytest <args...>', possibly
    preceded by environment variable assignments (TOIL_OWNER_TAG=... etc.).

    Returns a deduplicated list of arg lists, where each list contains the
    arguments that appeared after 'pytest' on that line.
    """
    clean_log = strip_ansi(log_text)
    results = []
    seen: set[tuple] = set()

    for line in clean_log.splitlines():
        m = re.search(r'\bpython\d*(?:\.\d+)?\s+-m\s+pytest\b(.*)', line)
        if not m:
            continue
        rest = m.group(1)
        try:
            args = shlex.split(rest)
        except ValueError:
            args = rest.split()

        key = tuple(args)
        if key not in seen:
            seen.add(key)
            results.append(args)

    return results


def build_collect_from_pytest_args(pytest_args: list[str]) -> list[str]:
    """
    Build a `pytest --collect-only -q --no-header` command from the argument
    list of a `python -m pytest` invocation found in the log.

    Keeps args relevant to test collection:
        - positional test paths
        - -m  (marker filter)
        - -k  (keyword filter)
        - --ignore / --ignore-glob

    Drops everything else: parallelism (-n, --dist), logging, coverage,
    verbosity, timing, random ordering, plugin-specific flags, etc.
    """
    out = [sys.executable, '-m', 'pytest', '--collect-only', '-q', '--no-header']

    i = 0
    while i < len(pytest_args):
        arg = pytest_args[i]

        # --flag=value form
        if arg.startswith('-') and '=' in arg:
            flag = arg.split('=', 1)[0]
            if flag in _PYTEST_COLLECT_FLAGS:
                out.append(arg)
            i += 1
            continue

        # Compact -mVALUE or -kVALUE (e.g. -mnot_slow)
        matched_short = False
        for short in ('-m', '-k'):
            if arg.startswith(short) and len(arg) > len(short):
                if short in _PYTEST_COLLECT_FLAGS:
                    out += [short, arg[len(short):]]
                i += 1
                matched_short = True
                break
        if matched_short:
            continue

        if arg in _PYTEST_FLAGS_WITH_VALUE:
            # Flag + separate value token
            val = pytest_args[i + 1] if i + 1 < len(pytest_args) else ''
            if arg in _PYTEST_COLLECT_FLAGS:
                out += [arg, val]
            i += 2
        elif arg.startswith('-'):
            # Boolean flag — skip
            i += 1
        else:
            # Positional argument (test path)
            out.append(arg)
            i += 1

    return out


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
            print(f"  pytest stderr: {result.stderr.strip()[:500]}", file=sys.stderr)

    tests: set[str] = set()
    for line in result.stdout.splitlines():
        line = line.strip()
        # pytest --collect-only -q outputs one test ID per line, e.g.:
        #   src/toil/test/wdl/wdltoil_test.py::TestWDL::test_MD5sum
        # Skip summary lines like "3 tests collected in 1.23s" or errors.
        if '::' in line and not line.startswith('=') and not line.startswith('ERROR'):
            tests.add(line.split()[0])
    return tests


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

    # pytest-xdist: [gw0] [XX%] STATUS test_id  (possibly "- reason" after)
    xdist_re = re.compile(
        r'\[gw\d+\]\s+(?:\[\s*\d+%\]\s+)?' + STATUS + r'\s+' + TEST_ID
    )
    # Standard verbose: test_id STATUS
    standard_re = re.compile(TEST_ID + r'\s+' + STATUS + r'(?:\s|$)')
    # Reversed (some reporters): STATUS test_id
    reversed_re = re.compile(STATUS + r'\s+' + TEST_ID)

    completed: set[str] = set()
    for line in log_text.splitlines():
        m = xdist_re.search(line)
        if m:
            completed.add(m.group(1))
            continue
        m = standard_re.search(line)
        if m:
            completed.add(m.group(1))
            continue
        m = reversed_re.search(line)
        if m:
            completed.add(m.group(1))

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
        '--token', '-t',
        help='Token for fetching the log if the job page requires authentication',
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
    log_text = fetch_url(raw_url, args.token)
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
    for pytest_args in pytest_arg_lists:
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
        print(f"{len(missing)} test(s) expected but not completed (possible hangers):")
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
