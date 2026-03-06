#!/usr/bin/env python3
"""
Find tests that were expected to run in a CI job but did not complete.

Useful for identifying tests that may be hanging and causing a CI job to time out.

Usage:
    python contrib/admin/find_hanging_tests.py https://ucsc-ci.com/databiosphere/toil/-/jobs/105237

The script:
1. Retrieves the raw job log from the GitLab job URL.
2. Uses the GitLab API to determine which .gitlab-ci.yml job the run corresponds to.
3. Runs 'pytest --collect-only -q' on that job's test specifiers to enumerate expected tests.
4. Compares the expected tests against tests that appear as completed in the log.
5. Prints tests that were expected but did not complete.

Assumes the correct git commit is already checked out locally.
"""

import argparse
import json
import os
import re
import shlex
import subprocess
import sys
import urllib.error
import urllib.parse
import urllib.request
from typing import Optional

try:
    import yaml
except ImportError:
    print("PyYAML is required: pip install pyyaml", file=sys.stderr)
    sys.exit(1)


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
            print("Authentication required. Provide a GitLab token with --token.", file=sys.stderr)
        elif e.code == 403:
            print("Access denied. Try providing a GitLab API token with --token.", file=sys.stderr)
        sys.exit(1)
    except urllib.error.URLError as e:
        print(f"Failed to fetch {url}: {e.reason}", file=sys.stderr)
        sys.exit(1)


def parse_job_url(job_url: str) -> tuple[str, str, str]:
    """
    Parse a GitLab job URL into (base_url, project_path, job_id).

    Example:
        https://ucsc-ci.com/databiosphere/toil/-/jobs/105237
        -> ('https://ucsc-ci.com', 'databiosphere/toil', '105237')
    """
    parsed = urllib.parse.urlparse(job_url)
    base_url = f"{parsed.scheme}://{parsed.netloc}"
    m = re.match(r'^(.+?)/-/jobs/(\d+)$', parsed.path)
    if not m:
        raise ValueError(
            f"Cannot parse job URL: {job_url!r}\n"
            "Expected format: https://<host>/<project>/-/jobs/<id>"
        )
    project_path = m.group(1).lstrip('/')
    job_id = m.group(2)
    return base_url, project_path, job_id


def get_gitlab_job(base_url: str, project_path: str, job_id: str,
                   token: Optional[str] = None) -> dict:
    """
    Retrieve job metadata from the GitLab API.

    See: https://docs.gitlab.com/api/jobs/#retrieve-a-job-by-job-id
    """
    encoded_project = urllib.parse.quote(project_path, safe='')
    api_url = f"{base_url}/api/v4/projects/{encoded_project}/jobs/{job_id}"
    return json.loads(fetch_url(api_url, token))


def resolve_ci_vars(text: str, variables: dict[str, str]) -> str:
    """Expand ${VAR} and $VAR references using the given variable dict."""
    def replace(m: re.Match) -> str:
        varname = m.group(1) or m.group(2)
        return variables.get(varname, m.group(0))
    return re.sub(r'\$\{(\w+)\}|\$(\w+)', replace, text)


def extract_make_test_invocations(script, variables: dict[str, str]) -> list[dict]:
    """
    Find all `make test`, `make doctest`, and `make test_offline` invocations in a
    CI job script (either a string or a list of strings).

    Returns a list of dicts with keys:
        'type'       - 'test', 'doctest', or 'test_offline'
        'tests'      - value of tests= (expanded, may include paths and -k filter)
        'marker'     - value of marker= (expanded), or '' if not specified
    """
    if isinstance(script, list):
        script_text = '\n'.join(str(s) for s in script)
    else:
        script_text = str(script)

    # Expand CI variable references so ${MARKER} etc. become their values
    script_text = resolve_ci_vars(script_text, variables)

    results = []

    # Match make test / make doctest / make test_offline, possibly preceded by
    # env var assignments or 'time'. Capture everything after the target on the line.
    make_re = re.compile(
        r'^[^\n#]*?\bmake\s+(test_offline|doctest|test)\b(.*?)$',
        re.MULTILINE,
    )

    for m in make_re.finditer(script_text):
        target = m.group(1)  # 'test', 'doctest', or 'test_offline'
        rest = m.group(2)

        # Extract tests= "..." or tests=...
        tests_val = ''
        tm = re.search(r'\btests=(["\'])(.*?)\1', rest)
        if tm:
            tests_val = tm.group(2)
        else:
            tm = re.search(r'\btests=(\S+)', rest)
            if tm:
                tests_val = tm.group(1)

        # Extract marker= "..." or marker=...
        marker_val = ''
        mm = re.search(r'\bmarker=(["\'])(.*?)\1', rest)
        if mm:
            marker_val = mm.group(2)
        else:
            mm = re.search(r'\bmarker=(\S+)', rest)
            if mm:
                marker_val = mm.group(1).strip('"\'')

        results.append({
            'type': target,
            'tests': tests_val,
            'marker': marker_val,
        })

    return results


def build_collect_command(invocation: dict) -> list[str]:
    """
    Build a `pytest --collect-only -q` command from a parsed make-test invocation.

    The tests= value is passed through to pytest as-is (after shell splitting),
    so paths, -k filters, and --ignore options all work naturally.

    When tests= is empty (not specified in the make invocation), the Makefile
    default is used: `src/toil --ignore src/toil/batchSystems/htcondor.py`.

    For `make doctest`, `--ignore src/toil/test` is also added to restrict
    collection to source files only.
    """
    tests_str = invocation.get('tests', '')
    marker = invocation.get('marker', '')
    target = invocation.get('type', 'test')

    if tests_str:
        try:
            tests_parts = shlex.split(tests_str)
        except ValueError:
            tests_parts = tests_str.split()
    else:
        # Makefile default: src/toil --ignore src/toil/batchSystems/htcondor.py
        tests_parts = ['src/toil', '--ignore', 'src/toil/batchSystems/htcondor.py']

    cmd = [sys.executable, '-m', 'pytest', '--collect-only', '-q', '--no-header']

    if target == 'doctest':
        # doctest runs on source files only, not the test directory
        cmd += ['--ignore', 'src/toil/test']

    if marker:
        cmd += ['-m', marker]

    cmd += tests_parts
    return cmd


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
        # Print warnings but don't abort; partial results are still useful
        if verbose and result.stderr.strip():
            print(f"  pytest stderr: {result.stderr.strip()[:500]}", file=sys.stderr)

    tests: set[str] = set()
    for line in result.stdout.splitlines():
        line = line.strip()
        # pytest --collect-only -q outputs one test ID per line, e.g.:
        #   src/toil/test/wdl/wdltoil_test.py::TestWDL::test_MD5sum
        # Skip summary lines like "3 tests collected in 1.23s" or errors
        if '::' in line and not line.startswith('=') and not line.startswith('ERROR'):
            # Take only the first token in case there's trailing annotation
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
    # A test ID starts with a path containing '/' and must contain '::'
    # It runs until whitespace.
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
        help='GitLab personal access token (required for private projects or API auth)',
    )
    parser.add_argument(
        '--gitlab-ci', default='.gitlab-ci.yml',
        metavar='FILE',
        help='Path to .gitlab-ci.yml (default: .gitlab-ci.yml)',
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

    # --- Parse the job URL ---
    try:
        base_url, project_path, job_id = parse_job_url(job_url)
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1

    # --- Get job name from GitLab API ---
    print(f"Querying GitLab API for job {job_id} ...", file=sys.stderr)
    job_info = get_gitlab_job(base_url, project_path, job_id, args.token)
    job_name = job_info['name']
    print(f"Job name: {job_name}", file=sys.stderr)

    # --- Fetch raw log ---
    raw_url = f"{job_url}/raw"
    print(f"Fetching raw log ...", file=sys.stderr)
    log_text = fetch_url(raw_url, args.token)
    print(f"Log: {len(log_text):,} bytes", file=sys.stderr)

    # --- Load CI config ---
    if not os.path.exists(args.gitlab_ci):
        print(
            f"Error: {args.gitlab_ci} not found. Run from the repository root.",
            file=sys.stderr,
        )
        return 1

    with open(args.gitlab_ci) as f:
        ci_config = yaml.safe_load(f)

    job_def = ci_config.get(job_name)
    if not job_def:
        print(f"Error: Job '{job_name}' not found in {args.gitlab_ci}", file=sys.stderr)
        return 1

    # Build the variable namespace: global CI vars, which may be referenced in scripts
    ci_variables: dict[str, str] = {
        k: str(v) for k, v in ci_config.get('variables', {}).items()
    }

    # --- Extract make test invocations from the job script ---
    script = job_def.get('script', '')
    invocations = extract_make_test_invocations(script, ci_variables)

    if not invocations:
        print(
            f"No 'make test', 'make doctest', or 'make test_offline' invocations "
            f"found in job '{job_name}'.",
            file=sys.stderr,
        )
        return 0

    print(
        f"Found {len(invocations)} test invocation(s) in '{job_name}':",
        file=sys.stderr,
    )
    for inv in invocations:
        print(
            f"  make {inv['type']}  tests={inv['tests']!r}  marker={inv['marker']!r}",
            file=sys.stderr,
        )

    # --- Collect expected tests via pytest --collect-only ---
    expected_tests: set[str] = set()
    for inv in invocations:
        cmd = build_collect_command(inv)
        if args.verbose:
            print(f"Collecting for: make {inv['type']} ...", file=sys.stderr)
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
