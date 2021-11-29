import argparse
import json
import logging
import os
import subprocess
import sys
import time
from typing import Optional, Dict, Any

from toil.test.server.wes_client import WESClient

"""
A CWL runner that submits a workflow to a WES server, waits for it to finish,
and outputs the results.

Example usage with cwltest:

```
cwltest --verbose \
    --tool=python \
    --test=conformance_tests.yaml \
    -n=0-197 \
    --timeout=2400 \
    -j2 \
    -- \
    src/toil/test/server/wes_cwl_runner.py \
    --disableCaching \
    --clean=always \
    --logDebug
```
"""

logger = logging.getLogger(__name__)


def get_deps_from_cwltool(cwl_file, input_file, input_deps: bool = False):
    """
    Return a list of dependencies of the given workflow from cwltool.
    """
    if input_deps and not input_file:
        raise ValueError("Must provide an input_file if input_deps = True.")

    option = '--print-input-deps' if input_deps else '--print-deps'

    args = ['cwltool', option, '--relative-deps', 'cwd', cwl_file]
    if input_file:
        args.append(input_file)

    p = subprocess.run(args=args, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)

    result = p.stdout.decode()
    if not result:
        return []

    json_result: Dict[str, Any] = json.loads(result)

    deps = []

    def get_deps(obj: Any):
        for file in obj:
            if isinstance(file, dict) and "location" in file:
                loc = file.get("location")

                # Check directory
                if file.get("class") == "Directory":
                    if file.get("listing"):
                        get_deps(file.get("listing"))
                    else:
                        # no listing, so import all files in the directory
                        for folder, _, sub_files in os.walk(loc):
                            for sub_file in sub_files:
                                deps.append(os.path.join(folder, sub_file))
                else:
                    deps.append(file.get("location"))

                # check secondaryFiles
                if "secondaryFiles" in file:
                    get_deps(file.get("secondaryFiles"))

    get_deps(json_result.get("secondaryFiles", []))
    return deps


def submit_run(client: WESClient, cwl_file: str, input_file: Optional[str] = None) -> str:
    # First, get the list of files to attach to this workflow
    attachments = get_deps_from_cwltool(cwl_file, input_file)

    if input_file:
        attachments.extend(get_deps_from_cwltool(cwl_file, input_file, input_deps=True))

    # logging.warning(f"Files to import: {attachments}")
    # raise

    run_result: Dict[str, Any] = client.run(cwl_file, input_file, attachments=attachments)
    return run_result.get("run_id", None)


def poll_run(client: WESClient, run_id: str) -> bool:
    """ Return True if the given workflow run is in a finished state."""
    status_result = client.get_run_status(run_id)
    state = status_result.get("state")

    return state in ("COMPLETE", "CANCELING", "CANCELED", "EXECUTOR_ERROR", "SYSTEM_ERROR")


def print_logs_and_exit(client: WESClient, run_id: str) -> None:
    # cwltest checks for stdout, stderr, and return code
    data = client.get_run_log(run_id)

    outputs = json.dumps(data.get("outputs", {}), indent=4)
    exit_code = data.get("run_log", {}).get("exit_code", 1)

    sys.stdout.write(outputs)
    sys.exit(exit_code)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("cwl_file", type=str)
    parser.add_argument("input_file", nargs=argparse.REMAINDER)
    options, rest = parser.parse_known_args()

    # The last two arguments are the CWL workflow and input file.
    cwl_file = options.cwl_file
    input_file = None if len(options.input_file) == 0 else options.input_file[0]

    # Initialize client and run the workflow
    client = WESClient({
        "auth": None,
        "proto": "http",
        "host": "127.0.0.1:8080"
    })

    run_id = submit_run(client, cwl_file, input_file)
    assert run_id

    # logger.warning(f"Submitted run. Waiting for {run_id} to finish...")

    done = False
    while not done:
        time.sleep(1)
        done = poll_run(client, run_id)

    print_logs_and_exit(client, run_id)


if __name__ == '__main__':
    main()
