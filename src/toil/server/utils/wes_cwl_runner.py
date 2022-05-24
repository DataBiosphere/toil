import argparse
import json
import logging
import os
import subprocess
import sys
import time
from io import BytesIO
from urllib.parse import urlparse, urldefrag

import requests
import ruamel.yaml
from typing import Optional, Dict, Any, List, Tuple
from toil.wdl.utils import get_version as get_wdl_version

from werkzeug.utils import secure_filename
from wes_client.util import WESClient, wes_reponse as wes_response


"""
A CWL runner that submits a workflow to a WES server, waits for it to finish,
and outputs the results.

Example usage with cwltest:

```
cwltest --verbose \
    --tool=python \
    --test=src/toil/test/cwl/spec_v12/conformance_tests.yaml \
    -n=306 \
    --timeout=2400 \
    -j1 \
    -- \
    src/toil/test/server/wes_cwl_runner.py \
    --wes_endpoint=http://localhost:8080 \
    --wes_user=test \
    --wes_password=test \
    --disableCaching \
    --clean=always \
    --logDebug
```
"""

logger = logging.getLogger(__name__)


class WESClientWithWorkflowEngineParameters(WESClient):
    """
    A modified version of the WESClient from the wes-service package that
    includes workflow_engine_parameters support.
    """
    def __init__(self, base_url: str, auth: Optional[Tuple[str, str]] = None):
        proto, host = base_url.split("://")  # TODO: use urlparse
        super().__init__({
            "auth": auth,
            "proto": proto,
            "host": host
        })

    @staticmethod
    def get_version(extension: str, workflow_file: str):
        """Determines the version of a .py, .wdl, or .cwl file."""
        if extension == "py":
            return "3.8"
        elif extension == "cwl":
            with open(workflow_file) as f:
                return ruamel.yaml.safe_load(f)["cwlVersion"]
        elif extension == "wdl":
            with open(workflow_file) as f:
                return get_wdl_version(f)
        else:
            raise RuntimeError(f"Invalid workflow extension: {extension}.")

    @staticmethod
    def parse_params(workflow_params: Dict[str, Any]) -> None:
        """
        Loop through files in the input workflow parameters json and make sure
        the paths do not include relative paths to parent directories.
        """

        def secure_path(path: str) -> str:
            return os.path.join(*[str(secure_filename(p)) for p in path.split("/") if p not in ("", ".", "..")])

        def replace_paths(obj: Any) -> None:
            for file in obj:
                if isinstance(file, dict) and "location" in file:
                    loc = file.get("location")
                    if isinstance(loc, str) and urlparse(loc).scheme in ("file", ""):
                        file["location"] = secure_path(loc)

                    if "secondaryFiles" in file:
                        replace_paths(file.get("secondaryFiles"))

        replace_paths(workflow_params.values())

    @staticmethod
    def build_wes_request(workflow_url: str,
                          workflow_params_url: str,
                          attachments: List[str],
                          workflow_engine_parameters: Optional[List[str]] = None):

        # Read from the workflow_param file and parse it into a dict
        if workflow_params_url:
            with open(workflow_params_url, "r") as f:
                if workflow_params_url.endswith((".yaml", ".yml")):
                    workflow_params = ruamel.yaml.safe_load(f)
                elif workflow_params_url.endswith(".json"):
                    workflow_params = json.load(f)
                else:
                    raise ValueError(
                        f"Unsupported file type for workflow_params: '{os.path.basename(workflow_params_url)}'")
        else:
            workflow_params = {}
        WESClientWithWorkflowEngineParameters.parse_params(workflow_params)

        # Initialize the basic parameters for the run request
        wf_url, frag = urldefrag(workflow_url)
        workflow_type = wf_url.lower().split(".")[-1]  # Grab the file extension
        workflow_type_version = WESClientWithWorkflowEngineParameters.get_version(workflow_type, wf_url)
        data = {
            "workflow_url": os.path.basename(workflow_url),
            "workflow_params": json.dumps(workflow_params),
            "workflow_type": workflow_type,
            "workflow_type_version": workflow_type_version
        }

        # Convert engine arguments into a JSON object
        if workflow_engine_parameters:
            params = {}
            for param in workflow_engine_parameters:
                if '=' not in param:  # flags like "--logDebug"
                    k, v = param, None
                else:
                    k, v = param.split('=', 1)
                params[k] = v
            data["workflow_engine_parameters"] = json.dumps(params)

        # Deal with workflow attachments
        base = os.path.dirname(wf_url)
        attachments.append(wf_url)

        workflow_attachments = []
        for file in attachments:
            with open(file, "rb") as f:
                rel: str = os.path.relpath(file, base)
                if '../' in rel:
                    # when inputs are in a different directory from the workflow
                    rel = os.path.basename(file)
                workflow_attachments.append((rel, BytesIO(f.read())))

        return data, (("workflow_attachment", val) for val in
                      workflow_attachments)

    def run_with_engine_options(self, workflow_file, secondary_file, attachments, engine_options):
        """
        Composes and sends a post request that signals the WES server to run a
        workflow.

        :param workflow_file: A local/http/https path to a CWL/WDL/python
                              workflow file.
        :param secondary_file: A local path to a json or yaml file.
        :param attachments: A list of local paths to files that will be
                            uploaded to the server.
        :param engine_options: A list of engine options to attach.

        :return: The body of the post result as a dictionary.
        """
        data, files = WESClientWithWorkflowEngineParameters.build_wes_request(
            workflow_file, secondary_file, attachments, engine_options)
        post_result = requests.post(
            f"{self.proto}://{self.host}/ga4gh/wes/v1/runs",
            data=data,
            files=files,
            auth=self.auth,
        )

        return wes_response(post_result)


def get_deps_from_cwltool(cwl_file: str, input_file: Optional[str] = None):
    """
    Return a list of dependencies of the given workflow from cwltool.

    :param cwl_file: The CWL file.
    :param input_file: Omit to get the dependencies from the CWL file. If set,
                       this returns the dependencies from the input file.
    """

    option = '--print-input-deps' if input_file else '--print-deps'

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


def submit_run(client: WESClientWithWorkflowEngineParameters,
               cwl_file: str,
               input_file: Optional[str] = None,
               engine_options: Optional[List[str]] = None) -> str:
    """
    Given a CWL file, its input files, and an optional list of engine options,
    submit the CWL workflow to the WES server via the WES client.
    """
    # First, get the list of files to attach to this workflow
    attachments = get_deps_from_cwltool(cwl_file, input_file)

    if input_file:
        attachments.extend(get_deps_from_cwltool(cwl_file, input_file))

    run_result: Dict[str, Any] = client.run_with_engine_options(
        cwl_file,
        input_file,
        attachments=attachments,
        engine_options=engine_options)
    return run_result.get("run_id", None)


def poll_run(client: WESClientWithWorkflowEngineParameters, run_id: str) -> bool:
    """ Return True if the given workflow run is in a finished state."""
    status_result = client.get_run_status(run_id)
    state = status_result.get("state")

    return state in ("COMPLETE", "CANCELING", "CANCELED", "EXECUTOR_ERROR", "SYSTEM_ERROR")


def print_logs_and_exit(client: WESClientWithWorkflowEngineParameters, run_id: str, download_files: bool = False) -> None:
    """
    Fetch the workflow logs from the WES server and print the results.

    :param client: The WES client.
    :param run_id: The run_id of the target workflow.
    :param download_files: If True, download the output files to the local
                           filesystem.
    """
    data = client.get_run_log(run_id)

    outputs = json.dumps(data.get("outputs", {}), indent=4)
    exit_code = data.get("run_log", {}).get("exit_code", 1)

    sys.stdout.write(outputs)
    sys.exit(exit_code)


def main():
    parser = argparse.ArgumentParser(description="A CWL runner that runs workflows through WES.")

    # the first two positional arguments are the CWL file and its input file
    parser.add_argument("cwl_file", type=str)
    parser.add_argument("input_file", type=str, nargs="?", default=None)
    # arguments used by the WES runner
    parser.add_argument("--wes_endpoint",
                        default=os.environ.get("TOIL_WES_ENDPOINT", "http://localhost:8080"),
                        help="The http(s) URL of the WES server.  (default: %(default)s)")
    # the rest of the arguments are passed as engine options to the WES server
    options, rest = parser.parse_known_args()

    cwl_file = options.cwl_file
    input_file = options.input_file

    # Initialize client and run the workflow
    endpoint = options.wes_endpoint
    wes_user = os.environ.get("TOIL_WES_USER", None)
    wes_password = os.environ.get("TOIL_WES_USER", None)

    client = WESClientWithWorkflowEngineParameters(
        base_url=endpoint,
        auth=(wes_user, wes_password) if wes_user and wes_password else None)

    run_id = submit_run(client, cwl_file, input_file, engine_options=rest)
    assert run_id

    done = False
    while not done:
        time.sleep(1)
        done = poll_run(client, run_id)

    print_logs_and_exit(client, run_id)


if __name__ == '__main__':
    main()
