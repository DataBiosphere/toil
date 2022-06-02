import os
import sys
import time
import json
import logging
import argparse
import requests
import subprocess
import ruamel.yaml
import schema_salad

from io import BytesIO
from base64 import b64encode
from urllib.parse import urlparse, urldefrag, urljoin
from typing import Optional, Dict, Any, List, Tuple, Iterable, cast

from werkzeug.utils import secure_filename
from toil.wdl.utils import get_version as get_wdl_version
from wes_client.util import WESClient, wes_reponse as wes_response  # type: ignore


"""
A CWL runner that submits a workflow to a WES server, waits for it to finish,
and outputs the results.


Environment variables:
+----------------------------------+----------------------------------------------------+
| TOIL_WES_ENDPOINT                | URL to the WES server to use for this WES-based    |
|                                  | CWL runner.                                        |
+----------------------------------+----------------------------------------------------+
| TOIL_WES_USER                    | Username to use with HTTP Basic Authentication to  |
|                                  | log into the WES server.                           |
+----------------------------------+----------------------------------------------------+
| TOIL_WES_PASSWORD                | Password to use with HTTP Basic Authentication to  |
|                                  | log into the WES server.                           |
+----------------------------------+----------------------------------------------------+


Example usage with cwltest:

```
cwltest --verbose \
    --tool=toil-wes-cwl-runner \
    --test=src/toil/test/cwl/spec_v12/conformance_tests.yaml \
    -n=1-50 \
    --timeout=2400 \
    -j2 \
    -- \
    --wes_endpoint=http://localhost:8080 \
    --disableCaching \
    --clean=always \
    --logDebug
```
"""

logger = logging.getLogger(__name__)


def generate_attachment_path_name(base_dir: str, file_path: str) -> str:
    """
    Generate a relative path under base_dir as input fields or as attachment
    keys.

    This is to detect attachment path names that are above the CWL workflow file
    and modify the path names in both the File or Directory objects in
    workflow_params and the attachment key.

    For example, for the following CWL workflow where "hello.yaml" references
    a file "message.txt",

        ~/toil/workflows/hello.cwl
        ~/toil/input_files/hello.yaml
        ~/toil/input_files/message.txt

    This may be run with the command:
        toil-wes-cwl-runner hello.cwl ../input_files/hello.yaml

    Where "message.txt" is resolved to "../input_files/message.txt".

    For the workflow attachment "../input_files/message.txt", we must replace
    this relative path to something else that is not above the CWL workflow
    file, otherwise the Toil WES server will drop all the traversals up to
    contain attachments within its "execution" folder.

    TODO: we need to find a way to call this function and replace the relative
     paths referenced inside the CWL workflow directly.
     e.g.: https://github.com/common-workflow-language/cwl-v1.2/blob/1.2.1_proposed/tests/iwd/iwd-fileobjs1.cwl#L7-L11

    :param base_dir: The local directory where the CWL file is located.
    :param file_path: The path to the attachment file. May be an absolute path
                      or different from the CWL workflow location.
    """

    # Make sure we are working with a relative path to the CWL workflow file
    if file_path.startswith("file://"):
        file_path = file_path[7:]
    file_path = os.path.relpath(file_path, base_dir)

    # TODO: don't just drop the up traversals
    #  Ideally we should make the output unique, but we should also make sure that the same call to this function
    #  will have the same output.
    return os.path.join(*[str(secure_filename(p)) for p in file_path.split("/") if p not in ("", ".", "..")])


class WESClientWithWorkflowEngineParameters(WESClient):  # type: ignore
    """
    A modified version of the WESClient from the wes-service package that
    includes workflow_engine_parameters support.

    TODO: Propose a PR in wes-service to include workflow_engine_params.
    """
    def __init__(self, endpoint: str, auth: Optional[Tuple[str, str]] = None) -> None:
        """
        :param endpoint: The http(s) URL of the WES server. Must include the
                         protocol.
        :param auth: Authentication information that will be attached to every
                     request to the WES server.
        """
        proto, host = endpoint.split("://")
        super().__init__({
            # TODO: use the auth argument in requests.post so we don't need to encode it ourselves
            "auth": {"Authorization": "Basic " + b64encode(f"{auth[0]}:{auth[1]}".encode("utf-8")).decode("utf-8")} if auth else {},
            "proto": proto,
            "host": host
        })

    def get_version(self, extension: str, workflow_file: str) -> str:
        """Determines the version of a .py, .wdl, or .cwl file."""
        # TODO: read from the web?

        if workflow_file.startswith("file://"):
            workflow_file = workflow_file[7:]

        if extension == "py":
            return "3.8"
        elif extension == "cwl":
            with open(workflow_file) as f:
                return str(ruamel.yaml.safe_load(f)["cwlVersion"])
        elif extension == "wdl":
            with open(workflow_file) as f:
                return get_wdl_version(f)
        else:
            raise RuntimeError(f"Invalid workflow extension: {extension}.")

    def parse_and_modify_params(self, base_dir: str, workflow_params_file: str) -> Dict[str, Any]:
        """
        Loop through files in the input workflow parameters json and modify
        the paths to not include relative paths to parent directories.

        :param base_dir: The local directory where the CWL file is located.
        :param workflow_params_file: The URL or path to the CWL input file.
        """
        loader = schema_salad.ref_resolver.Loader(
            {"location": {"@type": "@id"}}
        )

        # recursive types may be complicated for MyPy to deal with
        workflow_params: Any
        workflow_params, _ = loader.resolve_ref(workflow_params_file, checklinks=False)

        def replace_paths(obj: Any) -> None:
            for file in obj:
                if isinstance(file, dict) and "location" in file:
                    loc = file.get("location")
                    if isinstance(loc, str) and urlparse(loc).scheme in ("file", ""):
                        file["location"] = generate_attachment_path_name(base_dir, loc)
                    # recursively find all imported files
                    if "secondaryFiles" in file:
                        replace_paths(file.get("secondaryFiles"))
                elif isinstance(file, dict):
                    replace_paths(file.values())
                elif isinstance(file, list):
                    replace_paths(file)

        replace_paths(workflow_params.values())
        return cast(Dict[str, Any], workflow_params)

    def build_wes_request(
            self,
            workflow_file: str,
            workflow_params_file: Optional[str],
            attachments: Optional[List[str]],
            workflow_engine_parameters: Optional[List[str]] = None
    ) -> Tuple[Dict[str, str], Iterable[Tuple[str, Tuple[str, BytesIO]]]]:
        """
        Build the workflow run request to submit to WES.

        :param workflow_file: The path or URL to the CWL workflow document.
                              Only file:// URL supported at the moment.
        :param workflow_params_file: The path or URL to the CWL input file.
        :param attachments: A list of local paths to files that will be uploaded
                            to the server.
        :param workflow_engine_parameters: A list of engine parameters to set
                                           along with this workflow run.

        :returns: A dictionary of parameters as the body of the request, and an
                  iterable for the pairs of filename and file contents to upload
                  to the server.
        """

        local_workflow_file = urlparse(workflow_file).scheme in ("", "file")

        if workflow_file.startswith("file://"):
            workflow_file = workflow_file[7:]

        # Define the base directory in which file dependencies should be relative to.
        # This is used so that we wouldn't generate file names that include the entire absolute path.
        # TODO: if workflow_file is not local, we should make the workflow_params_file the base_dir. If neither file
        #  is local, we wouldn't need this.
        base_dir = os.path.dirname(workflow_file)

        # Read from the workflow_param file and parse it into a dict
        if workflow_params_file:
            workflow_params = self.parse_and_modify_params(base_dir, workflow_params_file)
        else:
            workflow_params = {}

        # Initialize the basic parameters for the run request

        wf_url, frag = urldefrag(workflow_file)

        # For local CWL files, strip out paths from the local directory
        if local_workflow_file:
            workflow_file = os.path.basename(workflow_file)

        workflow_type = wf_url.lower().split(".")[-1]  # Grab the file extension
        workflow_type_version = self.get_version(workflow_type, wf_url)
        data: Dict[str, str] = {
            "workflow_url": workflow_file,
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

        if attachments is None:
            attachments = []

        # Upload the CWL workflow file if it is local
        if local_workflow_file:
            attachments.append(wf_url)

        # Prepare attachments and generate new path names that match the modified paths in workflow_params
        workflow_attachments = []
        for file in attachments:
            with open(file, "rb") as f:
                filename = generate_attachment_path_name(base_dir, file)
                workflow_attachments.append((filename, BytesIO(f.read())))

        return data, [("workflow_attachment", val) for val in workflow_attachments]

    def run_with_engine_options(
            self,
            workflow_file: str,
            workflow_params_file: Optional[str],
            attachments: Optional[List[str]],
            workflow_engine_parameters: Optional[List[str]]
    ) -> Dict[str, Any]:
        """
        Composes and sends a post request that signals the WES server to run a
        workflow.

        :param workflow_file: The path to the CWL workflow document.
        :param workflow_params_file: The path to the CWL input file.
        :param attachments: A list of local paths to files that will be uploaded
                            to the server.
        :param workflow_engine_parameters: A list of engine parameters to set
                                           along with this workflow run.

        :return: The body of the post result as a dictionary.
        """
        data, files = self.build_wes_request(workflow_file,
                                             workflow_params_file,
                                             attachments,
                                             workflow_engine_parameters)
        post_result = requests.post(
            urljoin(f"{self.proto}://{self.host}", "/ga4gh/wes/v1/runs"),
            data=data,
            files=files,
            headers=self.auth,
        )

        return cast(Dict[str, Any], wes_response(post_result))


def get_deps_from_cwltool(cwl_file: str, input_file: Optional[str] = None) -> List[str]:
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

    def get_deps(obj: Any) -> None:
        """
        A recursive function to add file dependencies from the cwltool output to
        the deps list. For directory objects without listing, contents of the
        entire directory will be included.
        """
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

    This function also attempts to find the attachments from the CWL workflow
    and its input file, and attach them to the WES run request.

    :param client: The WES client.
    :param cwl_file: The path to the CWL workflow document.
    :param input_file: The path to the CWL input file.
    :param engine_options: A list of engine parameters to set along with this
                           workflow run.
    """
    # First, get the list of files to attach to this workflow
    attachments = get_deps_from_cwltool(cwl_file)

    if input_file:
        attachments.extend(get_deps_from_cwltool(cwl_file, input_file))

    run_result: Dict[str, Any] = client.run_with_engine_options(
        cwl_file,
        input_file,
        attachments=attachments,
        workflow_engine_parameters=engine_options)
    return run_result.get("run_id", None)


def poll_run(client: WESClientWithWorkflowEngineParameters, run_id: str) -> bool:
    """ Return True if the given workflow run is in a finished state."""
    status_result = client.get_run_status(run_id)
    state = status_result.get("state")

    return state in ("COMPLETE", "CANCELING", "CANCELED", "EXECUTOR_ERROR", "SYSTEM_ERROR")


def print_logs_and_exit(client: WESClientWithWorkflowEngineParameters, run_id: str) -> None:
    """
    Fetch the workflow logs from the WES server, print the results, then exit
    the program with the same exit code as the workflow run.

    :param client: The WES client.
    :param run_id: The run_id of the target workflow.
    """
    data = client.get_run_log(run_id)

    outputs = json.dumps(data.get("outputs", {}), indent=4)
    exit_code = data.get("run_log", {}).get("exit_code", 1)

    sys.stdout.write(outputs)
    sys.exit(exit_code)


def main() -> None:
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

    # For security reasons, username and password can only come from environment variables
    wes_user = os.environ.get("TOIL_WES_USER", None)
    wes_password = os.environ.get("TOIL_WES_PASSWORD", None)

    client = WESClientWithWorkflowEngineParameters(
        endpoint=endpoint,
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
