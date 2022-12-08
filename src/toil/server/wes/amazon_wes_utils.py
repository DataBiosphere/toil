# Copyright (C) 2022 Regents of the University of California
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# This file contains WES utility functions borrowed from the Amazon Genomics
# CLI adapter codebase, at a6cc9904cd2d1899f7f64bf3c024a242c1a33749, to allow
# Toil to function as an AGC-compatible WES implementation without the use of a
# separate adapter. These functions help Toil use the WES semantics that AGC
# expects.

import json
import logging
import os
import sys
import zipfile
from os import path
from typing import IO, Any, Dict, List, Optional, Union, cast

if sys.version_info >= (3, 8):
    from typing import TypedDict
else:
    from typing_extensions import TypedDict

from urllib.parse import ParseResult, urlparse

from toil.bus import JobStatus
from toil.server.wes.abstract_backend import \
    MalformedRequestException as InvalidRequestError
from toil.server.wes.abstract_backend import TaskLog

logger = logging.getLogger(__name__)

# These functions are licensed under the same Apache 2.0 license as Toil is,
# but they come from a repo with a NOTICE file, so we must preserve this notice
# text. We can print it if Toil ever grows a function to print third-party
# license and notice text. Note that despite what this notice says, the Apache
# license claims that it doesn't constitute part of the licensing terms.
NOTICE = """
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
"""

# The license also requires a note that these functions have been modified by us.
# They needed to be modified to typecheck.

# The official spec we are working with here is: https://aws.github.io/amazon-genomics-cli/docs/concepts/workflows/#multi-file-workflows

class WorkflowPlan(TypedDict):
    """
    These functions pass around dicts of a certain type, with `data` and `files` keys.
    """
    data: "DataDict"
    files: "FilesDict"

class DataDict(TypedDict, total=False):
    """
    Under `data`, there can be:
    * `workflowUrl` (required if no `workflowSource`): URL to main workflow code.
    """
    workflowUrl: str

class FilesDict(TypedDict, total=False):
    """
    Under `files`, there can be:
    * `workflowSource` (required if no `workflowUrl`): Open binary-mode file for the main workflow code.
    * `workflowInputFiles`: List of open binary-mode file for input files. Expected to be JSONs.
    * `workflowOptions`: Open binary-mode file for a JSON of options sent along with the workflow.
    * `workflowDependencies`: Open binary-mode file for the zip the workflow came in, if any.
    """
    workflowSource: IO[bytes]
    workflowInputFiles: List[IO[bytes]]
    workflowOptions: IO[bytes]
    workflowDependencies: IO[bytes]

def parse_workflow_zip_file(file: str, workflow_type: str) -> WorkflowPlan:
    r"""
    Processes a workflow zip bundle

    :param file: String or Path-like path to a workflow.zip file
    :param workflow_type: String, extension of workflow to expect (e.g. "wdl")

    :rtype: dict of `data` and `files`

    If the zip only contains a single file, that file is set as `workflowSource`

    If the zip contains multiple files with a MANIFEST.json file, the MANIFEST is used to determine
    appropriate `data` and `file` arguments. (See: parse_workflow_manifest_file())

    If the zip contains multiple files without a MANIFEST.json file:
      * a `main` workflow file with an extension matching the workflow_type is expected and will be set as `workflowSource`
      * optionally, if `inputs*.json` files are found in the root level of the zip, they will be set as `workflowInputs(_\d)*` in the order they are found
      * optionally, if an `options.json` file is found in the root level of the zip, it will be set as `workflowOptions`

    If the zip contains multiple files, the original zip is set as `workflowDependencies`
    """
    data: DataDict = dict()
    files: FilesDict = dict()

    wd = path.dirname(file)
    with zipfile.ZipFile(file) as zip:
        zip.extractall(wd)

        contents = zip.namelist()
        if not contents:
            raise RuntimeError("empty workflow.zip")

        if len(contents) == 1:
            # single file workflow
            files["workflowSource"] = open(path.join(wd, contents[0]), "rb")

        else:
            # multifile workflow
            if "MANIFEST.json" in contents:
                props = parse_workflow_manifest_file(path.join(wd, "MANIFEST.json"))

                if props.get("data"):
                    data.update(props["data"])

                if props.get("files"):
                    files.update(props["files"])

            else:
                if not f"main.{workflow_type.lower()}" in contents:
                    raise RuntimeError(f"'main.{workflow_type}' file not found")

                files["workflowSource"] = open(
                    path.join(wd, f"main.{workflow_type.lower()}"), "rb"
                )

                input_files = [f for f in contents if f.startswith("inputs")]
                if input_files:
                    if not files.get("workflowInputFiles"):
                        files["workflowInputFiles"] = []

                    for input_file in input_files:
                        files[f"workflowInputFiles"] += [
                            open(path.join(wd, input_file), "rb")
                        ]

                if "options.json" in contents:
                    files["workflowOptions"] = open(path.join(wd, "options.json"), "rb")

            # add the original zip bundle as a workflow dependencies file
            files["workflowDependencies"] = open(file, "rb")

    return {"data": data, "files": files}


def parse_workflow_manifest_file(manifest_file: str) -> WorkflowPlan:
    r"""
    Reads a MANIFEST.json file for a workflow zip bundle

    :param manifest_file: String or Path-like path to a MANIFEST.json file

    :rtype: dict of `data` and `files`

    MANIFEST.json is expected to be formatted like:
    .. code-block:: json
       {
           "mainWorkflowURL": "relpath/to/workflow",
           "inputFileURLs": [
               "relpath/to/input-file-1",
               "relpath/to/input-file-2",
               ...
           ],
           "optionsFileURL" "relpath/to/option-file
       }

    The `mainWorkflowURL` property that provides a relative file path in the zip to a workflow file, which will be set as `workflowSource`

    The inputFileURLs property is optional and provides a list of relative file paths in the zip to input.json files. The list is assumed
    to be in the order the inputs should be applied - e.g. higher list index is higher priority. If present, it will be used to set
    `workflowInputs(_\d)` arguments.

    The optionsFileURL property is optional and  provides a relative file path in the zip to an options.json file. If present, it will be
    used to set `workflowOptions`.

    """
    data: DataDict = dict()
    files: FilesDict = dict()
    with open(manifest_file) as f:
        manifest = json.loads(f.read())

    u = urlparse(manifest["mainWorkflowURL"])
    if not u.scheme or u.scheme == "file":
        # expect "/path/to/file" or "file:///path/to/file"
        # root is relative to the zip root
        files["workflowSource"] = open(
            workflow_manifest_url_to_path(u, path.dirname(manifest_file)), "rb"
        )

    else:
        data["workflowUrl"] = manifest["mainWorkflowUrl"]

    if manifest.get("inputFileURLs"):
        if not files.get("workflowInputFiles"):
            files["workflowInputFiles"] = []

        for url in manifest["inputFileURLs"]:
            u = urlparse(url)
            if not u.scheme or u.scheme == "file":
                files[f"workflowInputFiles"] += [
                    open(
                        workflow_manifest_url_to_path(u, path.dirname(manifest_file)),
                        "rb",
                    )
                ]

            else:
                raise InvalidRequestError(
                    f"unsupported input file url scheme for: '{url}'"
                )

    if manifest.get("optionsFileURL"):
        u = urlparse(manifest["optionsFileURL"])
        if not u.scheme or u.scheme == "file":
            files["workflowOptions"] = open(
                workflow_manifest_url_to_path(u, path.dirname(manifest_file)), "rb"
            )
        else:
            raise InvalidRequestError(
                f"unsupported option file url scheme for: '{manifest['optionFileURL']}'"
            )

    return {"data": data, "files": files}


def workflow_manifest_url_to_path(url: ParseResult, parent_dir: Optional[str] = None) -> str:
    """
    Interpret a possibly-relative parsed URL, relative to the given parent directory.
    """
    relpath = url.path if not url.path.startswith("/") else url.path[1:]
    if parent_dir:
        return path.join(parent_dir, relpath)
    return relpath

# This one is all UCSC code
def task_filter(task: TaskLog, job_status: JobStatus) -> Optional[TaskLog]:
    """
    AGC requires task names to be annotated with an AWS Batch job ID that they
    were run under. If it encounters an un-annotated task name, it will crash.
    See <https://github.com/aws/amazon-genomics-cli/issues/494>.

    This encodes the AWSBatchJobID annotation, from the AmazonBatchBatchSystem,
    into the task name of the given task, and returns the modified task. If no
    such annotation is available, the task is censored and None is returned.
    """

    # Get the Batch ID for the task
    batch_id = job_status.external_batch_id

    if batch_id is None:
        return None

    modified_task = dict(task)
    # Tack the batch ID onto the end of the name with the required separator
    modified_task["name"] = "|".join([cast(str, modified_task.get("name", "")), batch_id])
    logger.info("Transformed task %s to %s", task, modified_task)
    return modified_task
