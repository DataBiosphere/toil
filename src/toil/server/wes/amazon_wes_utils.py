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

import zipfile
from typing import Optional

# These functions are licensed under the same Apache 2.0 license as Toil is,
# but they come from a repo with a NOTICE file, so we must preserve this notice
# text. We can print it if Toil ever grows a function to print third-party
# license and notice text. Note that despite what this notice says, the Apache
# license claims that it doesn't constitute part of the licensing terms.
NOTICE = """
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
"""

# The license also requires a note that these functions have been modified by us.
# We need to adjust them to hook into Toil's ways of talking to AWS.

def get_workflow_from_s3(s3_uri: str, localpath: str, workflow_type: str):
    """
    Retrieves a workflow from S3

    :param s3_uri: The S3 URI to the workflow (e.g. s3://bucketname/path/to/workflow.zip)
    :param localpath: The location on the local filesystem to download the workflow
    :param workflow_type: Type of workflow to expect (e.g. wdl, cwl, etc)

    :rtype: dict of `data` and `files`

    If the object is a generic file the file is set as `workflowSource`

    If the object is a `workflow.zip` file containing a single file, that file is set as `workflowSource`

    If the object is a `workflow.zip` file containing multiple files with a MANIFEST.json the MANIFEST is expected to have
      * a mainWorkflowURL property that provides a relative file path in the zip to a workflow file, which will be set as `workflowSource`
      * optionally, if an inputFileURLs property exists that provides a list of relative file paths in the zip to input.json, it will be used to set `workflowInputs`
      * optionally, if an optionFileURL property exists that provides a relative file path in the zip to an options.json file, it will be used to set `workflowOptions`

    If the object is a `workflow.zip` file containing multiple files without a MANIFEST.json
      * a `main` workflow file with an extension matching the workflow_type is expected and will be set as `workflowSource`
      * optionally, if `inputs*.json` files are found in the root level of the zip, they will be set as `workflowInputs(_\d)*` in the order they are found
      * optionally, if an `options.json` file is found in the root level of the zip, it will be set as `workflowOptions`

    If the object is a `workflow.zip` file containing multiple files, the `workflow.zip` file is set as `workflowDependencies`
    """
    s3 = boto3.resource("s3")

    u = urlparse(s3_uri)
    bucket = s3.Bucket(u.netloc)
    key = u.path[1:]

    data = dict()
    files = dict()

    if not key:
        raise RuntimeError("invalid or missing S3 object key")

    try:
        file = path.join(localpath, path.basename(key))
        bucket.download_file(key, file)
    except botocore.exceptions.ClientError as e:
        raise RuntimeError(f"invalid S3 object: {e}")

    if path.basename(file) == "workflow.zip":
        try:
            props = parse_workflow_zip_file(file, workflow_type)
        except Exception as e:
            raise RuntimeError(f"{s3_uri} is not a valid workflow.zip file: {e}")

        if props.get("data"):
            data.update(props.get("data"))

        if props.get("files"):
            files.update(props.get("files"))
    else:
        files["workflowSource"] = open(file, "rb")

    return {"data": data, "files": files}


def parse_workflow_zip_file(file, workflow_type):
    """
    Processes a workflow zip bundle

    :param file: String or Path-like path to a workflow.zip file
    :param workflow_type: String, type of workflow to expect (e.g. "wdl")

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
    data = dict()
    files = dict()

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
                    data.update(props.get("data"))

                if props.get("files"):
                    files.update(props.get("files"))

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


def parse_workflow_manifest_file(manifest_file):
    """
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
    data = dict()
    files = dict()
    with open(manifest_file, "rt") as f:
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


def workflow_manifest_url_to_path(url, parent_dir=None):
    relpath = url.path if not url.path.startswith("/") else url.path[1:]
    if parent_dir:
        return path.join(parent_dir, relpath)
    return relpath
