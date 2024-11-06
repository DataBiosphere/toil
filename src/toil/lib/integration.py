# Copyright (C) 2024 Regents of the University of California
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

"""
Contains functions for integrating Toil with external services such as
Dockstore.
"""

import hashlib
import logging
import os
import shutil
import sys
import tempfile
import zipfile
from typing import Any, Dict, List, Optional, Set, Tuple, cast

from urllib.parse import urlparse, unquote, quote
import requests

from toil.lib.retry import retry
from toil.lib.io import file_digest, robust_rmtree
from toil.version import baseVersion

logger = logging.getLogger(__name__)

# We manage a Requests session at the module level in case we're supposed to be
# doing cookies, and to send a sensible user agent.
# We expect the Toil and Python version to not be personally identifiable even
# in theory (someone might make a new Toil version first, buit there's no way
# to know for sure that nobody else did the same thing).
session = requests.Session()
session.headers.update({"User-Agent": f"Toil {baseVersion} on Python {'.'.join([str(v) for v in sys.version_info])}"})

def is_dockstore_workflow(workflow: str) -> bool:
    """
    Returns True if a workflow string smells Dockstore-y.

    Detects Dockstore page URLs and strings that could be Dockstore TRS IDs.
    """

    return workflow.startswith("https://dockstore.org/workflows/") or workflow.startswith("#workflow/")

def find_trs_spec(workflow: str) -> str:
    """
    Parse a Dockstore workflow URL or TSR ID to a string that is definitely a TRS ID.
    """

    if workflow.startswith("#workflow/"):
        # Looks like a Dockstore TRS ID already.
        # TODO: Does Dockstore guartantee we can recognize its TRS IDs like this?
        logger.debug("Workflow %s is a TRS specifier already", workflow)
        trs_spec = workflow
    else:
        # We need to get the right TRS ID from the Docstore URL
        parsed = urlparse(workflow)
        # TODO: We assume the Docksotre page URL structure and the TRS IDs are basically the same.
        page_path = unquote(parsed.path)
        if not page_path.startswith("/workflows/"):
            raise RuntimeError("Cannot parse Dockstore URL " + workflow)
        trs_spec = "#workflow/" + page_path[len("/workflows/"):]
        logger.debug("Translated %s to TRS: %s", workflow, trs_spec)

    return trs_spec

def parse_trs_spec(trs_spec: str) -> tuple[str, Optional[str]]:
    """
    Parse a TRS ID to workflow and optional version.
    """
    parts = trs_spec.split(':', 1)
    trs_workflow_id = parts[0]
    if len(parts) > 1:
        # The ID has the version we want after a colon
        trs_version = parts[1]
    else:
        # We don't know the version we want, we will have to pick one somehow.
        trs_version = None
    return trs_workflow_id, trs_version

@retry(errors=[requests.exceptions.ConnectionError])
def get_workflow_root_from_dockstore(workflow: str, supported_languages: Optional[set[str]] = None) -> str:
    """
    Given a Dockstore URL or TRS identifier, get the root WDL or CWL URL for the workflow.

    Accepts inputs like:

        - https://dockstore.org/workflows/github.com/dockstore-testing/md5sum-checker:master?tab=info
        - #workflow/github.com/dockstore-testing/md5sum-checker

    Assumes the input is actually one of the supported formats. See is_dockstore_workflow().

    TODO: Needs to handle multi-workflow files if Dockstore can.

    """

    if supported_languages is not None and len(supported_languages) == 0:
        raise ValueError("Set of supported languages must be nonempty if provided.")

    # Get the TRS id[:version] string from what might be a Dockstore URL
    trs_spec = find_trs_spec(workflow)
    # Parse out workflow and possible version
    trs_workflow_id, trs_version = parse_trs_spec(trs_spec)

    logger.debug("TRS %s parses to workflow %s and version %s", trs_spec, trs_workflow_id, trs_version)

    # Fetch the main TRS document.
    # See e.g. https://dockstore.org/api/ga4gh/trs/v2/tools/%23workflow%2Fgithub.com%2Fdockstore-testing%2Fmd5sum-checker
    trs_workflow_url = f"https://dockstore.org/api/ga4gh/trs/v2/tools/{quote(trs_workflow_id, safe='')}"
    trs_workflow_document = session.get(trs_workflow_url).json()

    # Make a map from version to version info. We will need the
    # "descriptor_type" array to find eligible languages, and the "url" field
    # to get the version's base URL.
    workflow_versions: dict[str, dict[str, Any]] = {}

    # We also check which we actually know how to run
    eligible_workflow_versions: set[str] = set()

    for version_info in trs_workflow_document.get("versions", []):
        version_name: str = version_info["name"]
        workflow_versions[version_name] = version_info
        version_languages: list[str] = version_info["descriptor_type"]
        if supported_languages is not None:
            # Filter to versions that have a language we know
            has_supported_language = False
            for language in version_languages:
                if language in supported_languages:
                    # TODO: Also use "descriptor_type_version" dict to make
                    # sure we support all needed language versions to actually
                    # use this workflow version.
                    has_supported_language = True
                    continue
            if not has_supported_language:
                # Can't actually run this one.
                continue
        eligible_workflow_versions.add(version_name)

    for default_version in ['main', 'master']:
        if trs_version is None and default_version in eligible_workflow_versions:
            # Fill in a version if the user didn't provide one.
            trs_version = default_version
            logger.debug("Defaulting to workflow version %s", default_version)
            break

    if trs_version is None and len(eligible_workflow_versions) == 1:
        # If there's just one version use that.
        trs_version = next(iter(eligible_workflow_versions))
        logger.debug("Defaulting to only eligible workflow version %s", trs_version)


    # If we don't like what we found we compose a useful error message.
    problems: list[str] = []
    if trs_version is None:
        problems.append(f"Workflow {workflow} does not specify a version")
    elif trs_version not in workflow_versions:
        problems.append(f"Workflow version {trs_version} from {workflow} does not exist")
    elif trs_version not in eligible_workflow_versions:
        message = f"Workflow version {trs_version} from {workflow} is not available"
        if supported_languages is not None:
            message += f" in any of: {', '.join(supported_languages)}"
        problems.append(message)
    if len(problems) > 0:
        if len(eligible_workflow_versions) == 0:
            message = "No versions of the workflow are available"
            if supported_languages is not None:
                message += f" in any of: {', '.join(supported_languages)}"
            problems.append(message)
        elif trs_version is None:
            problems.append(f"Add ':' and the name of a workflow version ({', '.join(eligible_workflow_versions)}) after '{trs_workflow_id}'")
        else:
            problems.append(f"Replace '{trs_version}' with one of ({', '.join(eligible_workflow_versions)})")
        raise RuntimeError("; ".join(problems))

    # Tell MyPy we now have a version, or we would have raised
    assert trs_version is not None

    # Select the language we will actually run
    chosen_version_languages: list[str] = workflow_versions[trs_version]["descriptor_type"]
    for candidate_language in chosen_version_languages:
        if supported_languages is None or candidate_language in supported_languages:
            language = candidate_language

    logger.debug("Going to use %s version %s in %s", trs_workflow_id, trs_version, language)
    trs_version_url = workflow_versions[trs_version]["url"]

    # Fetch the list of all the files
    trs_files_url = f"{trs_version_url}/{language}/files"
    logger.debug("Workflow files URL: %s", trs_files_url)
    trs_files_document = session.get(trs_files_url).json()

    # Find the information we need to ID the primary descriptor file
    primary_descriptor_path: Optional[str] = None
    primary_descriptor_hash_algorithm: Optional[str] = None
    primary_descriptor_hash: Optional[str] = None
    for file_info in trs_files_document:
        if file_info["file_type"] == "PRIMARY_DESCRIPTOR":
            primary_descriptor_path = file_info["path"]
            primary_descriptor_hash_algorithm = file_info["checksum"]["type"]
            primary_descriptor_hash = file_info["checksum"]["checksum"]
            break
    if primary_descriptor_path is None or primary_descriptor_hash is None or primary_descriptor_hash_algorithm is None:
        raise RuntimeError("Could not find a primary descriptor file for the workflow")
    primary_descriptor_basename = os.path.basename(primary_descriptor_path)

    # Work out how to compute the hash we are looking for. See
    # <https://github.com/ga4gh-discovery/ga4gh-checksum/blob/master/hash-alg.csv>
    # for the GA4GH names and <https://docs.python.org/3/library/hashlib.html>
    # for the Python names.
    #
    # TODO: We don't support the various truncated hash flavors or the other checksums not in hashlib.
    python_hash_name = primary_descriptor_hash_algorithm.replace("sha-", "sha").replace("blake2b-512", "blake2b").replace("-", "_")
    if python_hash_name not in hashlib.algorithms_available:
        raise RuntimeError(f"Primary descriptor is identified by a {primary_descriptor_hash_algorithm} hash but {python_hash_name} is not available in hashlib")

    # Figure out where to store the workflow. We don't want to deal with temp
    # dir cleanup since we don't want to run the whole workflow setup and
    # execution in a context manager. So we declare a cache.
    # Note that it's still not safe to symlink out of this cache since XDG
    # cache directories aren't guaranteed to be on shared storage.
    cache_base_dir = os.path.join(os.environ.get("XDG_CACHE_HOME", os.path.expanduser("~/.cache")), "toil/workflows")

    # Hash the workflow file list.
    hasher = hashlib.sha256()
    for file_info in sorted(trs_files_document, key=lambda rec: rec["path"]):
        hasher.update(file_info["path"].encode("utf-8"))
        hasher.update(b"\0")
        hasher.update(file_info["checksum"]["type"].encode("utf-8"))
        hasher.update(b"\0")
        hasher.update(file_info["checksum"]["checksum"].encode("utf-8"))
        hasher.update(b"\0")
    cache_workflow_dir = os.path.join(cache_base_dir, hasher.hexdigest())

    if os.path.exists(cache_workflow_dir):
        logger.debug("Workflow already cached at %s", cache_workflow_dir)
    else:
        # Need to download the workflow

        # Download the ZIP to a temporary file
        trs_zip_file_url = f"{trs_files_url}?format=zip"
        logger.debug("Workflow ZIP URL: %s", trs_zip_file_url)
        with tempfile.NamedTemporaryFile(suffix=".zip") as zip_file:
            # We want to stream the zip to a file, but when we do it with the Requests
            # file object like <https://stackoverflow.com/a/39217788> we don't get
            # Requests' decoding of gzip or deflate response encodings. Since this file
            # is already compressed the response compression can't help a lot anyway,
            # so we tell the server that we can't understand it.
            headers = {
                "Accept-Encoding": "identity",
                # Help Dockstore avoid serving ZIP with a JSON content type. See
                # <https://github.com/dockstore/dockstore/issues/6010>.
                "Accept": "application/zip"
            }
            # If we don't set stream=True, we can't actually read anything from the
            # raw stream, since Requests will have done it already.
            with session.get(trs_zip_file_url, headers=headers, stream=True) as response:
                response_content_length = response.headers.get("Content-Length")
                logger.debug("Server reports content length: %s", response_content_length)
                shutil.copyfileobj(response.raw, zip_file)
            zip_file.flush()

            logger.debug("Downloaded ZIP to %s", zip_file.name)

            # Unzip it to a directory next to where it will live
            os.makedirs(cache_base_dir, exist_ok=True)
            workflow_temp_dir = tempfile.mkdtemp(dir=cache_base_dir)
            with zipfile.ZipFile(zip_file.name, "r") as zip_ref:
                zip_ref.extractall(workflow_temp_dir)
            logger.debug("Extracted workflow ZIP to %s", workflow_temp_dir)

            # Try to atomically install into the cache
            try:
                os.rename(workflow_temp_dir, cache_workflow_dir)
                logger.debug("Moved workflow to %s", cache_workflow_dir)
            except OSError:
                # Collision. Someone else installed the workflow before we could.
                robust_rmtree(workflow_temp_dir)
                logger.debug("Workflow cached at %s by someone else while we were donwloading it", cache_workflow_dir)

    # Hunt throught he directory for a file with the right basename and hash
    found_path: Optional[str] = None
    for containing_dir, subdirectories, files in os.walk(cache_workflow_dir):
        for filename in files:
            if filename == primary_descriptor_basename:
                # This could be it. Open the file off disk and hash it with the right algorithm.
                file_path = os.path.join(containing_dir, filename)
                file_hash = file_digest(open(file_path, "rb"), python_hash_name).hexdigest()
                if file_hash == primary_descriptor_hash:
                    # This looks like the right file
                    logger.debug("Found candidate primary descriptor %s", file_path)
                    if found_path is not None:
                        # But there are multiple instances of it so we can't know which to run.
                        # TODO: Find out the right path from Dockstore somehow!
                        raise RuntimeError(f"Workflow contains multiple files named {primary_descriptor_basename} with {python_hash_name} hash {file_hash}: {found_path} and {file_path}")
                    # This is the first file with the right name and hash
                    found_path = file_path
                else:
                    logger.debug("Rejected %s because its %s hash %s is not %s", file_path, python_hash_name, file_hash, primary_descriptor_hash)
    if found_path is None:
        # We couldn't find the promised primary descriptor
        raise RuntimeError(f"Could not find a {primary_descriptor_basename} with {primary_descriptor_hash_algorithm} hash {primary_descriptor_hash}")

    return found_path

def resolve_workflow(workflow: str, supported_languages: Optional[set[str]] = None) -> str:
    """
    Find the real workflow URL or filename from a command line argument.

    Transform a workflow URL or path that might actually be a Dockstore page
    URL or TRS specifier to an actual URL or path to a workflow document.
    """

    if is_dockstore_workflow(workflow):
        # Ask Dockstore where to find Dockstore-y things
        resolved = get_workflow_root_from_dockstore(workflow, supported_languages=supported_languages)
        logger.info("Resolved Dockstore workflow %s to %s", workflow, resolved)
        return resolved
    else:
        # Pass other things through.
        return workflow











