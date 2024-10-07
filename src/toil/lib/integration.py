import logging
import os
import shutil
import sys
import zipfile
from typing import Dict, List, Optional, Set, cast

from urllib.parse import urlparse, unquote, quote
import requests

from toil.lib.retry import retry

logger = logging.getLogger(__name__)

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

def parse_trs_spec(trs_spec: str) -> Tuple[str, Optional[str]]:
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

@retry(errors=[requests.exceptions.ConnectionError])
def get_workflow_root_from_dockstore(workflow: str, supported_languages: Optional[Set[str]] = None) -> str:
    """
    Given a Dockstore URL or TRS identifier, get the root WDL or CWL URL for the workflow.
    
    Accepts inputs like:

        - https://dockstore.org/workflows/github.com/dockstore-testing/md5sum-checker:master?tab=info
        - #workflow/github.com/dockstore-testing/md5sum-checker

    Assumes the input is actually one of the supported formats. See is_dockstore_workflow().

    TODO: Needs to handle multi-workflow files if Dockstore can.

    """

    # Get the TRS id[:version] string from what might be a Dockstore URL
    trs_spec = find_trs_spec(workflow)
    # Parse out workflow and possible version
    trs_workflow_id, trs_version = parse_trs_spec(trs_spec)

    logger.debug("TRS %s parses to workflow %s and version %s", trs_spec, trs_workflow_id, trs_version)

    # Fetch the main TRS document.
    # See e.g. https://dockstore.org/api/ga4gh/trs/v2/tools/%23workflow%2Fgithub.com%2Fdockstore-testing%2Fmd5sum-checker
    trs_workflow_url = f"https://dockstore.org/api/ga4gh/trs/v2/tools/{quote(trs_workflow_id, safe='')}"
    trs_workflow_document = requests.get(trs_workflow_url).json()

    # Make a map from version to version info. We will need the
    # "descriptor_type" array to find eligible languages, and the "url" field
    # to get the version's base URL.
    workflow_versions: Dict[str, Dict[str, Any]] = {}

    # We also check which we actually know how to run
    eligible_workflow_versions: Set(str) = set()

    for version_info in trs_workflow_document.get("versions", []):
        version_name: str = version_info["name"]
        workflow_versions[version_name] = version_info
        version_languages: List[str] = version_info["descriptor_type"]
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

        # TODO: Why is there an array of descriptor types?
        version_to_language[version_info["name"]] = version_info["descriptor_type"][0]
        logger.debug("Workflow version %s is written in %s", version_info["name"], version_info["descriptor_type"][0])

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
    problems: List[str] = []
    if trs_version is None:
        problems.append(f"Workflow {workflow} does not specify a version")
    elif trs_version not in workflow_versions:
        problems.append(f"Workflow version {trs_version} from {workflow} does not exist")
    elif trs_version not in eligible_workflow_versions:
        problems.append(f"Workflow version {trs_version} from {workflow} is not available in any of: {', '.join(supported_languages)}")
    if len(problems) > 0:
        if len(eligible_workflow_versions) == 0:
            problems.append(f"No versions of the workflow are availalbe in: {', '.join(supported_languages)}")
        elif trs_version is None:
            problems.append(f"Add ':' and the name of a workflow version ({', '.join(eligible_workflow_versions)}) after '{trs_workflow_id}'")
        else:
            problems.append(f"Replace '{trs_version}' with one of ({', '.join(eligible_workflow_versions)})")
        raise RuntimeError("; ".join(problems))
    
    # Select the language we will actually run
    chosen_version_languages: List[str] = workflow_versions[trs_version]["descriptor_type"]
    for candidate_language in chosen_version_languages:
        if supported_languages is None or candidate_language in supported_languages:
            language = candidate_language

    logger.debug("Going to use %s version %s in %s", trs_workflow_id, trs_version, language)
    trs_version_url = workflow_versions[trs_version]["url"]

    # Fetch the list of all the files
    trs_files_url = f"{trs_version_url}/{language}/files"
    logger.debug("Workflow files URL: %s", trs_files_url)
    trs_files_document = requests.get(trs_files_url).json()
    
    # Find the information we need to ID the primary descriptor file
    primary_descriptor_path: Optional[str] = None
    primary_descriptor_hash: Optional[str] = None
    primary_descriptor_hash_algorithm: Optional[str] = None
    for file_info in trs_files_document:
        if file_info["file_type"] == "PRIMARY_DESCRIPTOR":
            primary_descriptor_path = file_info["path"]
            primary_descriptor_hash = file_info["checksum"]["checksum"]
            primary_descriptor_hash_algorithm = file_info["checksum"]["type"]
            break
    if primary_descriptor_path is None or primary_descriptor_hash is None or primary_descriptor_hash_algorithm is None:
        raise RuntimeError("Could not find a primary descriptor file for the workflow")

    # Download the ZIP to a temporary file
    trs_zip_file_url = f"{trs_files_url}?format=zip"
    with tempfile.NamedTemporaryFile(suffix=".zip") as zip_file:
        # We want to stream the zip to a file, but when we do it with the Requests
        # file object like <https://stackoverflow.com/a/39217788> we don't get
        # Requests' decoding of gzip or deflate response encodings. Since this file
        # is already compressed the response compression can't help a lot anyway,
        # so we tell the server that we can't understand it.
        headers = {
            "Accept-Encoding": "identity",
            "Accept": "application/zip" # Help Dockstore avoid serving ZIP with a JSON content type. See <https://github.com/dockstore/dockstore/issues/6010>.
        }
        with requests.get(trs_zip_file_url, trs_zip_file_url, headers=headers) as response:
            shutil.copyfileobj(response.raw, zip_file)
        zip_file.flush()

        # Unzip it to a directory we will leave laying around.
        # TODO: Make the caller know how to delete it.
        workflow_dir = tempfile.mkdtemp()
        with zipfile.ZipFile(zip_file.name, 'r') as zip_ref:
            zip_ref.extractall(workflow_dir)
        logger.debug("Extracted workflow ZIP to %s", workflow_dir)
   
    # TODO: Hunt throught he directory for a file with the right basename and hash
    primary_descriptor_basename = os.path.basename(primary_descriptor_path)

    # TODO: Return the path to it

    # If we get here, we could not find the right file.
    raise RuntimeError(f"Could not find a {primary_descriptor_basename} with {primary_descriptor_hash_algorithm} hash {primary_descriptor_hash} in ZIP file at {trs_zip_file_url}")

def resolve_workflow(workflow: str) -> str:
    """
    Find the real workflow URL or filename from a command line argument.

    Transform a workflow URL or path that might actually be a Dockstore page
    URL or TRS specifier to an actual URL or path to a workflow document.
    """

    if is_dockstore_workflow(workflow):
        # Ask Dockstore where to find Dockstore-y things
        resolved = get_workflow_root_from_dockstore(workflow)
        logger.info("Dockstore resolved workflow %s to %s", workflow, resolved)
        return resolved
    else:
        # Pass other things through.
        return workflow



        

    





