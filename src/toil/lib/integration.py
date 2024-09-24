import logging
import os
import sys
from typing import Dict, List, Optional

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

@retry(errors=[requests.exceptions.ConnectionError])
def get_workflow_root_from_dockstore(workflow: str) -> str:
    """
    Given a Dockstore URL or TRS identifier, get the root WDL or CWL URL for the workflow.
    
    Accepts inputs like:

        - https://dockstore.org/workflows/github.com/dockstore-testing/md5sum-checker:master?tab=info
        - #workflow/github.com/dockstore-testing/md5sum-checker

    Assumes the input is actually one of the supported formats. See is_dockstore_workflow().

    TODO: Needs to handle multi-workflow files if Dockstore can.

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
        
    # Parse the TRS ID 
    parts = trs_spec.split(':', 1)
    trs_workflow_id = parts[0]
    if len(parts) > 1:
        # The ID has the version we want after a colon
        trs_version = parts[1]
    else:
        # We don't know the version we want, we will have to pick one somehow.
        trs_version = None

    logger.debug("TRS %s parses to workflow %s and version %s", trs_spec, trs_workflow_id, trs_version)

    # Fetch the main TRS document.
    # See e.g. https://dockstore.org/api/ga4gh/trs/v2/tools/%23workflow%2Fgithub.com%2Fdockstore-testing%2Fmd5sum-checker
    trs_workflow_url = f"https://dockstore.org/api/ga4gh/trs/v2/tools/{quote(trs_workflow_id, safe='')}"
    trs_workflow_document = requests.get(trs_workflow_url).json()

    # Each version can have a different language so make a map of them.
    # Also lets us check for a default version's existence.
    version_to_language: Dict[str, str] = {}

    for version_info in trs_workflow_document.get("versions", []):
        # TODO: Why is there an array of descriptor types?
        version_to_language[version_info["name"]] = version_info["descriptor_type"][0]
        logger.debug("Workflow version %s is written in %s", version_info["name"], version_info["descriptor_type"][0])

    for default_version in ['main', 'master']:
        if trs_version is None and default_version in version_to_language:
            # Fill in a version if the user didn't provide one.
            trs_version = default_version
            logger.debug("Defaulting to workflow version %s", default_version)
            break

    if trs_version is None and len(version_to_language) == 1:
        # If there's just one version use that.
        trs_version = next(iter(version_to_language.keys()))
        logger.debug("Defaulting to only available workflow version %s", trs_version)

    if trs_version is None:
        raise RuntimeError(f"Workflow {workflow} does not specify a version; must be one of {list(version_to_language.keys())}")
    
    if trs_version not in version_to_language:
        raise RuntimeError(f"Workflow version {trs_version} from {workflow} does not exist; must be one of {list(version_to_language.keys())}")

    # Language can be "CWL" or "WDL".
    # TODO: We're probably already in a runner that expects one or the other.
    language = version_to_language[trs_version]

    # TODO: There's a {workflow}/versions/{version}/{language}/files endpoint
    # that can say which file is the PRIMARY_DESCRIPTOR, but it can't give us a
    # repo-root-relative path for that file, so we can't construct the
    # {workflow}/versions/{version}/PLAIN_{language}/descriptor//path/from/root/to/file.ext
    # URL that behaves properly with .. imports without special handling to
    # actually put ".." in the URL.
    #
    # So we fetch {workflow}/versions/{version}/{language}/descriptor and
    # follow its "url" to somewhere we hope the correct directory tree is.

    trs_version_url = f"{trs_workflow_url}/versions/{quote(trs_version, safe='')}"
    trs_descriptor_url = f"{trs_version_url}/{language}/descriptor"
    logger.debug("Workflow descriptor URL: %s", trs_descriptor_url)
    trs_descriptor_document = requests.get(trs_descriptor_url).json()

    return trs_descriptor_document["url"]

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



        

    





