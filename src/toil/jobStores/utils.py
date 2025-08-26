import errno
import logging
import os
import tempfile
import uuid
from abc import ABC, abstractmethod
from typing import Optional

from toil.lib.threading import ExceptionalThread

log = logging.getLogger(__name__)

class JobStoreUnavailableException(RuntimeError):
    """
    Raised when a particular type of job store is requested but can't be used.
    """


def generate_locator(
    job_store_type: str,
    local_suggestion: Optional[str] = None,
    decoration: Optional[str] = None,
) -> str:
    """
    Generate a random locator for a job store of the given type. Raises an
    JobStoreUnavailableException if that job store cannot be used.

    :param job_store_type: Registry name of the job store to use.
    :param local_suggestion: Path to a nonexistent local directory suitable for
        use as a file job store.
    :param decoration: Extra string to add to the job store locator, if
        convenient.

    :return str: Job store locator for a usable job store.
    """

    # Prepare decoration for splicing into strings
    decoration = ("-" + decoration) if decoration else ""

    try:
        if job_store_type == "google":
            # Make sure we have the Google job store
            from toil.jobStores.googleJobStore import GoogleJobStore  # noqa

            # Look for a project
            project = os.getenv("TOIL_GOOGLE_PROJECTID")
            project_part = (":" + project) if project else ""

            # Roll a random bucket name, possibly in the project.
            return f"google{project_part}:toil{decoration}-{str(uuid.uuid4())}"
        elif job_store_type == "aws":
            # Make sure we have AWS
            from toil.jobStores.aws.jobStore import AWSJobStore  # noqa

            # Find a region
            from toil.lib.aws import get_current_aws_region

            region = get_current_aws_region()

            if not region:
                # We can't generate an AWS job store without a region
                raise JobStoreUnavailableException(
                    f"{job_store_type} job store can't be made without a region"
                )

            # Roll a random name
            return f"aws:{region}:toil{decoration}-{str(uuid.uuid4())}"
        elif job_store_type == "file":
            if local_suggestion:
                # Just use the given local directory.
                return local_suggestion
            else:
                # Pick a temp path
                return os.path.join(
                    tempfile.gettempdir(), "toil-" + str(uuid.uuid4()) + decoration
                )
        else:
            raise JobStoreUnavailableException(
                f"{job_store_type} job store isn't known"
            )
    except ImportError:
        raise JobStoreUnavailableException(
            f"libraries for {job_store_type} job store are not installed"
        )
