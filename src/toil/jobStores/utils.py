import logging
import os
import tempfile
import uuid

from toil.batchSystems.registry import DEFAULT_BATCH_SYSTEM, get_batch_system
from toil.batchSystems.abstractGridEngineBatchSystem import AbstractGridEngineBatchSystem

logger = logging.getLogger(__name__)


class JobStoreUnavailableException(RuntimeError):
    """
    Raised when a particular type of job store is requested but can't be used.
    """


def generate_locator(
    job_store_type: str,
    local_suggestion: str | None = None,
    decoration: str | None = None,
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


# Exit code used by the worker when it cannot access the job store, so the
# leader can surface a clear error message rather than just a traceback
TOIL_WORKER_NO_JOB_STORE_EXIT_CODE = 76


class NoAvailableJobStoreException(Exception):
    """Indicates that no job store name is available."""


def generate_default_job_store(
    batch_system_name: str | None,
    provisioner_name: str | None,
    local_directory: str,
    decoration: str | None = None,
) -> str:
    """
    Choose a default job store appropriate to the requested batch system and
    provisioner, and installed modules. Raises an error if no good default is
    available and the user must choose manually.

    :param batch_system_name: Registry name of the batch system the user has
           requested, if any. If no name has been requested, should be None.
    :param provisioner_name: Name of the provisioner the user has requested,
           if any. Recognized provisioners include 'aws' and 'gce'. None
           indicates that no provisioner is in use.
    :param local_directory: Path to a nonexistent local directory suitable for
           use as a file job store.
    :param decoration: Extra string to add to the job store locator, if
        convenient.

    :return str: Job store specifier for a usable job store.
    """

    # Apply default batch system
    batch_system_name = batch_system_name or DEFAULT_BATCH_SYSTEM

    # Work out how to describe where we are
    situation = f"the '{batch_system_name}' batch system"
    if provisioner_name:
        situation += f" with the '{provisioner_name}' provisioner"

    # Default to local job store
    job_store_type = "file"

    try:
        if provisioner_name == "gce":
            logger.warning(
                "The GCE provisioner is deprecated and will be removed in a future release. Please use Kubernetes-based autoscaling instead."
            )
            # With GCE, always use the Google job store
            job_store_type = "google"
        elif provisioner_name == "aws" or batch_system_name in {"mesos", "kubernetes"}:
            # With AWS or these batch systems, always use the AWS job store
            job_store_type = "aws"
        elif provisioner_name is not None and provisioner_name not in ["aws", "gce"]:
            # We 've never heard of this provisioner and don't know what kind
            # of job store to use with it.
            raise NoAvailableJobStoreException()
        elif issubclass(get_batch_system(batch_system_name), AbstractGridEngineBatchSystem):
            raise NoAvailableJobStoreException(
                f"{batch_system_name} batch system doesn't work with a default job store because it requires a shared filesystem. "
                f"Please specify a file job store on shared storage with --jobStore."
            )

        # Then if we get here a job store type has been selected, so try and
        # make it
        
        return generate_locator(
            job_store_type, local_suggestion=local_directory, decoration=decoration
        )

    except JobStoreUnavailableException as e:
        raise NoAvailableJobStoreException(
            f"Could not determine a job store appropriate for "
            f"{situation} because: {e}. Please specify a jobstore with the "
            f"--jobStore option."
        )
    except ImportError:
        raise NoAvailableJobStoreException(
            f"Could not determine a job store appropriate for "
            f"{situation}. Please specify a jobstore with the "
            f"--jobStore option."
        )
    # NoAvailableJobStoreException propagates as-is
