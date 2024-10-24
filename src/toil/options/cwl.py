import os
from argparse import ArgumentParser

from configargparse import SUPPRESS

from toil.version import baseVersion


def add_cwl_options(parser: ArgumentParser, suppress: bool = True) -> None:
    """
    Add CWL options to the parser. This only adds nonpositional CWL arguments.

    :param parser: Parser to add options to
    :param suppress: Suppress help output
    :return: None
    """
    suppress_help = SUPPRESS if suppress else None

    # These are options that we have to match cwltool
    # TODO: Are there still any Toil-specific options in here?
    parser.add_argument("--not-strict", action="store_true", help=suppress_help)
    parser.add_argument(
        "--enable-dev",
        action="store_true",
        help=suppress_help
        or suppress_help
        or "Enable loading and running development versions of CWL",
    )
    parser.add_argument(
        "--enable-ext",
        action="store_true",
        help=suppress_help
        or "Enable loading and running 'cwltool:' extensions to the CWL standards.",
        default=False,
    )
    parser.add_argument(
        "--quiet", dest="quiet", action="store_true", default=False, help=suppress_help
    )
    parser.add_argument(
        "--basedir", type=str, help=suppress_help
    )  # TODO: Might be hard-coded?
    parser.add_argument("--outdir", type=str, default=None, help=suppress_help)
    parser.add_argument(
        "--version",
        action="version",
        version=baseVersion,
        help=suppress_help or "show program's version number and exit",
    )
    parser.add_argument(
        "--log-dir",
        type=str,
        default="",
        help=suppress_help
        or "Log your tools stdout/stderr to this location outside of container",
    )
    # this is as a result of suppressed help statements not working well with mutually_exclusive_groups, which will
    # cause an assertion error
    # https://github.com/python/cpython/issues/62090
    dockergroup = (
        parser.add_mutually_exclusive_group()
        if not suppress_help
        else parser.add_argument_group()
    )
    dockergroup.add_argument(
        "--user-space-docker-cmd",
        help=suppress_help
        or "(Linux/OS X only) Specify a user space docker command (like "
        "udocker or dx-docker) that will be used to call 'pull' and 'run'",
    )
    dockergroup.add_argument(
        "--singularity",
        action="store_true",
        default=False,
        help=suppress_help
        or "Use Singularity runtime for running containers. "
        "Requires Singularity v2.6.1+ and Linux with kernel version v3.18+ or "
        "with overlayfs support backported.",
    )
    dockergroup.add_argument(
        "--podman",
        action="store_true",
        default=False,
        help=suppress_help or "Use Podman runtime for running containers. ",
    )
    dockergroup.add_argument(
        "--no-container",
        action="store_true",
        help=suppress_help
        or "Do not execute jobs in a "
        "Docker container, even when `DockerRequirement` "
        "is specified under `hints`.",
    )
    dockergroup.add_argument(
        "--leave-container",
        action="store_false",
        default=True,
        help=suppress_help
        or "Do not delete Docker container used by jobs after they exit",
        dest="rm_container",
    )
    parser.add_argument(
        "--custom-net",
        help=suppress_help
        or "Specify docker network name to pass to docker run command",
    )
    cidgroup = parser.add_argument_group(
        "Options for recording the Docker container identifier into a file."
    )
    cidgroup.add_argument(
        # Disabled as containerid is now saved by default
        "--record-container-id",
        action="store_true",
        default=False,
        help=suppress_help or SUPPRESS,
        dest="record_container_id",
    )

    cidgroup.add_argument(
        "--cidfile-dir",
        type=str,
        help=suppress_help
        or "Store the Docker container ID into a file in the specified directory.",
        default=None,
        dest="cidfile_dir",
    )

    cidgroup.add_argument(
        "--cidfile-prefix",
        type=str,
        help=suppress_help
        or "Specify a prefix to the container ID filename. "
        "Final file name will be followed by a timestamp. "
        "The default is no prefix.",
        default=None,
        dest="cidfile_prefix",
    )

    parser.add_argument(
        "--no-prepull",
        action="store_true",
        default=False,
        help=suppress_help
        or "Do not prepull the container prior to running the workflow",
    )

    parser.add_argument(
        "--preserve-environment",
        type=str,
        nargs="+",
        help=suppress_help
        or "Preserve specified environment variables when running" " CommandLineTools",
        metavar=("VAR1 VAR2"),
        default=("PATH",),
        dest="preserve_environment",
    )
    parser.add_argument(
        "--preserve-entire-environment",
        action="store_true",
        help=suppress_help
        or "Preserve all environment variable when running CommandLineTools.",
        default=False,
        dest="preserve_entire_environment",
    )
    parser.add_argument(
        "--beta-dependency-resolvers-configuration", default=None, help=suppress_help
    )
    parser.add_argument(
        "--beta-dependencies-directory", default=None, help=suppress_help
    )
    parser.add_argument(
        "--beta-use-biocontainers",
        default=None,
        action="store_true",
        help=suppress_help,
    )
    parser.add_argument(
        "--beta-conda-dependencies",
        default=None,
        action="store_true",
        help=suppress_help,
    )
    parser.add_argument(
        "--tmpdir-prefix",
        type=str,
        help=suppress_help or "Path prefix for temporary directories",
        default=None,
    )
    parser.add_argument(
        "--tmp-outdir-prefix",
        type=str,
        help=suppress_help or "Path prefix for intermediate output directories",
        default=None,
    )
    parser.add_argument(
        "--force-docker-pull",
        action="store_true",
        default=False,
        dest="force_docker_pull",
        help=suppress_help or "Pull latest docker image even if it is locally present",
    )
    parser.add_argument(
        "--no-match-user",
        action="store_true",
        default=False,
        help=suppress_help or "Disable passing the current uid to `docker run --user`",
    )
    parser.add_argument(
        "--no-read-only",
        action="store_true",
        default=False,
        help=suppress_help or "Do not set root directory in the container as read-only",
    )
    parser.add_argument(
        "--strict-memory-limit",
        action="store_true",
        help=suppress_help
        or "When running with "
        "software containers and the Docker engine, pass either the "
        "calculated memory allocation from ResourceRequirements or the "
        "default of 1 gigabyte to Docker's --memory option.",
    )
    parser.add_argument(
        "--strict-cpu-limit",
        action="store_true",
        help=suppress_help
        or "When running with "
        "software containers and the Docker engine, pass either the "
        "calculated cpu allocation from ResourceRequirements or the "
        "default of 1 core to Docker's --cpu option. "
        "Requires docker version >= v1.13.",
    )
    parser.add_argument(
        "--relax-path-checks",
        action="store_true",
        default=False,
        help=suppress_help
        or "Relax requirements on path names to permit " "spaces and hash characters.",
        dest="relax_path_checks",
    )
    parser.add_argument(
        "--default-container",
        help=suppress_help
        or "Specify a default docker container that will be "
        "used if the workflow fails to specify one.",
    )
    parser.add_argument(
        "--disable-validate",
        dest="do_validate",
        action="store_false",
        default=True,
        help=suppress_help or SUPPRESS,
    )
    parser.add_argument(
        "--fast-parser",
        dest="fast_parser",
        action="store_true",
        default=False,
        help=suppress_help or SUPPRESS,
    )
    # same workaround as dockergroup
    checkgroup = (
        parser.add_mutually_exclusive_group()
        if not suppress_help
        else parser.add_argument_group()
    )
    checkgroup.add_argument(
        "--compute-checksum",
        action="store_true",
        default=True,
        help=suppress_help or "Compute checksum of contents while collecting outputs",
        dest="compute_checksum",
    )
    checkgroup.add_argument(
        "--no-compute-checksum",
        action="store_false",
        help=suppress_help
        or "Do not compute checksum of contents while collecting outputs",
        dest="compute_checksum",
    )

    parser.add_argument(
        "--eval-timeout",
        help=suppress_help
        or "Time to wait for a Javascript expression to evaluate before giving "
        "an error, default 20s.",
        type=float,
        default=20,
    )
    parser.add_argument(
        "--overrides",
        type=str,
        default=None,
        help=suppress_help or "Read process requirement overrides from file.",
    )

    parser.add_argument(
        "--mpi-config-file",
        type=str,
        default=None,
        help=suppress_help
        or "Platform specific configuration for MPI (parallel "
        "launcher, its flag etc). See the cwltool README "
        "section 'Running MPI-based tools' for details of the format: "
        "https://github.com/common-workflow-language/cwltool#running-mpi-based-tools-that-need-to-be-launched",
    )

    provgroup = parser.add_argument_group(
        "Options for recording provenance information of the execution"
    )
    provgroup.add_argument(
        "--provenance",
        help=suppress_help
        or "Save provenance to specified folder as a "
        "Research Object that captures and aggregates "
        "workflow execution and data products.",
        type=str,
    )

    provgroup.add_argument(
        "--enable-user-provenance",
        default=False,
        action="store_true",
        help=suppress_help or "Record user account info as part of provenance.",
        dest="user_provenance",
    )
    provgroup.add_argument(
        "--disable-user-provenance",
        default=False,
        action="store_false",
        help=suppress_help or "Do not record user account info in provenance.",
        dest="user_provenance",
    )
    provgroup.add_argument(
        "--enable-host-provenance",
        default=False,
        action="store_true",
        help=suppress_help or "Record host info as part of provenance.",
        dest="host_provenance",
    )
    provgroup.add_argument(
        "--disable-host-provenance",
        default=False,
        action="store_false",
        help=suppress_help or "Do not record host info in provenance.",
        dest="host_provenance",
    )
    provgroup.add_argument(
        "--orcid",
        help=suppress_help
        or "Record user ORCID identifier as part of "
        "provenance, e.g. https://orcid.org/0000-0002-1825-0097 "
        "or 0000-0002-1825-0097. Alternatively the environment variable "
        "ORCID may be set.",
        dest="orcid",
        default=os.environ.get("ORCID", ""),
        type=str,
    )
    provgroup.add_argument(
        "--full-name",
        help=suppress_help
        or "Record full name of user as part of provenance, "
        "e.g. Josiah Carberry. You may need to use shell quotes to preserve "
        "spaces. Alternatively the environment variable CWL_FULL_NAME may "
        "be set.",
        dest="cwl_full_name",
        default=os.environ.get("CWL_FULL_NAME", ""),
        type=str,
    )

    # These are Toil-specific options
    parser.add_argument(
        "--bypass-file-store",
        action="store_true",
        default=False,
        help=suppress_help
        or "Do not use Toil's file store and assume all "
        "paths are accessible in place from all nodes.",
        dest="bypass_file_store",
    )
    parser.add_argument(
        "--reference-inputs",
        action="store_true",
        default=False,
        help=suppress_help
        or "Do not copy remote inputs into Toil's file "
        "store and assume they are accessible in place from "
        "all nodes.",
        dest="reference_inputs",
    )
    parser.add_argument(
        "--disable-streaming",
        action="store_true",
        default=False,
        help=suppress_help
        or "Disable file streaming for files that have 'streamable' flag True.",
        dest="disable_streaming",
    )
    ram_group = (
        parser.add_mutually_exclusive_group()
        if not suppress_help
        else parser.add_argument_group()
    )
    ram_group.add_argument(
        "--cwl-default-ram",
        action="store_true",
        default=True,
        help=suppress_help or "Apply CWL specification default ramMin.",
        dest="cwl_default_ram",
    )
    ram_group.add_argument(
        "--no-cwl-default-ram",
        action="store_false",
        help=suppress_help
        or "Do not apply CWL specification default ramMin, so that Toil --defaultMemory applies.",
        dest="cwl_default_ram",
    )
    parser.add_argument(
        "--destBucket",
        type=str,
        help=suppress_help or "Specify a cloud bucket endpoint for output files.",
    )
