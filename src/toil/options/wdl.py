from argparse import ArgumentParser

from configargparse import SUPPRESS

from toil.lib.conversions import strtobool


def add_wdl_options(parser: ArgumentParser, suppress: bool = True) -> None:
    """
    Add WDL options to a parser. This only adds nonpositional WDL arguments
    :param parser: Parser to add options to
    :param suppress: Suppress help output
    :return: None
    """
    suppress_help = SUPPRESS if suppress else None
    # include arg names without a wdl specifier if suppress is False
    # this is to avoid possible duplicate options in custom toil scripts, ex outputFile can be a common argument name
    # TODO: Why do we even need them at all in other Toil scripts? Do we have to worry about dest= collisions?
    # TODO: Can the better option name be first?
    output_dialect_arguments = ["--wdlOutputDialect"] + (
        ["--outputDialect"] if not suppress else []
    )
    parser.add_argument(
        *output_dialect_arguments,
        dest="output_dialect",
        type=str,
        default="cromwell",
        choices=["cromwell", "miniwdl"],
        help=suppress_help
        or (
            "JSON output format dialect. 'cromwell' just returns the workflow's "
            "output values as JSON, while 'miniwdl' nests that under an 'outputs' "
            "key, and includes a 'dir' key where files are written."
        )
    )
    output_directory_arguments = ["--wdlOutputDirectory"] + (
        ["--outputDirectory", "-o"] if not suppress else []
    )
    parser.add_argument(
        *output_directory_arguments,
        dest="output_directory",
        type=str,
        default=None,
        help=suppress_help
        or (
            "Directory or URI prefix to save output files at. By default a new directory is created "
            "in the current directory."
        )
    )
    output_file_arguments = ["--wdlOutputFile"] + (
        ["--outputFile", "-m"] if not suppress else []
    )
    parser.add_argument(
        *output_file_arguments,
        dest="output_file",
        type=str,
        default=None,
        help=suppress_help or "File or URI to save output JSON to."
    )
    reference_inputs_arguments = ["--wdlReferenceInputs"] + (
        ["--referenceInputs"] if not suppress else []
    )
    parser.add_argument(
        *reference_inputs_arguments,
        dest="reference_inputs",
        type=strtobool,
        default=False,
        help=suppress_help or "Pass input files by URL"
    )
    container_arguments = ["--wdlContainer"] + (["--container"] if not suppress else [])
    parser.add_argument(
        *container_arguments,
        dest="container",
        type=str,
        choices=["singularity", "docker", "auto"],
        default="auto",
        help=suppress_help or "Container engine to use to run WDL tasks"
    )
    all_call_outputs_arguments = ["--wdlAllCallOutputs"] + (
        ["--allCallOutputs"] if not suppress else []
    )
    parser.add_argument(
        *all_call_outputs_arguments,
        dest="all_call_outputs",
        type=strtobool,
        default=None,
        help=suppress_help or "Keep and return all call outputs as workflow outputs"
    )
