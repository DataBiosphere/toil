from argparse import SUPPRESS
from WDL.CLI import runner_input_completer


def add_run_options(parser):
    parser.add_argument(
        "uri",
        metavar="URI",
        type=str,
        help="WDL document filename/URI"
    )
    parser.add_argument(
        "inputs",
        metavar="input_key=value",
        type=str,
        nargs="*",
        help="Workflow inputs. Optional space between = and value."
        " For arrays repeat, key=value1 key=value2 ...",
    ).completer = runner_input_completer
    group = parser.add_argument_group("input")
    group.add_argument(
        "-i",
        "--input",
        metavar="INPUT.json",
        dest="input_file",
        help="Cromwell-style input JSON object, filename, or -; command-line inputs will be merged in",
    )
    group.add_argument(
        "--empty",
        metavar="input_key",
        action="append",
        help="explicitly set a string input to the empty string OR an array input to the empty array",
    )
    group.add_argument(
        "--none",
        metavar="input_key",
        action="append",
        help="explicitly set an optional input to None (to override a default)",
    )
    group.add_argument(
        "--task",
        metavar="TASK_NAME",
        help="name of task to run (for WDL documents with multiple tasks & no workflow)",
    )
    group.add_argument(
        "-j",
        "--json",
        dest="json_only",
        action="store_true",
        help="just print Cromwell-style input JSON to standard output, then exit",
    )
    group = parser.add_argument_group("output")
    group.add_argument(
        "-d",
        "--dir",
        metavar="DIR",
        dest="run_dir",
        help=(
            "directory under which to create a timestamp-named subdirectory for this run (defaults to current "
            " working directory); supply '.' or 'some/dir/.' to instead run in this directory exactly"
        ),
    )
    group.add_argument(
        "--error-json",
        action="store_true",
        help="upon failure, print error information JSON to standard output (in addition to standard error logging)",
    )
    group.add_argument(
        "-o",
        metavar="OUT.json",
        dest="stdout_file",
        help="write JSON output/error to specified file instead of standard output (implies --error-json)",
    )
    group = parser.add_argument_group("logging")
    group.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="increase logging detail & stream tasks' stderr",
    )
    group.add_argument(
        "--no-color",
        action="store_true",
        help="disable colored logging and status bar on terminal (also set by NO_COLOR environment variable)",
    )
    group.add_argument("--log-json", action="store_true", help="write all logs in JSON")
    group = parser.add_argument_group("configuration")
    group.add_argument(
        "--cfg",
        metavar="FILE",
        type=str,
        default=None,
        help=(
            "configuration file to load (in preference to file named by MINIWDL_CFG environment, or "
            "XDG_CONFIG_{HOME,DIRS}/miniwdl.cfg)"
        ),
    )
    group.add_argument("-@", metavar="N", dest="max_tasks", type=int, default=None, help=SUPPRESS)
    group.add_argument("--runtime-cpu-max", metavar="N", type=int, default=None, help=SUPPRESS)
    group.add_argument("--runtime-memory-max", metavar="N", type=str, default=None, help=SUPPRESS)
    group.add_argument(
        "--runtime-defaults",
        metavar="JSON",
        type=str,
        default=None,
        help="""default runtime settings for all tasks (JSON filename or literal object e.g. '{"maxRetries":2}')""",
    )
    group.add_argument(
        "--no-cache",
        action="store_true",
        help="override any configuration enabling cache lookup for call outputs & downloaded files",
    )
    group.add_argument(
        "--copy-input-files",
        action="store_true",
        help="copy input files for each task and mount them read/write (unblocks task commands that mv/rm/write them)",
    )
    group.add_argument(
        "--as-me",
        action="store_true",
        help=(
            "run all containers as the invoking user uid:gid (more secure, but potentially blocks task commands e.g. "
            "apt-get)"
        ),
    )
    return parser
