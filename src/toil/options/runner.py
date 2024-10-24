from argparse import ArgumentParser

from toil.lib.conversions import human2bytes


def add_runner_options(
    parser: ArgumentParser, cwl: bool = False, wdl: bool = False
) -> None:
    """
    Add to the WDL or CWL runners options that are shared or the same between runners
    :param parser: parser to add arguments to
    :param cwl: bool
    :param wdl: bool
    :return: None
    """
    # This function should be constructed so that even when wdl and cwl are false, the "default" options are still added
    run_imports_on_workers_arguments = ["--runImportsOnWorkers"]
    if cwl:
        run_imports_on_workers_arguments.append("--run-imports-on-workers")
    parser.add_argument(
        *run_imports_on_workers_arguments,
        action="store_true",
        default=False,
        dest="run_imports_on_workers",
        help="Run the file imports on a worker instead of the leader. This is useful if the leader is not optimized for high network performance. "
        "If set to true, the argument --importWorkersDisk must also be set."
    )
    import_workers_threshold_argument = ["--importWorkersThreshold"]
    if cwl:
        import_workers_threshold_argument.append("--import-workers-threshold")
    parser.add_argument(
        *import_workers_threshold_argument,
        dest="import_workers_threshold",
        type=lambda x: human2bytes(str(x)),
        default="1 GiB",
        help="Specify the file size threshold that determines if the file import will happen in its own job. All files below the threshold "
        "will go into a batch import job. This should be set in conjunction with the argument --runImportsOnWorkers."
    )
