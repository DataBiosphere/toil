from argparse import ArgumentParser, SUPPRESS

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
    import_workers_batchsize_argument = ["--importWorkersBatchSize"]
    if cwl:
        import_workers_batchsize_argument.append("--import-workers-batch-size")
    parser.add_argument(
        *import_workers_batchsize_argument,
        dest="import_workers_batchsize",
        type=lambda x: human2bytes(str(x)),
        default="1 GiB",
        help="Specify the target total file size for file import batches. "
        "As many files as can fit will go into each batch import job. This should be set in conjunction with the argument --runImportsOnWorkers."
    )

    # Deprecated
    parser.add_argument(
        "--importWorkersThreshold", "--import-workers-threshold", dest="import_workers_batchsize",type=lambda x: human2bytes(str(x)), help=SUPPRESS
    )

    import_workers_disk_argument = ["--importWorkersDisk"]
    if cwl:
        import_workers_disk_argument.append("--import-workers-disk")
    parser.add_argument(
        *import_workers_disk_argument,
        dest="import_workers_disk",
        type=lambda x: human2bytes(str(x)),
        default="1 MiB",
        help="Specify the disk size each import worker will get. This usually will not need to be set as Toil will attempt to use file streaming when downloading files. "
             "If not possible, for example, when downloading from AWS to a GCE job store, "
             "this should be set to the largest file size of all files to import. This should be set in conjunction with the arguments "
             "--runImportsOnWorkers and --importWorkersBatchSize."
    )
