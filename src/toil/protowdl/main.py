import WDL
import logging
import json
import sys
import os
import platform
import tempfile
import json
import logging
import asyncio
import atexit
import textwrap
from shlex import quote as shellquote
from argparse import ArgumentParser, Action, SUPPRESS, RawDescriptionHelpFormatter
from contextlib import ExitStack
import argcomplete
import argparse
import base64
import copy
import datetime
import errno
import functools
import json
import logging
import os
import shutil
import socket
import stat
import sys
import tempfile
import textwrap
import urllib
import uuid
from typing import (
    Any,
    Callable,
    Dict,
    IO,
    Iterable,
    Iterator,
    List,
    Mapping,
    MutableMapping,
    MutableSequence,
    Optional,
    Pattern,
    Text,
    TextIO,
    Tuple,
    TypeVar,
    Union,
    cast,
)
from urllib import parse as urlparse

import cwltool.builder
import cwltool.command_line_tool
import cwltool.context
import cwltool.errors
import cwltool.expression
import cwltool.load_tool
import cwltool.main
import cwltool.provenance
import cwltool.resolver
import cwltool.stdfsaccess
import schema_salad.ref_resolver
from cwltool.loghandler import _logger as cwllogger
from cwltool.loghandler import defaultStreamHandler
from cwltool.mpi import MpiConfig
from cwltool.mutation import MutationManager
from cwltool.pathmapper import MapperEnt, PathMapper
from cwltool.process import (
    Process,
    add_sizes,
    compute_checksums,
    fill_in_defaults,
    shortname,
)
from cwltool.secrets import SecretStore
from cwltool.software_requirements import (
    DependenciesConfiguration,
    get_container_from_software_requirements,
)
from cwltool.utils import (
    CWLObjectType,
    CWLOutputType,
    adjustDirObjs,
    adjustFileObjs,
    aslist,
    get_listing,
    normalizeFilesDirs,
    visit_class,
    downloadHttpFile,
)
from ruamel.yaml.comments import CommentedMap, CommentedSeq
from schema_salad import validate
from schema_salad.exceptions import ValidationException
from schema_salad.avro.schema import Names
from schema_salad.sourceline import SourceLine, cmap
from threading import Thread

from toil.batchSystems.registry import DEFAULT_BATCH_SYSTEM
from options import addOptions, Config
from toil.cwl.utils import (
    download_structure,
    visit_top_cwl_class,
    visit_cwl_class_and_reduce,
)
from toil.fileStores import FileID
from toil.fileStores.abstractFileStore import AbstractFileStore
from toil.job import Job
from toil.jobStores.abstractJobStore import NoSuchFileException
from toil.jobStores.fileJobStore import FileJobStore
from toil.lib.threading import ExceptionalThread
from toil.version import baseVersion

from WDL.runtime.config import Loader
from WDL.CLI import (fill_common,
                     fill_check_subparser,
                     fill_run_subparser,
                     fill_configure_subparser,
                     fill_run_self_test_subparser,
                     fill_localize_subparser,
                     check,
                     runner,
                     run_self_test,
                     localize,
                     configure)
import logging
import multiprocessing
import os
import math
import itertools
import json
import signal
import traceback
import pickle
import threading
import toil
from concurrent import futures
from typing import Optional, List, Set, Tuple, NamedTuple, Dict, Union, Iterable, Callable, Any
from contextlib import ExitStack
from WDL import Env, Type, Value, Tree, StdLib
from WDL.Error import InputError
from WDL.runtime.workflow import StateMachine, _StdLib, _download_input_files, _add_downloadable_defaults, run_local_workflow
from WDL.runtime.task import run_local_task, _fspaths, link_outputs, _add_downloadable_defaults
from WDL.runtime.download import able as downloadable, run_cached as download
from WDL._util import (
    write_atomic,
    write_values_json,
    provision_run_dir,
    TerminationSignalFlag,
    LoggingFileHandler,
    compose_coroutines,
    pathsize,
)
from WDL._util import StructuredLogMessage as _
from WDL.runtime import config, _statusbar
from WDL.runtime.cache import CallCache, new as new_call_cache
from WDL.runtime.error import RunFailed, Terminated, error_json


logger = logging.getLogger(__name__)


class WDLJob(Job):
    def __init__(self, options, *args, **kwargs):
        super(WDLJob, self).__init__(
            cores=1,
            memory=1024 * 1024 * 1024,
            disk=1024 * 1024 * 1024,
            unitName='change_this',
            displayName='whatever'
        )
        self.options = options
        self.args = args
        self.kwargs = kwargs

    def run(self, file_store: AbstractFileStore) -> Any:
        logger.debug('Running a job.')
        # self.job = resolve_dict_w_promises(self.job, file_store)
        # fill_in_defaults(self.step_inputs, cwljob, self.runtime_context.make_fs_access(""))
        # required_env_vars = self.populate_env_vars(cwljob)

        _, output = run_local_task(*self.args, **self.kwargs)
        # Upload all the Files and set their and the Directories' locations, if needed.
        return output


# def task(wdl: str, inputs: str, **kwargs):
#     cfg = Loader(logging.getLogger('test'), [])
#     with open(wdl, 'r') as f:
#         wdl_content = f.read()
#     with open(inputs, 'r') as f:
#         inputs_dict = json.load(f)
#     doc = WDL.parse_document(wdl_content, version='draft-2')
#     doc.typecheck()
#
#     for task in doc.tasks:
#         print(task.available_inputs)
#         print([i.name for i in task.available_inputs])
#         print(task.required_inputs)
#         print([i.name for i in task.required_inputs])
#         inputs_dict = {'inputFile': '/home/quokka/git/toil/src/toil/test/wdl/md5sum/md5sum.input'}
#         inputs = WDL.values_from_json(inputs_dict, doc.tasks[0].available_inputs, doc.tasks[0].required_inputs)
#         rundir, outputs = WDL.runtime.run_local_task(cfg, doc.tasks[0], (inputs or WDL.Env.Bindings()),
#                                                      run_dir='/home/quokka/git/miniwdl/run_dir', **kwargs)
#         return WDL.values_to_json(outputs)


def run(
    uri,
    task=None,
    inputs=[],
    input_file=None,
    empty=[],
    none=[],
    json_only=False,
    run_dir=None,
    path=None,
    check_quant=True,
    cfg=None,
    runtime_cpu_max=None,
    runtime_memory_max=None,
    env=[],
    runtime_defaults=None,
    max_tasks=None,
    copy_input_files=False,
    as_me=False,
    no_cache=False,
    error_json=False,
    log_json=False,
    stdout_file=None,
    stderr_file=None,
    no_outside_imports=False,
    **kwargs,
):
    if cfg:
        assert os.path.isfile(cfg), f'--cfg file not found: {cfg}'
        cfg = WDL.runtime.config.Loader(logger, filenames=[cfg])
    else:
        cfg = WDL.runtime.config.Loader(logger, filenames=None)
    with open(uri, 'r') as f:
        wdl_content = f.read()
    doc = WDL.parse_document(wdl_content, version='draft-2')
    doc.typecheck()
    eff_root = '/'

    workflow, input_env, input_json = WDL.CLI.runner_input(
        doc,
        inputs,
        input_file,
        empty,
        none,
        task=task,
        downloadable=lambda fn, is_dir: WDL.runtime.download.able(cfg, fn, directory=is_dir),
        root=eff_root,  # if copy_input_files is set, then input files need not reside under the configured root
    )

    run_id = workflow.name
    _run_id_stack = []
    run_dir = provision_run_dir(workflow.name, run_dir, last_link=not _run_id_stack)

    logfile = os.path.join(run_dir, "workflow.log")
    with ExitStack() as cleanup:
        with WDL.runtime.cache.CallCache(cfg, logger) as cache:
            cleanup.enter_context(
                LoggingFileHandler(
                    logger,
                    logfile,
                )
            )
            if cfg.has_option("logging", "json") and cfg["logging"].get_bool("json"):
                cleanup.enter_context(
                    LoggingFileHandler(
                        logger,
                        logfile + ".json",
                        json=True,
                    )
                )
            logger.notice(  # pyre-fixme
                _(
                    "workflow start",
                    name=workflow.name,
                    source=workflow.pos.uri,
                    line=workflow.pos.line,
                    column=workflow.pos.column,
                    dir=run_dir,
                )
            )
            logger.debug(_("thread", ident=threading.get_ident()))
            terminating = cleanup.enter_context(TerminationSignalFlag(logger))
            write_values_json(inputs, os.path.join(run_dir, "inputs.json"), namespace=workflow.name)

            # inputs = WDL.values_from_json(input_json, doc.tasks[0].available_inputs, doc.tasks[0].required_inputs)
            # rundir, output_env = WDL.runtime.run(cfg, target, input_env, run_dir=run_dir)
            out = _workflow_main_loop(cfg=cfg,
                                      workflow=workflow,
                                      inputs=input_env,
                                      run_dir=run_dir,
                                      logger=logger,
                                      logger_id=[__name__],
                                      terminating=terminating,
                                      _test_pickle=False,
                                      run_id_stack=[],
                                      cache=cache)
    print({b.name: b.value.value for b in out})

# def run_local_workflow(
#     cfg: config.Loader,
#     workflow: Tree.Workflow,
#     inputs: Env.Bindings[Value.Base],
#     run_id: Optional[str] = None,
#     run_dir: Optional[str] = None,
#     logger_prefix: Optional[List[str]] = None,
#     _thread_pools: Optional[Tuple[futures.ThreadPoolExecutor, futures.ThreadPoolExecutor]] = None,
#     _cache: Optional[CallCache] = None,
#     _test_pickle: bool = False,
#     _run_id_stack: Optional[List[str]] = None,
# ) -> Tuple[str, Env.Bindings[Value.Base]]:

def _workflow_main_loop(
    cfg: config.Loader,
    workflow: Tree.Workflow,
    inputs: Env.Bindings[Value.Base],
    run_id_stack: List[str],
    run_dir: str,
    logger: logging.Logger,
    logger_id: List[str],
    terminating: Callable[[], bool],
    _test_pickle: bool,
    cache
) -> Env.Bindings[Value.Base]:
    assert isinstance(cfg, config.Loader)
    tasks = {}
    try:
        # start plugin coroutines and process inputs through them
        with compose_coroutines(
            [
                (
                    lambda kwargs, cor=cor: cor(
                        cfg, logger, run_id_stack, run_dir, workflow, **kwargs
                    )
                )
                for cor in [cor2 for _, cor2 in sorted(config.load_plugins(cfg, "workflow"))]
            ],
            {"inputs": inputs},
        ) as plugins:
            recv = next(plugins)
            inputs = recv["inputs"]

            # run workflow state machine to completion
            state = StateMachine(".".join(logger_id), run_dir, workflow, inputs)
            options = Job.Runner.getDefaultOptions('/home/quokka/git/mwp/delete')
            with toil.common.Toil(options) as toil_workflow:
                while state.outputs is None:
                    if _test_pickle:
                        state = pickle.loads(pickle.dumps(state))
                    if terminating():
                        raise Terminated()
                    # schedule all runnable calls
                    stdlib = _StdLib(workflow.effective_wdl_version, cfg, state, cache)
                    next_call = state.step(cfg, stdlib)
                    while next_call:
                        call_dir = os.path.join(run_dir, next_call.id)
                        if os.path.exists(call_dir):
                            logger.warning(
                                _("call subdirectory already exists, conflict likely", dir=call_dir)
                            )
                        sub_args = (cfg, next_call.callee, next_call.inputs)
                        sub_kwargs = {
                            "run_id": next_call.id,
                            "run_dir": os.path.join(call_dir, "."),
                            "logger_prefix": logger_id,
                            "_cache": cache,
                            "_run_id_stack": run_id_stack,
                        }
                        if isinstance(next_call.callee, Tree.Task):
                            _statusbar.task_backlogged()
                            _, outputs = run_local_task(*sub_args, **sub_kwargs)
                        elif isinstance(next_call.callee, Tree.Workflow):
                            _, outputs = run_local_workflow(*sub_args, **sub_kwargs)
                        else:
                            assert False
                        state.call_finished(next_call.id, outputs)
                        next_call = state.step(cfg, stdlib)
                    # no more calls to launch right now; wait for an outstanding call to finish
                    assert state.outputs is not None

            # create output_links
            outputs = link_outputs(
                state.outputs, run_dir, hardlinks=cfg["file_io"].get_bool("output_hardlinks")
            )

            # process outputs through plugins
            recv = plugins.send({"outputs": outputs})
            outputs = recv["outputs"]

            # write outputs.json
            write_values_json(
                outputs, os.path.join(run_dir, "outputs.json"), namespace=workflow.name
            )
            logger.notice("done")
            return outputs
    except Exception as exn:
        logger.debug(traceback.format_exc())
        cause = exn
        while isinstance(cause, RunFailed) and cause.__cause__:
            cause = cause.__cause__
        wrapper = RunFailed(workflow, run_id_stack[-1], run_dir)
        try:
            write_atomic(
                json.dumps(error_json(wrapper, cause=exn), indent=2),
                os.path.join(run_dir, "error.json"),
            )
        except Exception as exn2:
            logger.debug(traceback.format_exc())
            logger.critical(_("failed to write error.json", dir=run_dir, message=str(exn2)))
        if not isinstance(exn, RunFailed):
            logger.error(_(str(wrapper), dir=run_dir, **error_json(exn)))
        elif not isinstance(exn.__cause__, Terminated):
            logger.error(
                _("call failure propagating", **{"from": getattr(exn, "run_id"), "dir": run_dir})
            )
        # Cancel all future tasks that havent started
        for key in tasks:
            key.cancel()
        raise wrapper from exn

    # for task in doc.tasks:
    #     print(task.available_inputs)
    #     print([i.name for i in task.available_inputs])
    #     print(task.required_inputs)
    #     print([i.name for i in task.required_inputs])
    #     print(kwargs)
    #     run_local_task_inputs = {'cfg': cfg,
    #                              'task': task,
    #                              'inputs': inputs or WDL.Env.Bindings(),
    #                              'run_id': kwargs.get('run_id'),
    #                              'run_dir': '/home/quokka/git/miniwdl/run_dir',
    #                              'logger_prefix': kwargs.get('logger_prefix'),
    #                              '_run_id_stack': kwargs.get('_run_id_stack'),
    #                              '_cache': kwargs.get('_cache'),
    #                              '_plugins': kwargs.get('_plugins')}
    #     rundir, outputs = WDL.runtime.run_local_task(**run_local_task_inputs)
    #     json_output = WDL.values_to_json(outputs)
    #     print(json_output)
    #     return json_output


def main(args=None):
    args = args if args is not None else sys.argv[1:]
    sys.setrecursionlimit(1_000_000)  # permit as much call stack depth as OS can give us

    parser = argparse.ArgumentParser('toil-wdl-runner')

    config = Config()
    addOptions(parser, config, jobstore_is_optional=True)
    subparsers = parser.add_subparsers()
    subparsers.required = True
    subparsers.dest = "command"
    fill_common(fill_check_subparser(subparsers))
    fill_configure_subparser(subparsers)
    fill_common(fill_run_subparser(subparsers))
    argcomplete.autocomplete(parser)

    replace_COLUMNS = os.environ.get("COLUMNS", None)
    os.environ["COLUMNS"] = "100"  # make help descriptions wider
    args = parser.parse_args(args)

    if replace_COLUMNS is not None:
        os.environ["COLUMNS"] = replace_COLUMNS
    else:
        del os.environ["COLUMNS"]

    try:
        if args.command == "check":
            check(**vars(args))
        elif args.command == "run":
            print(args)
            run(**vars(args))
        elif args.command == "run_self_test":
            run_self_test(**vars(args))
        elif args.command == "localize":
            localize(**vars(args))
        elif args.command == "configure":
            configure(**vars(args))
        else:
            assert False
    except (
        WDL.Error.SyntaxError,
        WDL.Error.ImportError,
        WDL.Error.ValidationError,
        WDL.Error.MultipleValidationErrors,
    ) as exn:
        global quant_warning
        if args.check_quant and quant_warning:
            print(
                "* Hint: for compatibility with older existing WDL code, try setting --no-quant-check to relax "
                "quantifier validation rules.",
                file=sys.stderr,
            )
        if args.debug:
            raise exn
        sys.exit(2)
    sys.exit(0)


if __name__ == '__main__':
    main()
