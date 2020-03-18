from __future__ import absolute_import
# Copyright (C) 2015-2016 Regents of the University of California
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
#

from toil.lib.threading import cpu_count

from .registry import batchSystemFactoryFor, defaultBatchSystem, uniqueNames

import socket
from contextlib import closing

def getPublicIP():
    """Get the IP that this machine uses to contact the internet.

    If behind a NAT, this will still be this computer's IP, and not the router's."""
    try:
        # Try to get the internet-facing IP by attempting a connection
        # to a non-existent server and reading what IP was used.
        with closing(socket.socket(socket.AF_INET, socket.SOCK_DGRAM)) as sock:
            # 203.0.113.0/24 is reserved as TEST-NET-3 by RFC 5737, so
            # there is guaranteed to be no one listening on the other
            # end (and we won't accidentally DOS anyone).
            sock.connect(('203.0.113.1', 1))
            ip = sock.getsockname()[0]
        return ip
    except:
        # Something went terribly wrong. Just give loopback rather
        # than killing everything, because this is often called just
        # to provide a default argument
        return '127.0.0.1'


def _parasolOptions(addOptionFn, config=None):
    addOptionFn("--parasolCommand", dest="parasolCommand", default=None,
                      help="The name or path of the parasol program. Will be looked up on PATH "
                           "unless it starts with a slash. default=%s" % 'parasol')
    addOptionFn("--parasolMaxBatches", dest="parasolMaxBatches", default=None,
                help="Maximum number of job batches the Parasol batch is allowed to create. One "
                     "batch is created for jobs with a a unique set of resource requirements. "
                     "default=%i" % 1000)


def _singleMachineOptions(addOptionFn, config):
    addOptionFn("--scale", dest="scale", default=None,
                help=("A scaling factor to change the value of all submitted "
                      "tasks's submitted cores. Used in singleMachine batch "
                      "system. default=%s" % 1))
    if config.cwl:
        addOptionFn(
            "--noLinkImports", dest="linkImports", default=True,
            action='store_false', help="When using a filesystem based job "
            "store, CWL input files are by default symlinked in. "
            "Specifying this option instead copies the files into the job "
            "store, which may protect them from being modified externally. "
            "When not specified and as long as caching is enabled, Toil will "
            "protect the file automatically by changing the permissions to "
            "read-only.")
        addOptionFn(
            "--noMoveExports", dest="moveExports", default=True,
            action='store_false', help="When using a filesystem based job "
            "store, output files are by default moved to the output directory, "
            "and a symlink to the moved exported file is created at the initial location. "                           
            "Specifying this option instead copies the files into the output "
            "directory. Applies to filesystem-based job stores only.")
    else:
        addOptionFn(
            "--linkImports", dest="linkImports", default=False,
            action='store_true', help="When using Toil's importFile function "
            "for staging, input files are copied to the job store. Specifying "
            "this option saves space by sym-linking imported files. As long "
            "as caching is enabled Toil will protect the file "
            "automatically by changing the permissions to read-only.")
        addOptionFn(
            "--moveExports", dest="moveExports", default=False,
            action='store_true', help="When using Toil's exportFile function "
            "for staging, output files are copied to the output directory. Specifying "
            "this option saves space by moving exported files, and making a symlink to "
            "the exported file in the job store. Applies to filesystem-based job stores only.")

def _mesosOptions(addOptionFn, config=None):
    addOptionFn("--mesosMaster", dest="mesosMasterAddress", default=getPublicIP() + ':5050',
                help=("The host and port of the Mesos master separated by colon. (default: %(default)s)"))

def _kubernetesOptions(addOptionFn, config=None):
    addOptionFn("--kubernetesHostPath", dest="kubernetesHostPath", default=None,
                help=("Path on Kubernetes hosts to use as shared inter-pod temp directory (default: %(default)s)"))

# Built in batch systems that have options
_options = [
    _parasolOptions,
    _singleMachineOptions,
    _mesosOptions,
    _kubernetesOptions
    ]


def addOptionsDefinition(optionsDefinition):
    _options.append(optionsDefinition)


def setOptions(config, setOption):
    batchSystem = config.batchSystem

    factory = batchSystemFactoryFor(batchSystem)
    batchSystem = factory()

    batchSystem.setOptions(setOption)


def addOptions(addOptionFn, config):
    addOptionFn("--batchSystem", dest="batchSystem", default=defaultBatchSystem(),
                help=("The type of batch system to run the job(s) with, currently can be one "
                      "of %s'. default=%s" % (', '.join(uniqueNames()), defaultBatchSystem())))
    addOptionFn("--disableHotDeployment", dest="disableAutoDeployment",
                action='store_true', default=None,
                help=("Hot-deployment was renamed to auto-deployment.  Option now redirects to "
                "--disableAutoDeployment.  Left in for backwards compatibility."))
    addOptionFn("--disableAutoDeployment", dest="disableAutoDeployment",
                action='store_true', default=None,
                help=("Should auto-deployment of the user script be deactivated? If True, the user "
                      "script/package should be present at the same location on all workers. "
                      "default=false"))
    localCores = cpu_count()
    addOptionFn("--maxLocalJobs", default=localCores,
                help="For batch systems that support a local queue for "
                "housekeeping jobs (Mesos, GridEngine, htcondor, lsf, slurm, "
                "torque), the maximum number of these housekeeping jobs to "
                "run on the local system. "
                "The default (equal to the number of cores) is a maximum of "
                "{} concurrent local housekeeping jobs.".format(localCores))
    addOptionFn("--manualMemArgs", default=False, action='store_true', dest="manualMemArgs",
                help="Do not add the default arguments: 'hv=MEMORY' & 'h_vmem=MEMORY' to "
                     "the qsub call, and instead rely on TOIL_GRIDGENGINE_ARGS to supply "
                     "alternative arguments.  Requires that TOIL_GRIDGENGINE_ARGS be set.")
    addOptionFn("--runCwlInternalJobsOnWorkers", dest="runCwlInternalJobsOnWorkers",
                action='store_true', default=None,
                help=("Whether to run CWL internal jobs (e.g. CWLScatter) on the worker nodes "
                      "instead of the primary node. If false (default), then all such jobs are run on "
                      "the primary node. Setting this to true can speed up the pipeline for very large "
                      "workflows with many sub-workflows and/or scatters, provided that the worker "
                      "pool is large enough."))

    for o in _options:
        o(addOptionFn, config)

def setDefaultOptions(config):
    """
    Set default options for builtin batch systems. This is required if a Config
    object is not constructed from an Options object.
    """
    config.batchSystem = "singleMachine"
    config.disableAutoDeployment = False
    config.environment = {}
    config.statePollingWait = None  # if not set, will default to seconds in getWaitDuration()
    config.maxLocalJobs = cpu_count()
    config.manualMemArgs = False

    # parasol
    config.parasolCommand = 'parasol'
    config.parasolMaxBatches = 10000

    # single machine
    config.scale = 1
    config.linkImports = False
    config.moveExports = False

    # mesos
    config.mesosMasterAddress = '%s:5050' % getPublicIP()
    
    # Kubernetes
    config.kubernetesHostPath = None

    
