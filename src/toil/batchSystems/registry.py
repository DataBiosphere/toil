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


def _gridengineBatchSystemFactory():
    from toil.batchSystems.gridengine import GridEngineBatchSystem
    return GridEngineBatchSystem


def _parasolBatchSystemFactory():
    from toil.batchSystems.parasol import ParasolBatchSystem
    return ParasolBatchSystem


def _lsfBatchSystemFactory():
    from toil.batchSystems.lsf import LSFBatchSystem
    return LSFBatchSystem


def _singleMachineBatchSystemFactory():
    from toil.batchSystems.singleMachine import SingleMachineBatchSystem
    return SingleMachineBatchSystem


def _mesosBatchSystemFactory():
    from toil.batchSystems.mesos.batchSystem import MesosBatchSystem
    return MesosBatchSystem


def _slurmBatchSystemFactory():
    from toil.batchSystems.slurm import SlurmBatchSystem
    return SlurmBatchSystem


def _torqueBatchSystemFactory():
    from toil.batchSystems.torque import TorqueBatchSystem
    return TorqueBatchSystem


def _htcondorBatchSystemFactory():
    from toil.batchSystems.htcondor import HTCondorBatchSystem
    return HTCondorBatchSystem


def _kubernetesBatchSystemFactory():
    from toil.batchSystems.kubernetes import KubernetesBatchSystem
    return KubernetesBatchSystem


_DEFAULT_REGISTRY = {
    'parasol'        : _parasolBatchSystemFactory,
    'single_machine' : _singleMachineBatchSystemFactory,
    'grid_engine'    : _gridengineBatchSystemFactory,
    'lsf'            : _lsfBatchSystemFactory,
    'mesos'          : _mesosBatchSystemFactory,
    'slurm'          : _slurmBatchSystemFactory,
    'torque'         : _torqueBatchSystemFactory,
    'htcondor'       : _htcondorBatchSystemFactory,
    'kubernetes'     : _kubernetesBatchSystemFactory,
    }

_UNIQUE_NAME = {
    'parasol',
    'single_machine',
    'grid_engine',
    'lsf',
    'mesos',
    'slurm',
    'torque',
    'htcondor',
    'kubernetes'
    }

_batchSystemRegistry = _DEFAULT_REGISTRY.copy()
_batchSystemNames = set(_UNIQUE_NAME)


def addBatchSystemFactory(key, batchSystemFactory):
    _batchSystemNames.add(key)
    _batchSystemRegistry[key] = batchSystemFactory


def batchSystemFactoryFor(batchSystem):
    return _batchSystemRegistry[batchSystem]


def defaultBatchSystem():
    return 'single_machine'


def uniqueNames():
    return list(_batchSystemNames)


def batchSystems():
    list(set(_batchSystemRegistry.values()))
