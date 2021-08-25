# Copyright (C) 2015-2021 Regents of the University of California
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
# limitations under the License.


from typing import Callable, Type, TYPE_CHECKING

if TYPE_CHECKING:
    from toil.batchSystems.abstractBatchSystem import AbstractBatchSystem


def gridengine_batch_system_factory():
    from toil.batchSystems.gridengine import GridEngineBatchSystem
    return GridEngineBatchSystem


def parasol_batch_system_factory():
    from toil.batchSystems.parasol import ParasolBatchSystem
    return ParasolBatchSystem


def lsf_batch_system_factory():
    from toil.batchSystems.lsf import LSFBatchSystem
    return LSFBatchSystem


def single_machine_batch_system_factory():
    from toil.batchSystems.singleMachine import SingleMachineBatchSystem
    return SingleMachineBatchSystem


def mesos_batch_system_factory():
    from toil.batchSystems.mesos.batchSystem import MesosBatchSystem
    return MesosBatchSystem


def slurm_batch_system_factory():
    from toil.batchSystems.slurm import SlurmBatchSystem
    return SlurmBatchSystem


def torque_batch_system_factory():
    from toil.batchSystems.torque import TorqueBatchSystem
    return TorqueBatchSystem


def htcondor_batch_system_factory():
    from toil.batchSystems.htcondor import HTCondorBatchSystem
    return HTCondorBatchSystem


def kubernetes_batch_system_factory():
    from toil.batchSystems.kubernetes import KubernetesBatchSystem
    return KubernetesBatchSystem


BATCH_SYSTEM_FACTORY_REGISTRY = {
    'parasol'        : parasol_batch_system_factory,
    'single_machine' : single_machine_batch_system_factory,
    'grid_engine'    : gridengine_batch_system_factory,
    'lsf'            : lsf_batch_system_factory,
    'mesos'          : mesos_batch_system_factory,
    'slurm'          : slurm_batch_system_factory,
    'torque'         : torque_batch_system_factory,
    'htcondor'       : htcondor_batch_system_factory,
    'kubernetes'     : kubernetes_batch_system_factory
}
BATCH_SYSTEMS = list(BATCH_SYSTEM_FACTORY_REGISTRY.keys())
DEFAULT_BATCH_SYSTEM = 'single_machine'

def addBatchSystemFactory(key: str, batchSystemFactory: Callable[[], Type['AbstractBatchSystem']]):
    """
    Adds a batch system to the registry for workflow-supplied batch systems.
    """
    BATCH_SYSTEMS.append(key)
    BATCH_SYSTEM_FACTORY_REGISTRY[key] = batchSystemFactory