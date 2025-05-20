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

import logging
import pkgutil
import warnings
from collections.abc import Sequence
from typing import TYPE_CHECKING, Callable

from toil.lib.compatibility import deprecated
from toil.lib.memoize import memoize
import toil.lib.plugins

if TYPE_CHECKING:
    from toil.batchSystems.abstractBatchSystem import AbstractBatchSystem

logger = logging.getLogger(__name__)

#####
# Plugin system/API
#####


def add_batch_system_factory(
    key: str, class_factory: Callable[[], type["AbstractBatchSystem"]]
):
    """
    Adds a batch system to the registry for workflow or plugin-supplied batch systems.

    :param class_factory: A function that returns a batch system class (NOT an instance), which implements :class:`toil.batchSystems.abstractBatchSystem.AbstractBatchSystem`.
    """
    toil.lib.plugins.register_plugin("batch_system", key, class_factory)


def get_batch_systems() -> Sequence[str]:
    """
    Get the names of all the available batch systems.
    """
    return toil.lib.plugins.get_plugin_names("batch_system")


def get_batch_system(key: str) -> type["AbstractBatchSystem"]:
    """
    Get a batch system class by name.

    :raises: KeyError if the key is not the name of a batch system, and
             ImportError if the batch system's class cannot be loaded.
    """
    return toil.lib.plugins.get_plugin("batch_system", key)()


DEFAULT_BATCH_SYSTEM = "single_machine"

#####
# Built-in batch systems
#####


def aws_batch_batch_system_factory():
    from toil.batchSystems.awsBatch import AWSBatchBatchSystem

    return AWSBatchBatchSystem


def gridengine_batch_system_factory():
    from toil.batchSystems.gridengine import GridEngineBatchSystem

    return GridEngineBatchSystem


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


#####
# Registers all built-in batch system
#####

add_batch_system_factory("aws_batch", aws_batch_batch_system_factory)
add_batch_system_factory("single_machine", single_machine_batch_system_factory)
add_batch_system_factory("grid_engine", gridengine_batch_system_factory)
add_batch_system_factory("lsf", lsf_batch_system_factory)
add_batch_system_factory("mesos", mesos_batch_system_factory)
add_batch_system_factory("slurm", slurm_batch_system_factory)
add_batch_system_factory("torque", torque_batch_system_factory)
add_batch_system_factory("htcondor", htcondor_batch_system_factory)
add_batch_system_factory("kubernetes", kubernetes_batch_system_factory)