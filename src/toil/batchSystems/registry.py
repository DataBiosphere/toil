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

import importlib
import logging
import pkgutil
import warnings
from collections.abc import Sequence
from typing import TYPE_CHECKING, Callable

from toil.lib.compatibility import deprecated
from toil.lib.memoize import memoize

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
    _registry_keys.append(key)
    _registry[key] = class_factory


def get_batch_systems() -> Sequence[str]:
    """
    Get the names of all the availsble batch systems.
    """
    _load_all_plugins()

    return _registry_keys


def get_batch_system(key: str) -> type["AbstractBatchSystem"]:
    """
    Get a batch system class by name.

    :raises: KeyError if the key is not the name of a batch system, and
             ImportError if the batch system's class cannot be loaded.
    """

    return _registry[key]()


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
# Registry implementation
#####

_registry: dict[str, Callable[[], type["AbstractBatchSystem"]]] = {
    "aws_batch": aws_batch_batch_system_factory,
    "single_machine": single_machine_batch_system_factory,
    "grid_engine": gridengine_batch_system_factory,
    "lsf": lsf_batch_system_factory,
    "mesos": mesos_batch_system_factory,
    "slurm": slurm_batch_system_factory,
    "torque": torque_batch_system_factory,
    "htcondor": htcondor_batch_system_factory,
    "kubernetes": kubernetes_batch_system_factory,
}
_registry_keys = list(_registry.keys())

# We will load any packages starting with this prefix and let them call
# add_batch_system_factory()
_PLUGIN_NAME_PREFIX = "toil_batch_system_"


@memoize
def _load_all_plugins() -> None:
    """
    Load all the batch system plugins that are installed.
    """

    for finder, name, is_pkg in pkgutil.iter_modules():
        # For all installed packages
        if name.startswith(_PLUGIN_NAME_PREFIX):
            # If it is a Toil batch system plugin, import it
            importlib.import_module(name)


#####
# Deprecated API
#####

# We used to directly access these constants, but now the Right Way to use this
# module is add_batch_system_factory() to register and get_batch_systems() to
# get the list/get_batch_system() to get a class by name.


def __getattr__(name):
    """
    Implement a fallback attribute getter to handle deprecated constants.

    See <https://stackoverflow.com/a/48242860>.
    """
    if name == "BATCH_SYSTEM_FACTORY_REGISTRY":
        warnings.warn(
            "BATCH_SYSTEM_FACTORY_REGISTRY is deprecated; use get_batch_system() or add_batch_system_factory()",
            DeprecationWarning,
        )
        return _registry
    elif name == "BATCH_SYSTEMS":
        warnings.warn(
            "BATCH_SYSTEMS is deprecated; use get_batch_systems()", DeprecationWarning
        )
        return _registry_keys
    else:
        raise AttributeError(f"Module {__name__} ahs no attribute {name}")


@deprecated(new_function_name="add_batch_system_factory")
def addBatchSystemFactory(
    key: str, batchSystemFactory: Callable[[], type["AbstractBatchSystem"]]
):
    """
    Deprecated method to add a batch system.
    """
    return add_batch_system_factory(key, batchSystemFactory)


#####
# Testing utilities
#####

# We need a snapshot save/restore system for testing. We can't just tamper with
# the globals because module-level globals are their own references, so we
# can't touch this module's global name bindings from a client module.


def save_batch_system_plugin_state() -> (
    tuple[list[str], dict[str, Callable[[], type["AbstractBatchSystem"]]]]
):
    """
    Return a snapshot of the plugin registry that can be restored to remove
    added plugins. Useful for testing the plugin system in-process with other
    tests.
    """

    snapshot = (list(_registry_keys), dict(_registry))
    return snapshot


def restore_batch_system_plugin_state(
    snapshot: tuple[list[str], dict[str, Callable[[], type["AbstractBatchSystem"]]]]
):
    """
    Restore the batch system registry state to a snapshot from
    save_batch_system_plugin_state().
    """

    # We need to apply the snapshot without rebinding the names, because that
    # won't affect modules that imported the names.
    wanted_batch_systems, wanted_registry = snapshot
    _registry_keys.clear()
    _registry_keys.extend(wanted_batch_systems)
    _registry.clear()
    _registry.update(wanted_registry)
