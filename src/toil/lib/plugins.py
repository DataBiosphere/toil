# Copyright (C) 2015-2025 Regents of the University of California
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

"""
Generic plugin system for Toil plugins.

Plugins come in Python packages named::

    toil_{PLUGIN_TYPE}_{WHATEVER}

When looking for plugins, Toil will list all the Python packages with the right
name prefix for the given type of plugin, and load them. The plugin modules
then have an opportunity to import :meth:`register_plugin` and register
themselves.
"""

import importlib
import pkgutil
from typing import Any, Literal, Union

from toil.lib.memoize import memoize

PluginType = Union[Literal["batch_system"], Literal["url_access"]]
plugin_types: list[PluginType] = ["batch_system", "url_access"]

_registry: dict[str, dict[str, Any]] = {k: {} for k in plugin_types}


def register_plugin(
    plugin_type: PluginType, plugin_name: str, plugin_being_registered: Any
) -> None:
    """
    Adds a plugin to the registry for the given type of plugin.

    :param plugin_name: For batch systems, this is the string the user will use
        to select the batch system on the command line with ``--batchSystem``.
        For URL access plugins, this is the URL scheme that the plugin
        implements.
    :param plugin_being_registered: This is a function that, when called,
        imports and returns a plugin-provided class type. For batch systems,
        the resulting type must extend
        :class:`toil.batchSystems.abstractBatchSystem.AbstractBatchSystem`. For
        URL access plugins, it must extend :class:`toil.lib.url.URLAccess`.
        Note that the function used here should return the class itself; it
        should not construct an instance of the class.
    """
    _registry[plugin_type][plugin_name] = plugin_being_registered


def remove_plugin(plugin_type: PluginType, plugin_name: str) -> None:
    """
    Removes a plugin from the registry for the given type of plugin.
    """
    try:
        del _registry[plugin_type][plugin_name]
    except KeyError:
        # If the plugin does not exist, it can be ignored
        pass


def get_plugin_names(plugin_type: PluginType) -> list[str]:
    """
    Get the names of all the available plugins of the given type.
    """
    _load_all_plugins(plugin_type)
    return list(_registry[plugin_type].keys())


def get_plugin(plugin_type: PluginType, plugin_name: str) -> Any:
    """
    Get a plugin class factory function by name.

    :raises: KeyError if plugin_name is not the name of a plugin of the given
        type.
    """
    _load_all_plugins(plugin_type)
    return _registry[plugin_type][plugin_name]


def _plugin_name_prefix(plugin_type: PluginType) -> str:
    """
    Get prefix for plugin type.

    Any packages with prefix will count as toil plugins of that type.
    """
    return f"toil_{plugin_type}_"


@memoize
def _load_all_plugins(plugin_type: PluginType) -> None:
    """
    Load all the plugins of the given type that are installed.
    """
    prefix = _plugin_name_prefix(plugin_type)
    for finder, name, is_pkg in pkgutil.iter_modules():
        # For all installed packages
        if name.startswith(prefix):
            # If it is a Toil batch system plugin, import it
            importlib.import_module(name)
