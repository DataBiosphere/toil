import importlib
from typing import Any, Literal
import pkgutil
from toil.lib.memoize import memoize


PluginType = Literal["batch_system"] | Literal["url_access"]
plugin_types: list[PluginType] = ["batch_system", "url_access"]

_registry: dict[str, dict[str, Any]] = {k: {} for k in plugin_types}

def register_plugin(
    plugin_type: PluginType, plugin_name: str, plugin_being_registered: Any
) -> None:
    """
    Adds a plugin to the registry for the given type of plugin.
    """
    _registry[plugin_type][plugin_name] = plugin_being_registered

def remove_plugin(
    plugin_type: PluginType, plugin_name: str) -> None:
    """
    Removes a plugin from the registry for the given type of plugin.
    """
    try:
        del _registry[plugin_type][plugin_name]
    except KeyError:
        # If the plugin does not exist, it can be ignored
        pass

def get_plugin_names(plugin_type:PluginType) -> list[str]:
    """
    Get the names of all the available plugins.
    """
    _load_all_plugins(plugin_type)
    return list(_registry[plugin_type].keys())

def get_plugin(plugin_type: PluginType, plugin_name: str) -> Any:
    """
    Get a plugin class by name.

    :raises: KeyError if the key is not the name of a plugin, and
             ImportError if the plugin's class cannot be loaded.
    """
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
