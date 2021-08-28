import os
import re
import sys
import textwrap
import types

import pkg_resources

from toil.version import version
from typing import Any, Dict

def main() -> None:
    modules = loadModules()

    if len(sys.argv) < 2 or sys.argv[1] == '--help':
        printHelp(modules)
        sys.exit(0)

    cmd = sys.argv[1]
    if cmd == '--version':
        printVersion()
        sys.exit(0)

    try:
        module = modules[cmd]
    except KeyError:
        sys.stderr.write(f'Unknown option "{cmd}".  Pass --help to display usage information.\n')
        sys.exit(1)

    del sys.argv[1]
    get_or_die(module, 'main')()

def get_or_die(module: types.ModuleType, name: str) -> Any:
    """
    Get an object from a module or complain that it is missing.
    """
    if hasattr(module, name):
        return getattr(module, name)
    else:
        sys.stderr.write(f'Internal Toil error!\nToil utility module '
                         f'{module.__name__} is missing required attribute {name}\n')
        sys.exit(1)

def loadModules() -> Dict[str, types.ModuleType]:
    # noinspection PyUnresolvedReferences
    from toil.utils import (toilClean,
                            toilDebugFile,
                            toilDebugJob,
                            toilDestroyCluster,
                            toilKill,
                            toilLaunchCluster,
                            toilRsyncCluster,
                            toilSshCluster,
                            toilStats,
                            toilStatus)
    return {"-".join([i.lower() for i in re.findall('[A-Z][^A-Z]*', name)]): module for name, module in locals().items()}


def printHelp(modules: Dict[str, types.ModuleType]) -> None:
    name = os.path.basename(sys.argv[0])
    descriptions = '\n        '.join(f'{cmd} - {get_or_die(mod, "__doc__").strip()}' for cmd, mod in modules.items() if mod)
    print(textwrap.dedent(f"""
        Usage: {name} COMMAND ...
               {name} --help
               {name} COMMAND --help

        Where COMMAND is one of the following:

        {descriptions}
        """[1:]))


def printVersion() -> None:
    try:
        print(pkg_resources.get_distribution('toil').version)
    except:
        print(f'Version gathered from toil.version: {version}')
