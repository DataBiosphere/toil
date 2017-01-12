from __future__ import absolute_import, print_function
from toil.version import version
import pkg_resources
import os
import sys

# Python 3 compatibility imports
from six import iteritems, iterkeys

def main():
    modules = loadModules()
    try:
        command = sys.argv[1]
    except IndexError:
        printHelp(modules)
    else:
        if command == '--help':
            printHelp(modules)
        elif command == '--version':
            try:
                print(pkg_resources.get_distribution('toil').version)
            except:
                print("Version gathered from toil.version: "+version)
        else:
            try:
                module = modules[command]
            except KeyError:
                print("Unknown option '%s'. "
                      "Pass --help to display usage information.\n" % command, file=sys.stderr)
                sys.exit(1)
            else:
                del sys.argv[1]
                module.main()


def loadModules():
    # noinspection PyUnresolvedReferences
    from toil.utils import toilKill, toilStats, toilStatus, toilClean, toilLaunchCluster, toilDestroyCluster, toilSSHCluster, toilRsyncCluster
    commandMapping = {name[4:].lower(): module for name, module in iteritems(locals())}
    commandMapping = {name[:-7]+'-'+name[-7:] if name.endswith('cluster') else name: module for name, module in iteritems(commandMapping)}
    return commandMapping

def printHelp(modules):
    usage = ("\n"
             "Usage: {name} COMMAND ...\n"
             "       {name} --help\n"
             "       {name} COMMAND --help\n\n"
             "where COMMAND is one of the following:\n\n{descriptions}\n\n")
    print(usage.format(
        name=os.path.basename(sys.argv[0]),
        commands='|'.join(iterkeys(modules)),
        descriptions='\n'.join("%s - %s" % (n, m.__doc__.strip()) for n, m in iteritems(modules))))
