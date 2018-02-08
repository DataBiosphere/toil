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
# limitations under the License.

# Python 3 compatibility imports
from __future__ import absolute_import, print_function
from six import iteritems, iterkeys

import pkg_resources
import os
import sys
import re

from toil.version import version


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
    from toil.utils import (toilKill,
                            toilStats,
                            toilStatus,
                            toilClean,
                            toilLaunchCluster,
                            toilDestroyCluster,
                            toilSshCluster,
                            toilRsyncCluster,
                            toilDebugFile,
                            toilDebugJob)
    commandMapping = { "-".join(
                     map(lambda x : x.lower(), re.findall('[A-Z][^A-Z]*', name)
                     )) : module for name, module in iteritems(locals())}
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
