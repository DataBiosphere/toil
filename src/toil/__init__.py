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

from __future__ import absolute_import

import os
import sys

from subprocess import check_output

from bd2k.util import memoize


def toilPackageDirPath():
    """
    Returns the absolute path of the directory that corresponds to the top-level toil package.
    The return value is guaranteed to end in '/toil'.
    """
    result = os.path.dirname(os.path.realpath(__file__))
    assert result.endswith('/toil')
    return result


def inVirtualEnv():
    return hasattr(sys, 'real_prefix')


def resolveEntryPoint(entryPoint):
    """
    Returns the path to the given entry point (see setup.py) that *should* work on a worker. The
    return value may be an absolute or a relative path.
    """
    if inVirtualEnv():
        path = os.path.join(os.path.dirname(sys.executable), entryPoint)
        # Inside a virtualenv we try to use absolute paths to the entrypoints. 
        if os.path.isfile(path):
            # If the entrypoint is present, Toil must have been installed into the virtualenv (as 
            # opposed to being included via --system-site-packages). For clusters this means that 
            # if Toil is installed in a virtualenv on the leader, it must be installed in
            # a virtualenv located at the same path on each worker as well.
            assert os.access(path, os.X_OK)
            return path
        else:
            # For virtualenv's that have the toil package directory on their sys.path but whose 
            # bin directory lacks the Toil entrypoints, i.e. where Toil is included via 
            # --system-site-packages, we rely on PATH just as if we weren't in a virtualenv. 
            return entryPoint
    else:
        # Outside a virtualenv it is hard to predict where the entry points got installed. It is
        # the reponsibility of the user to ensure that they are present on PATH and point to the
        # correct version of Toil. This is still better than an absolute path because it gives
        # the user control over Toil's location on both leader and workers.
        return entryPoint


@memoize
def physicalMemory():
    """
    >>> n = physicalMemory()
    >>> n > 0
    True
    >>> n == physicalMemory()
    True
    """
    try:
        return os.sysconf('SC_PAGE_SIZE') * os.sysconf('SC_PHYS_PAGES')
    except ValueError:
        return int(check_output(['sysctl', '-n', 'hw.memsize']).strip())


def physicalDisk(config, toilWorkflowDir=None):
    if toilWorkflowDir is None:
        from toil.common import Toil
        toilWorkflowDir = Toil.getWorkflowDir(config.workflowID, config.workDir)
    diskStats = os.statvfs(toilWorkflowDir)
    return diskStats.f_frsize * diskStats.f_bavail

