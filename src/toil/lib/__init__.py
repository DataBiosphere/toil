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
# subprocess32 is a backport of python3's subprocess module for use on Python2,
# and includes many reliability bug fixes relevant on POSIX platforms.
if os.name == 'posix' and sys.version_info[0] < 3:
    import subprocess32 as subprocess
else:
    import subprocess

FORGO = 0
STOP = 1
RM = 2


def dockerPredicate(e):
    """
    Used to ensure Docker exceptions are retried if appropriate

    :param e: Exception
    :return: True if e retriable, else False
    """
    if not isinstance(e, subprocess.CalledProcessError):
        return False
    if e.returncode == 125:
        return True
