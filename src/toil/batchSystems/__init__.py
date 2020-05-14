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

import sys

from functools import total_ordering

class DeadlockException(Exception):
    """
    Exception thrown by the Leader or BatchSystem when a deadlock is encountered due to insufficient
    resources to run the workflow
    """
    def __init__(self, msg):
        self.msg = "Deadlock encountered: " + msg
        super().__init__()

    def __str__(self):
        """
        Stringify the exception, including the message.
        """
        return self.msg

@total_ordering
class MemoryString:
    """
    Represents an amount of bytes, as a string, using suffixes for the unit.
    
    Comparable based on the actual number of bytes instead of string value.
    """
    def __init__(self, string):
        if string[-1] == 'K' or string[-1] == 'M' or string[-1] == 'G' or string[-1] == 'T': #10K
            self.unit = string[-1]
            self.val = float(string[:-1])
        elif len(string) >= 3 and (string[-2] == 'k' or string[-2] == 'M' or string[-2] == 'G' or string[-2] == 'T'):
            self.unit = string[-2]
            self.val = float(string[:-2])
        else:
            self.unit = 'B'
            self.val = float(string)
        self.bytes = self.byteVal()

    def __str__(self):
        if self.unit != 'B':
            return str(self.val) + self.unit
        else:
            return str(self.val)

    def byteVal(self):
        if self.unit == 'B':
            return self.val
        elif self.unit == 'K':
            return self.val * 1024
        elif self.unit == 'M':
            return self.val * 1048576
        elif self.unit == 'G':
            return self.val * 1073741824
        elif self.unit == 'T':
            return self.val * 1099511627776

    def __eq__(self, other):
        return self.bytes == other.bytes

    def __lt__(self, other):
        return self.bytes < other.bytes
