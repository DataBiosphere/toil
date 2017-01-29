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
import sys

if sys.version_info >= (3, 0):

    # https://docs.python.org/3.0/whatsnew/3.0.html#ordering-comparisons
    def cmp(a, b):
        return (a > b) - (a < b)

class MemoryString:
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

    def __cmp__(self, other):
        return cmp(self.bytes, other.bytes)
