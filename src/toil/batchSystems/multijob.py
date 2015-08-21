#!/usr/bin/env python

# Copyright (C) 2015 UCSC Computational Genomics Lab
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
import os
import subprocess

try:
    import cPickle 
except ImportError:
    import pickle as cPickle

class MultiJob:
    def __init__(self, commands):
        self.commands = commands

    def execute(self):
        task_id = int(os.environ['SGE_TASK_ID'])
        if task_id is None:
             RuntimeError("Multi-job launched without task id")
        if task_id < 1 or task_id > len(self.commands):
             RuntimeError("Task ID not within the array range 1 <= %i <= %i", task_id, len(self.commands))
        (job, outfile) = self.commands[task_id - 1]
        if outfile is None:
                ret = subprocess.call(job.split())
        else:
                file = open(outfile, "w")
                ret = subprocess.call(job.split(), stderr=file,stdout=file)
                file.close()
        sys.exit(ret)

    def makeRunnable(self, tempDir):
        from toil.common import toilPackageDirPath

        pickleFile = tempDir.getTempFile(".pickle")
        fileHandle = open(pickleFile, 'w')
        cPickle.dump(self, fileHandle, cPickle.HIGHEST_PROTOCOL)
        fileHandle.close() 
        multijobexec = os.path.join(toilPackageDirPath(), "bin", "multijob")
        jtPath = os.path.split(toilPackageDirPath())[0]
        return "%s %s %s" % (multijobexec, pickleFile, jtPath)


def main():
    global fileHandle, multijob
    fileHandle = open(sys.argv[1], 'r')
    sys.path += [sys.argv[2]]
    multijob = cPickle.load(fileHandle)
    fileHandle.close()
    multijob.execute()


if __name__ == "__main__":
        main()
