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

from __future__ import absolute_import, print_function
from builtins import range
from toil.test import ToilTest
from uuid import uuid4

import math
import os
import random
import tempfile

# Python 3 compatibility imports
from six.moves import xrange

class MiscTests(ToilTest):
    """
    This class contains miscellaneous tests that don't have enough content to be their own test
    file, and that don't logically fit in with any of the other test suites.
    """
    def setUp(self):
        super(MiscTests, self).setUp()
        self.testDir = self._createTempDir()

    def testGetSizeOfDirectoryWorks(self):
        from toil.common import getDirSizeRecursively
        # os.stat lists the number of 512-byte blocks used, but really, the file system is using
        # a blocksize specific to itself.  For instance, my machine uses 4096-byte blocks.
        #
        #  >>> with open('/tmp/temp', 'w') as fileHandle:
        #  ...     fileHandle.write(os.urandom(512*3))
        #  ...
        #  >>> os.stat('/tmp/temp').st_blksize
        #  4096
        #  >>> os.stat('/tmp/temp')
        #  posix.stat_result(st_mode=33188, st_ino=2630547, st_dev=16777220, st_nlink=1, st_uid=501,
        #                    st_gid=0, st_size=1536, st_atime=1475620048, st_mtime=1475628030,
        #                    st_ctime=1475628030)
        #  >>> os.stat('/tmp/temp').st_blocks
        #  8
        #
        # Even though the file is just 3 * 512 bytes, the file system uses one 4096-byte block and
        # os.stat says it is using eight 512-byte blocks.
        systemBlockSize = os.stat('.').st_blksize
        # a list of the directories used in the test
        directories = [self.testDir]
        # A dict of {FILENAME: FILESIZE} for all files used in the test
        files = {}
        # Create a random directory structure
        for i in range(0,10):
            directories.append(tempfile.mkdtemp(dir=random.choice(directories), prefix='test'))
        # Create 50 random file entries in different locations in the directories. 75% of the time
        # these are fresh files of sixe [1, 10] MB and 25% of the time they are hard links to old
        # files.
        while len(files) <= 50:
            fileName = os.path.join(random.choice(directories), self._getRandomName())
            if random.randint(0,100) < 75:
                # Create a fresh file in the range of 1-10 MB
                fileSize = int(round(random.random(), 2) * 10 * 1024 * 1024)
                with open(fileName, 'w') as fileHandle:
                    fileHandle.write(os.urandom(fileSize))
                files[fileName] = int(math.ceil(fileSize * 1.0 / systemBlockSize) * systemBlockSize)
            else:
                # Link to one of the previous files
                if len(files) == 0:
                    continue
                linkSrc = random.choice(list(files.keys()))
                os.link(linkSrc, fileName)
                files[fileName] = 'Link to %s' % linkSrc

        computedDirectorySize = getDirSizeRecursively(self.testDir)
        totalExpectedSize = sum([x for x in list(files.values()) if isinstance(x, int)])
        self.assertEqual(computedDirectorySize, totalExpectedSize)

    @staticmethod
    def _getRandomName():
        return uuid4().hex

