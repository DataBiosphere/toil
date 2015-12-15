# Copyright (C) 2015 Curoverse, Inc
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
import json
import os

from toil.test import ToilTest, needs_cwl

@needs_cwl
class CWLTest(ToilTest):
    def test_run_revsort(self):
        from toil.cwl import cwltoil
        outDir = self._createTempDir()
        rootDir = self._projectRootPath()
        outputJson = os.path.join(outDir, 'cwl.output.json')
        try:
            cwltoil.main(['--outdir', outDir,
                          os.path.join(rootDir, 'src/toil/test/cwl/revsort.cwl'),
                          os.path.join(rootDir, 'src/toil/test/cwl/revsort-job.json')])
            with open(outputJson) as f:
                out = json.load(f)
        finally:
            if os.path.exists(outputJson):
                os.remove(outputJson)
        self.assertEquals(out, {
            # Having unicode string literals isn't necessary for the assertion but makes for a
            # less noisy diff in case the assertion fails.
            u'output': {
                u'path': unicode(os.path.join(outDir, 'output.txt')),
                u'size': 1111,
                u'class': u'File',
                u'checksum': u'sha1$b9214658cc453331b62c2282b772a5c063dbd284'}})
