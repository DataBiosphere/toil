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
import subprocess
import re
import StringIO

from toil.test import ToilTest, needs_cwl

@needs_cwl
class CWLTest(ToilTest):

    def _tester(self, cwlfile, jobfile, outDir, expect):
        from toil.cwl import cwltoil
        rootDir = self._projectRootPath()
        st = StringIO.StringIO()
        cwltoil.main(['--outdir', outDir,
                            os.path.join(rootDir, cwlfile),
                            os.path.join(rootDir, jobfile)],
                     stdout=st)
        out = json.loads(st.getvalue())
        # locations are internal objects in output for CWL
        out["output"].pop("location", None)
        self.assertEquals(out, expect)

    def test_run_revsort(self):
        outDir = self._createTempDir()
        self._tester('src/toil/test/cwl/revsort.cwl',
                     'src/toil/test/cwl/revsort-job.json',
                     outDir, {
            # Having unicode string literals isn't necessary for the assertion but makes for a
            # less noisy diff in case the assertion fails.
            u'output': {
                u'path': unicode(os.path.join(outDir, 'output.txt')),
                u'basename': unicode("output.txt"),
                u'size': 1111,
                u'class': u'File',
                u'checksum': u'sha1$b9214658cc453331b62c2282b772a5c063dbd284'}})


    def test_run_conformance(self):
        rootDir = self._projectRootPath()
        cwlSpec = os.path.join(rootDir, 'src/toil/test/cwl/spec')
        if os.path.exists(cwlSpec):
            subprocess.call(["git", "fetch"], cwd=cwlSpec)
        else:
            subprocess.check_call(["git", "clone", "https://github.com/common-workflow-language/common-workflow-language.git", cwlSpec])
        subprocess.check_call(["git", "checkout", "1d5714dc434ffd6ac45ec64f1535475fa163be09"], cwd=cwlSpec)
        subprocess.check_call(["git", "clean", "-f", "-x", "."], cwd=cwlSpec)
        try:
            subprocess.check_output(["./run_test.sh", "RUNNER=cwltoil", "DRAFT=v1.0"], cwd=cwlSpec,
                                    stderr=subprocess.STDOUT)
        except subprocess.CalledProcessError as e:
            only_unsupported = False
            # check output -- if we failed but only have unsupported features, we're okay
            p = re.compile(r"(?P<failures>\d+) failures, (?P<unsupported>\d+) unsupported features")
            for line in e.output.split("\n"):
                m = p.search(line)
                if m:
                    if int(m.group("failures")) == 0 and int(m.group("unsupported")) > 0:
                        only_unsupported = True
                        break
            if not only_unsupported:
                print(e.output)
                raise e
