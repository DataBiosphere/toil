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
        if os.path.exists("cwl.output.json"):
            os.remove("cwl.output.json")

        cwltoil.main(["src/toil/test/cwl/revsort.cwl", "src/toil/test/cwl/revsort-job.json"])
        with open("cwl.output.json") as f:
            out = json.load(f)
        os.remove("cwl.output.json")
        self.assertEquals(out, {
            "output": {
                "path": "/home/peter/work/toil/output.txt",
                "size": 1111,
                "class": "File",
                "checksum": "sha1$b9214658cc453331b62c2282b772a5c063dbd284"
            }})
