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
from builtins import str
from builtins import range
from toil.common import getNodeID
from toil.test import ToilTest

class GetNodeIDTest(ToilTest):
    def testIDStability(self):
        prevNodeID = None
        for i in range(10):
            nodeID = getNodeID()
            if i > 0:
                self.assertEquals(nodeID,prevNodeID)
            prevNodeID = nodeID

    def testSkipNonExistingFile(self):
        import uuid, os
        no_file = "/etc/totally_non_existing_"+str(uuid.uuid4())
        self.assertFalse(os.path.exists(no_file), "Could not make up a missing file name")
        nodeID1 = getNodeID([no_file])
        nodeID2 = getNodeID()
        self.assertEquals(nodeID1,nodeID2)

