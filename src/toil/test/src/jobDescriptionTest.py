# Copyright (C) 2015-2021 Regents of the University of California
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

import os
from argparse import ArgumentParser

from toil.common import Toil
from toil.job import Job, JobDescription, TemporaryID
from toil.test import ToilTest


class JobDescriptionTest(ToilTest):

    def setUp(self):
        super().setUp()
        self.jobStorePath = self._getTestJobStorePath()
        parser = ArgumentParser()
        Job.Runner.addToilOptions(parser)
        options = parser.parse_args(args=[self.jobStorePath])
        self.toil = Toil(options)
        self.assertEqual( self.toil, self.toil.__enter__() )

    def tearDown(self):
        self.toil.__exit__(None, None, None)
        self.toil._jobStore.destroy()
        self.assertFalse(os.path.exists(self.jobStorePath))
        super().tearDown()

    def testJobDescription(self):
        """
        Tests the public interface of a JobDescription.
        """

        command = "by your command"
        memory = 2^32
        disk = 2^32
        cores = "1"
        preemptible = 1

        j = JobDescription(command=command, requirements={"memory": memory, "cores": cores, "disk": disk, "preemptible": preemptible},
                           jobName='testJobGraph', unitName='noName')

        #Check attributes
        self.assertEqual(j.command, command)
        self.assertEqual(j.memory, memory)
        self.assertEqual(j.disk, disk)
        self.assertEqual(j.cores, int(cores))
        self.assertEqual(j.preemptible, bool(preemptible))
        self.assertEqual(type(j.jobStoreID), TemporaryID)
        self.assertEqual(list(j.successorsAndServiceHosts()), [])
        self.assertEqual(list(j.allSuccessors()), [])
        self.assertEqual(list(j.serviceHostIDsInBatches()), [])
        self.assertEqual(list(j.services), [])
        self.assertEqual(list(j.nextSuccessors()), [])
        self.assertEqual(j.predecessorsFinished, set())
        self.assertEqual(j.logJobStoreFileID, None)

        #Check equals function (should be based on object identity and not contents)
        j2 = JobDescription(command=command, requirements={"memory": memory, "cores": cores, "disk": disk, "preemptible": preemptible},
                            jobName='testJobGraph', unitName='noName')
        self.assertNotEqual(j, j2)
        ###TODO test other functionality

    def testJobDescriptionSequencing(self):
        j = JobDescription(command='command', requirements={},  jobName='unimportant')

        j.addChild('child')
        j.addFollowOn('followOn')

        # With a command, nothing should be ready to run
        self.assertEqual(list(j.nextSuccessors()), [])

        # With command cleared, child should be ready to run
        j.command = None
        self.assertEqual(list(j.nextSuccessors()), ['child'])

        # Without the child, the follow-on should be ready to run
        j.filterSuccessors(lambda jID: jID != 'child')
        self.assertEqual(list(j.nextSuccessors()), ['followOn'])

        # Without the follow-on, we should return None, to be distinct from an
        # empty list. Nothing left to do!
        j.filterSuccessors(lambda jID: jID != 'followOn')
        self.assertEqual(j.nextSuccessors(), None)
