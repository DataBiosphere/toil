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

from toil.common import Config
from toil.job import CheckpointJobDescription, JobDescription
from toil.jobStores.fileJobStore import FileJobStore
from toil.test import ToilTest
from toil.worker import nextChainable


class WorkerTests(ToilTest):
    """Test miscellaneous units of the worker."""
    def setUp(self):
        super().setUp()
        path = self._getTestJobStorePath()
        self.jobStore = FileJobStore(path)
        self.config = Config()
        self.config.jobStore = 'file:%s' % path
        self.jobStore.initialize(self.config)
        self.jobNumber = 0

    def testNextChainable(self):
        """Make sure chainable/non-chainable jobs are identified correctly."""
        def createTestJobDesc(memory, cores, disk, preemptible, checkpoint):
            """
            Create a JobDescription with no command (representing a Job that
            has already run) and return the JobDescription.
            """
            name = 'job%d' % self.jobNumber
            self.jobNumber += 1

            descClass = CheckpointJobDescription if checkpoint else JobDescription
            jobDesc = descClass(requirements={'memory': memory, 'cores': cores, 'disk': disk, 'preemptible': preemptible}, jobName=name)

            # Assign an ID
            self.jobStore.assign_job_id(jobDesc)

            # Save and return the JobDescription
            return self.jobStore.create_job(jobDesc)

        for successorType in ['addChild', 'addFollowOn']:
            # Try with the branch point at both child and follow-on stages

            # Identical non-checkpoint jobs should be chainable.
            jobDesc1 = createTestJobDesc(1, 2, 3, True, False)
            jobDesc2 = createTestJobDesc(1, 2, 3, True, False)
            getattr(jobDesc1, successorType)(jobDesc2.jobStoreID)
            chainable = nextChainable(jobDesc1, self.jobStore, self.config)
            self.assertNotEqual(chainable, None)
            self.assertEqual(jobDesc2.jobStoreID, chainable.jobStoreID)

            # Identical checkpoint jobs should not be chainable.
            jobDesc1 = createTestJobDesc(1, 2, 3, True, False)
            jobDesc2 = createTestJobDesc(1, 2, 3, True, True)
            getattr(jobDesc1, successorType)(jobDesc2.jobStoreID)
            self.assertEqual(None, nextChainable(jobDesc1, self.jobStore, self.config))

            # If there is no child we should get nothing to chain.
            jobDesc1 = createTestJobDesc(1, 2, 3, True, False)
            self.assertEqual(None, nextChainable(jobDesc1, self.jobStore, self.config))

            # If there are 2 or more children we should get nothing to chain.
            jobDesc1 = createTestJobDesc(1, 2, 3, True, False)
            jobDesc2 = createTestJobDesc(1, 2, 3, True, False)
            jobDesc3 = createTestJobDesc(1, 2, 3, True, False)
            getattr(jobDesc1, successorType)(jobDesc2.jobStoreID)
            getattr(jobDesc1, successorType)(jobDesc3.jobStoreID)
            self.assertEqual(None, nextChainable(jobDesc1, self.jobStore, self.config))

            # If there is an increase in resource requirements we should get nothing to chain.
            reqs = {'memory': 1, 'cores': 2, 'disk': 3, 'preemptible': True, 'checkpoint': False}
            for increased_attribute in ('memory', 'cores', 'disk'):
                jobDesc1 = createTestJobDesc(**reqs)
                reqs[increased_attribute] += 1
                jobDesc2 = createTestJobDesc(**reqs)
                getattr(jobDesc1, successorType)(jobDesc2.jobStoreID)
                self.assertEqual(None, nextChainable(jobDesc1, self.jobStore, self.config))

            # A change in preemptability from True to False should be disallowed.
            jobDesc1 = createTestJobDesc(1, 2, 3, True, False)
            jobDesc2 = createTestJobDesc(1, 2, 3, False, True)
            getattr(jobDesc1, successorType)(jobDesc2.jobStoreID)
            self.assertEqual(None, nextChainable(jobDesc1, self.jobStore, self.config))
