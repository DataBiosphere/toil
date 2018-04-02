# Copyright (C) 2015-2018 Regents of the University of California
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
import pickle

from toil.common import Config
from toil.job import Job
from toil.jobGraph import JobGraph
from toil.jobStores.fileJobStore import FileJobStore
from toil.test import ToilTest
from toil.worker import nextChainableJobGraph

class WorkerTests(ToilTest):
    """Test miscellaneous units of the worker."""
    def setUp(self):
        super(WorkerTests, self).setUp()
        path = self._getTestJobStorePath()
        self.jobStore = FileJobStore(path)
        self.config = Config()
        self.config.jobStore = 'file:%s' % path
        self.jobStore.initialize(self.config)
        self.jobGraphNumber = 0

    def testNextChainableJobGraph(self):
        """Make sure chainable/non-chainable jobs are identified correctly."""
        def createJobGraph(memory, cores, disk, preemptable, checkpoint):
            """Create a fake-ish Job and JobGraph pair, and return the
            jobGraph."""
            name = 'jobGraph%d' % self.jobGraphNumber
            self.jobGraphNumber += 1

            job = Job()
            job.checkpoint = checkpoint
            with self.jobStore.writeFileStream() as (f, fileStoreID):
                pickle.dump(job, f, pickle.HIGHEST_PROTOCOL)
            command = '_toil %s fooCommand toil True' % fileStoreID
            jobGraph = JobGraph(command=command, memory=memory, cores=cores,
                                disk=disk, unitName=name,
                                jobName=name, preemptable=preemptable,
                                jobStoreID=name, remainingRetryCount=1,
                                predecessorNumber=1)
            return self.jobStore.create(jobGraph)

        # Identical non-checkpoint jobs should be chainable.
        jobGraph1 = createJobGraph(1, 2, 3, True, False)
        jobGraph2 = createJobGraph(1, 2, 3, True, False)
        jobGraph1.stack = [[jobGraph2]]
        self.assertEquals(jobGraph2, nextChainableJobGraph(jobGraph1, self.jobStore))

        # Identical checkpoint jobs should not be chainable.
        jobGraph1 = createJobGraph(1, 2, 3, True, False)
        jobGraph2 = createJobGraph(1, 2, 3, True, True)
        jobGraph1.stack = [[jobGraph2]]
        self.assertEquals(None, nextChainableJobGraph(jobGraph1, self.jobStore))

        # If there is no child we should get nothing to chain.
        jobGraph1 = createJobGraph(1, 2, 3, True, False)
        jobGraph1.stack = []
        self.assertEquals(None, nextChainableJobGraph(jobGraph1, self.jobStore))

        # If there are 2 or more children we should get nothing to chain.
        jobGraph1 = createJobGraph(1, 2, 3, True, False)
        jobGraph2 = createJobGraph(1, 2, 3, True, False)
        jobGraph3 = createJobGraph(1, 2, 3, True, False)
        jobGraph1.stack = [[jobGraph2, jobGraph3]]
        self.assertEquals(None, nextChainableJobGraph(jobGraph1, self.jobStore))

        # If there is an increase in resource requirements we should get nothing to chain.
        reqs = {'memory': 1, 'cores': 2, 'disk': 3, 'preemptable': True, 'checkpoint': False}
        for increased_attribute in ('memory', 'cores', 'disk'):
            jobGraph1 = createJobGraph(**reqs)
            reqs[increased_attribute] += 1
            jobGraph2 = createJobGraph(**reqs)
            jobGraph1.stack = [[jobGraph2]]
            self.assertEquals(None, nextChainableJobGraph(jobGraph1, self.jobStore))

        # A change in preemptability from True to False should be disallowed.
        jobGraph1 = createJobGraph(1, 2, 3, True, False)
        jobGraph2 = createJobGraph(1, 2, 3, False, True)
        jobGraph1.stack = [[jobGraph2]]
        self.assertEquals(None, nextChainableJobGraph(jobGraph1, self.jobStore))
