# Copyright (C) 2015-2024 Regents of the University of California
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
import logging
import os
import sys
import time

from argparse import Namespace
from threading import Thread
from typing import Optional

from toil.common import Toil
from toil.job import Job
from toil.test import ToilTest, slow

log = logging.getLogger(__name__)
logging.basicConfig()

class EnvironmentTest(ToilTest):
    """
    Test to make sure that Toil's environment variable save and restore system
    (environment.pickle) works.

    The environment should be captured once at the start of the workflow and
    should be sent through based on that, not base don the leader's current
    environment when the job is launched.
    """
    
    def test_environment(self):
        options = Job.Runner.getDefaultOptions(self._getTestJobStorePath())
        options.logLevel = "DEBUG"
        options.retryCount = 0

        main(options)
        
    

        
def check_environment(job):
    """
    Fail if the test environment is wrong.
    """

    if os.environ["MAGIC_ENV_VAR_123"] != "Value1":
        raise RuntimeError("Environment variable is wrong!")

def wait_a_bit(job):
    """
    Toil job that waits.
    """
    time.sleep(10)

def check_environment_repeatedly(job):
    """
    Toil job that checks the environment, waits, and checks it again, as
    separate invocations.
    """

    check1 = job.addChildJobFn(check_environment)
    waiter = check1.addFollowOnJobFn(wait_a_bit)
    check2 = waiter.addFollowOnJobFn(check_environment)
    # Add another one to make sure we don't chain
    check3 = waiter.addFollowOnJobFn(check_environment)

def main(options: Optional[Namespace] = None):
    """
    Run the actual workflow with the given options.
    """
    if not options:
        # deal with command line arguments
        parser = Job.Runner.getDefaultArgumentParser()
        options = parser.parse_args()

    # Set something that should be seen by Toil jobs
    os.environ["MAGIC_ENV_VAR_123"] = "Value1"

    # Once the workflow has started, change the environment
    def change_environment_later():
        """
        After waiting, modify the environment.
        """
        time.sleep(5)
        os.environ["MAGIC_ENV_VAR_123"] = "Value2"
    changer_thread = Thread(target=change_environment_later)
    changer_thread.start()

    with Toil(options) as toil:
        toil.start(Job.wrapJobFn(check_environment_repeatedly))

if __name__ == "__main__":
    main()
