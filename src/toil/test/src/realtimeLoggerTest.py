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
from toil.job import Job
from toil.test import ToilTest
import logging
from toil.realtimeLogger import RealtimeLogger


class RealtimeLoggerTest(ToilTest):
    def testRealtimeLogger(self):
        options = Job.Runner.getDefaultOptions(self._getTestJobStorePath())
        options.realTimeLogging = True
        options.logLevel = 'INFO'

        detector = MessageDetector()

        # Set up a log message detector to the root logger
        logging.getLogger().addHandler(detector)

        Job.Runner.startToil(LogTest(), options)

        # We need the message we're supposed to see
        self.assertTrue(detector.detected)
        # But not the message that shouldn't be logged.
        self.assertFalse(detector.overLogged)


class MessageDetector(logging.StreamHandler):
    """
    Detect the secret message and set a flag.
    """

    def __init__(self):
        self.detected = False  # Have we seen the message we want?
        self.overLogged = False  # Have we seen the message we don't want?
        super(MessageDetector, self).__init__()

    def emit(self, record):
        if record.msg == 'This should be logged at info level':
            self.detected = True
        if record.msg == 'This should be logged at debug level':
            self.overLogged = True


class LogTest(Job):
    def __init__(self):
        Job.__init__(self, memory=100000, cores=2, disk='3G')

    def run(self, fileStore):
        RealtimeLogger.info('This should be logged at info level')
        RealtimeLogger.debug('This should be logged at debug level')
