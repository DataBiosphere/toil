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

# Python 3 compatibility imports
from __future__ import absolute_import

import base64
import json


class UnknownVersion(Exception):
    def __init__(self, version):
        super(UnknownVersion, self).__init__("Unknown message version %d" % version)
        self.version = version


class Message(object):
    """
    A message, mostly for passing information about events to agents. The message version is used
    to differentiate between incompatible message formats. For example, adding a field is a
    compatible change if there is a default value for that field, and does not require
    incrementing the version. Message consumers should ignore versions they don't understand.
    """

    TYPE_UPDATE_SSH_KEYS = 1

    @classmethod
    def from_sqs(cls, sqs_message):
        """
        :param sqs_message: the SQS message to initializes this instance from, assuiming that the
        SQS message originates from a SQS queue that is subscribed to an SNS topic :type
        sqs_message: SQSMessage

        :return: the parsed message or None if the message is of an unkwown version
        :rtype: Message
        """
        sns_message = json.loads(sqs_message.get_body())
        return Message.from_sns(sns_message['Message'])

    @classmethod
    def from_sns(cls, message):
        return cls.from_dict(json.loads(base64.standard_b64decode(message)))

    @classmethod
    def from_dict(cls, message):
        version = message['version']
        if version == 1:
            return cls(type=message['type'])
        else:
            raise UnknownVersion(version)

    def __init__(self, type):
        super(Message, self).__init__()
        self.type = type

    def to_dict(self):
        return dict(version=1, type=self.type)

    def to_sns(self):
        return base64.standard_b64encode(json.dumps(self.to_dict()))
