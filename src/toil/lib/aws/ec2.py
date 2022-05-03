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
"""AWS functions dealing with ec2."""
import os
import socket
from urllib.error import URLError
from urllib.request import urlopen


def running_on_ec2() -> bool:
    """
    Return True if we are currently running on EC2, and false otherwise.
    """
    # TODO: Move this to toil.lib.ec2 and make toil.lib.ec2 importable without boto?
    def file_begins_with(path, prefix):
        with open(path) as f:
            return f.read(len(prefix)) == prefix

    hv_uuid_path = '/sys/hypervisor/uuid'
    if os.path.exists(hv_uuid_path) and file_begins_with(hv_uuid_path, 'ec2'):
        return True
    # Some instances do not have the /sys/hypervisor/uuid file, so check the identity document instead.
    # See https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/instance-identity-documents.html
    try:
        urlopen('http://169.254.169.254/latest/dynamic/instance-identity/document', timeout=1)
        return True
    except (URLError, socket.timeout):
        return False
