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
"""These are utility functions for working with AWS that do not rely on boto3."""
import re
import os
import socket
from urllib.error import URLError
from urllib.request import urlopen

CLOUD_KEY_REGEX = re.compile(
    "^"
    "(?P<schema>(?:s3|gs|wasb))"
    "://"
    "(?P<bucket>[^/]+)"
    "/"
    "(?P<key>.+)"
    "$")
AWS_ZONE_REGEX = re.compile(r'^([a-z]{2}-[a-z]+-[1-9][0-9]*)([a-z])$')


def running_on_ec2() -> bool:
    hv_uuid_path = '/sys/hypervisor/uuid'
    if os.path.exists(hv_uuid_path):
        with open(hv_uuid_path) as f:
            file_begins_with_ec2 = f.read(len('ec2')) == 'ec2'
    if file_begins_with_ec2:
        return True
    # Some instances do not have the /sys/hypervisor/uuid file, so check the identity document instead.
    # See https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/instance-identity-documents.html
    try:
        urlopen('http://169.254.169.254/latest/dynamic/instance-identity/document', timeout=1)
        return True
    except (URLError, socket.timeout):
        return False


def zone_to_region(zone: str) -> str:
    """Get a region (e.g. us-west-2) from a zone (e.g. us-west-1c)."""
    aws_zone = AWS_ZONE_REGEX.match(zone)
    if not aws_zone:
        raise ValueError(f"Can't extract region from availability zone '{zone}'")
    return aws_zone.group(1)
