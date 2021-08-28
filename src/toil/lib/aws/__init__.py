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
"""Caches all boto3 clients we instantiate."""
import re

from functools import lru_cache


@lru_cache(maxsize=None)
def client(*args, **kwargs):
    import boto3
    return boto3.client(*args, **kwargs)


@lru_cache(maxsize=None)
def resource(*args, **kwargs):
    import boto3
    return boto3.resource(*args, **kwargs)


def zone_to_region(zone: str) -> str:
    """Get a region (e.g. us-west-2) from a zone (e.g. us-west-1c)."""
    # re.compile() caches the regex internally so we don't have to
    availability_zone = re.compile(r'^([a-z]{2}-[a-z]+-[1-9][0-9]*)([a-z])$')
    m = availability_zone.match(zone)
    if not m:
        raise ValueError(f"Can't extract region from availability zone '{zone}'")
    return m.group(1)
