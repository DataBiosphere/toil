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
from functools import lru_cache
from typing import Optional
from boto3 import Session
from botocore.credentials import JSONFileCache
from botocore.session import get_session


@lru_cache(maxsize=None)
def session(region_name: Optional[str] = None) -> Session:
    """Use this to quickly retrieve and use cached sessions, unless you're going to be spinning out on multiple threads."""
    return thread_safe_session(region_name)


@lru_cache(maxsize=None)
def client(*args, **kwargs):
    """Use this to quickly retrieve and use cached clients, unless you're going to be spinning out on multiple threads."""
    boto3_session = session(region_name=kwargs.get('region_name'))
    return boto3_session.client(*args, **kwargs)


@lru_cache(maxsize=None)
def resource(*args, **kwargs):
    """Use this to quickly retrieve and use cached resources, unless you're going to be spinning out on multiple threads."""
    boto3_session = session(region_name=kwargs.get('region_name'))
    return boto3_session.resource(*args, **kwargs)


def thread_safe_session(region_name: Optional[str] = None) -> Session:
    """
    This is the One True Place where Boto3 sessions should be established, and
    prepared with the necessary credential caching.

    New Session objects should be created for each thread to be thread-safe:
        https://boto3.amazonaws.com/v1/documentation/api/latest/guide/session.html#multithreading-or-multiprocessing-with-sessions

    :param region_name: If given, the session will be associated with the given AWS region.
    """
    # Make sure to use credential caching when talking to Amazon via boto3
    # See https://github.com/boto/botocore/pull/1338/
    # And https://github.com/boto/botocore/commit/2dae76f52ae63db3304b5933730ea5efaaaf2bfc
    botocore_session = get_session()
    botocore_session.get_component('credential_provider').get_provider('assume-role').cache = JSONFileCache()
    return Session(botocore_session=botocore_session, region_name=region_name)


def thread_safe_client(*args, **kwargs):
    boto3_session = thread_safe_session(region_name=kwargs.get('region_name'))
    return boto3_session.client(*args, **kwargs)


def thread_safe_resource(*args, **kwargs):
    boto3_session = thread_safe_session(region_name=kwargs.get('region_name'))
    return boto3_session.resource(*args, **kwargs)
