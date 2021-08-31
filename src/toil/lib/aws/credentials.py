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
"""
Acts as the one place to get AWS credentials.

Each of session, resource, and client should all cache the same session if within the same region.

AWS recommends a new Session object for each thread to be thread-safe, so there are thread-safe versions as well:
https://boto3.amazonaws.com/v1/documentation/api/latest/guide/session.html#multithreading-or-multiprocessing-with-sessions
"""
import os

from functools import lru_cache
from typing import Optional
from boto3 import Session
from botocore.credentials import JSONFileCache
from botocore.session import get_session


@lru_cache(maxsize=None)
def session(region_name: Optional[str] = None) -> Session:
    """Quickly retrieve and use cached sessions.  The Session object itself is cached here, not just credentials."""
    return thread_safe_session(region_name)


@lru_cache(maxsize=None)
def client(*args, thread_safe=False, **kwargs):
    """Quickly retrieve and use cached clients."""
    if thread_safe:
        boto3_session = thread_safe_session(region_name=kwargs.get('region_name'))
    else:
        boto3_session = session(region_name=kwargs.get('region_name'))
    if 's3' == args[0]:
        s3_boto_args = boto_args()
        if 'endpoint_url' in s3_boto_args:
            kwargs['endpoint_url'] = s3_boto_args['endpoint_url']
        return boto3_session.client(*args, **kwargs)
    else:
        return boto3_session.client(*args, **kwargs)


@lru_cache(maxsize=None)
def resource(*args, thread_safe=False, **kwargs):
    """Quickly retrieve and use cached resources."""
    if thread_safe:
        boto3_session = thread_safe_session(region_name=kwargs.get('region_name'))
    else:
        boto3_session = session(region_name=kwargs.get('region_name'))
    if 's3' == args[0]:
        s3_boto_args = boto_args()
        if 'endpoint_url' in s3_boto_args:
            kwargs['endpoint_url'] = s3_boto_args['endpoint_url']
        return boto3_session.resource(*args, **kwargs)
    else:
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


def boto_args():
    host = os.environ.get('TOIL_S3_HOST', None)
    port = os.environ.get('TOIL_S3_PORT', None)
    protocol = 'https'
    if os.environ.get('TOIL_S3_USE_SSL', True) == 'False':
        protocol = 'http'
    if host:
        return {'endpoint_url': f'{protocol}://{host}' + f':{port}' if port else ''}
    return {}
