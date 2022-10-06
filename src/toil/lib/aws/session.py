# Copyright (C) 2015-2022 Regents of the University of California
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
import collections
import inspect
import logging
import os
import re
import socket
import threading
from functools import lru_cache
from typing import (Any,
                    Callable,
                    Dict,
                    Iterable,
                    List,
                    Optional,
                    Tuple,
                    TypeVar,
                    Union,
                    cast)
from urllib.error import URLError
from urllib.request import urlopen

import boto3
import boto3.resources.base
import boto.connection
import botocore
from boto3 import Session
from botocore.credentials import JSONFileCache
from botocore.session import get_session

logger = logging.getLogger(__name__)

@lru_cache(maxsize=None)
def establish_boto3_session(region_name: Optional[str] = None) -> Session:
    """
    This is the One True Place where Boto3 sessions should be established, and
    prepares them with the necessary credential caching.

    :param region_name: If given, the session will be associated with the given AWS region.
    """

    # Make sure to use credential caching when talking to Amazon via boto3
    # See https://github.com/boto/botocore/pull/1338/
    # And https://github.com/boto/botocore/commit/2dae76f52ae63db3304b5933730ea5efaaaf2bfc

    botocore_session = get_session()
    botocore_session.get_component('credential_provider').get_provider(
        'assume-role').cache = JSONFileCache()

    return Session(botocore_session=botocore_session, region_name=region_name, profile_name=os.environ.get("TOIL_AWS_PROFILE", None))

@lru_cache(maxsize=None)
def client(service_name: str, *args: List[Any], region_name: Optional[str] = None, **kwargs: Dict[str, Any]) -> botocore.client.BaseClient:
    """
    Get a Boto 3 client for a particular AWS service.

    Global alternative to AWSConnectionManager.
    """
    session = establish_boto3_session(region_name=region_name)
    # MyPy can't understand our argument unpacking. See <https://github.com/vemel/mypy_boto3_builder/issues/121>
    client: botocore.client.BaseClient = session.client(service_name, *args, **kwargs) # type: ignore
    return client

@lru_cache(maxsize=None)
def resource(service_name: str, *args: List[Any], region_name: Optional[str] = None, **kwargs: Dict[str, Any]) -> boto3.resources.base.ServiceResource:
    """
    Get a Boto 3 resource for a particular AWS service.

    Global alternative to AWSConnectionManager.
    """
    session = establish_boto3_session(region_name=region_name)
    # MyPy can't understand our argument unpacking. See <https://github.com/vemel/mypy_boto3_builder/issues/121>
    resource: boto3.resources.base.ServiceResource = session.resource(service_name, *args, **kwargs) # type: ignore
    return resource

class AWSConnectionManager:
    """
    Class that represents a connection to AWS. Caches Boto 3 and Boto 2 objects
    by region.

    Access to any kind of item goes through the particular method for the thing
    you want (session, resource, service, Boto2 Context), and then you pass the
    region you want to work in, and possibly the type of thing you want, as arguments.

    This class is intended to eventually enable multi-region clusters, where
    connections to multiple regions may need to be managed in the same
    provisioner.

    Since connection objects may not be thread safe (see
    <https://boto3.amazonaws.com/v1/documentation/api/1.14.31/guide/session.html#multithreading-or-multiprocessing-with-sessions>),
    one is created for each thread that calls the relevant lookup method.
    """

    # TODO: mypy is going to have !!FUN!! with this API because the final type
    # we get out (and whether it has the right methods for where we want to use
    # it) depends on having the right string value for the service. We could
    # also individually wrap every service we use, but that seems like a good
    # way to generate a lot of boring code.

    def __init__(self) -> None:
        """
        Make a new empty AWSConnectionManager.
        """
        # This stores Boto3 sessions in .item of a thread-local storage, by
        # region.
        self.sessions_by_region: Dict[str, threading.local] = collections.defaultdict(threading.local)
        # This stores Boto3 resources in .item of a thread-local storage, by
        # (region, service name) tuples
        self.resource_cache: Dict[Tuple[str, str], threading.local] = collections.defaultdict(threading.local)
        # This stores Boto3 clients in .item of a thread-local storage, by
        # (region, service name) tuples
        self.client_cache: Dict[Tuple[str, str], threading.local] = collections.defaultdict(threading.local)
        # This stores Boto 2 connections in .item of a thread-local storage, by
        # (region, service name) tuples.
        self.boto2_cache: Dict[Tuple[str, str], threading.local] = collections.defaultdict(threading.local)

    def session(self, region: str) -> boto3.session.Session:
        """
        Get the Boto3 Session to use for the given region.
        """
        storage = self.sessions_by_region[region]
        if not hasattr(storage, 'item'):
            # This is the first time this thread wants to talk to this region
            # through this manager
            storage.item = establish_boto3_session(region_name=region)
        return cast(boto3.session.Session, storage.item)

    def resource(self, region: str, service_name: str) -> boto3.resources.base.ServiceResource:
        """
        Get the Boto3 Resource to use with the given service (like 'ec2') in the given region.
        """
        key = (region, service_name)
        storage = self.resource_cache[key]
        if not hasattr(storage, 'item'):
            # The Boto3 stubs are missing an overload for `resource` that takes
            # a non-literal string. See
            # <https://github.com/vemel/mypy_boto3_builder/issues/121#issuecomment-1011322636>
            storage.item = self.session(region).resource(service_name) # type: ignore
        return cast(boto3.resources.base.ServiceResource, storage.item)

    def client(self, region: str, service_name: str) -> botocore.client.BaseClient:
        """
        Get the Boto3 Client to use with the given service (like 'ec2') in the given region.
        """
        key = (region, service_name)
        storage = self.client_cache[key]
        if not hasattr(storage, 'item'):
            # The Boto3 stubs are probably missing an overload here too. See:
            # <https://github.com/vemel/mypy_boto3_builder/issues/121#issuecomment-1011322636>
            storage.item = self.session(region).client(service_name) # type: ignore
        return cast(botocore.client.BaseClient , storage.item)

    def boto2(self, region: str, service_name: str) -> boto.connection.AWSAuthConnection:
        """
        Get the connected boto2 connection for the given region and service.
        """
        if service_name == 'iam':
            # IAM connections are regionless
            region = 'universal'
        key = (region, service_name)
        storage = self.boto2_cache[key]
        if not hasattr(storage, 'item'):
            storage.item = getattr(boto, service_name).connect_to_region(region, profile_name=os.environ.get("TOIL_AWS_PROFILE", None))
        return cast(boto.connection.AWSAuthConnection, storage.item)
