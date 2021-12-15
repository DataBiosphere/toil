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
import collections
import inspect
import logging
import os
import re
import socket
import threading
from functools import lru_cache
from urllib.request import urlopen
from urllib.error import URLError

from typing import Any, Callable, Dict, Iterable, List, Optional, TypeVar, Union

logger = logging.getLogger(__name__)

try:
    # This file contains some stuff that needs boto/boto3 to be installed in
    # order to be defined. But this __init__ file and files under it also
    # contain some stuff that we need to be able to import when boto isn't
    # installed. So everything that does need those modules gets defined in
    # this try.

    import boto3
    import boto3.resources.base
    from boto3 import Session
    import botocore
    from botocore.credentials import JSONFileCache
    from botocore.session import get_session
    import boto.connection

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
        return Session(botocore_session=botocore_session, region_name=region_name)

    @lru_cache(maxsize=None)
    def client(service_name: str, *args, region_name: Optional[str] = None, **kwargs):
        """
        Get a Boto 3 client for a particular AWS service.

        Global alternative to AWSConnectionManager.
        """
        session = establish_boto3_session(region_name=region_name)
        return session.client(service_name, *args, **kwargs)

    @lru_cache(maxsize=None)
    def resource(service_name: str, *args, region_name: Optional[str] = None, **kwargs):
        """
        Get a Boto 3 resource for a particular AWS service.

        Global alternative to AWSConnectionManager.
        """
        session = establish_boto3_session(region_name=region_name)
        return session.resource(service_name, *args, **kwargs)

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

        def __init__(self):
            """
            Make a new empty AWSConnectionManager.
            """
            # This stores Boto3 sessions in .item of a thread-local storage, by
            # region.
            self.sessions_by_region = collections.defaultdict(threading.local)
            # This stores Boto3 resources in .item of a thread-local storage, by
            # (region, service name) tuples
            self.resource_cache = collections.defaultdict(threading.local)
            # This stores Boto3 clients in .item of a thread-local storage, by
            # (region, service name) tuples
            self.client_cache = collections.defaultdict(threading.local)
            # This stores Boto 2 connections in .item of a thread-local storage, by
            # (region, service name) tuples.
            self.boto2_cache = collections.defaultdict(threading.local)

        def session(self, region: str) -> boto3.session.Session:
            """
            Get the Boto3 Session to use for the given region.
            """
            storage = self.sessions_by_region[region]
            if not hasattr(storage, 'item'):
                # This is the first time this thread wants to talk to this region
                # through this manager
                storage.item = establish_boto3_session(region_name=region)
            return storage.item

        def resource(self, region: str, service_name: str) -> boto3.resources.base.ServiceResource:
            """
            Get the Boto3 Resource to use with the given service (like 'ec2') in the given region.
            """
            key = (region, service_name)
            storage = self.resource_cache[key]
            if not hasattr(storage, 'item'):
                storage.item = self.session(region).resource(service_name)
            return storage.item

        def client(self, region: str, service_name: str) -> botocore.client.BaseClient:
            """
            Get the Boto3 Client to use with the given service (like 'ec2') in the given region.
            """
            key = (region, service_name)
            storage = self.client_cache[key]
            if not hasattr(storage, 'item'):
                storage.item = self.session(region).client(service_name)
            return storage.item

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
                storage.item = getattr(boto, service_name).connect_to_region(region)
            return storage.item
except ImportError:
    # Boto modules aren't available, so don't define this stuff that needs them.
    pass

def get_current_aws_region() -> Optional[str]:
    """
    Return the AWS region that the currently configured AWS zone (see
    get_current_aws_zone()) is in.
    """
    aws_zone = get_current_aws_zone()
    return zone_to_region(aws_zone) if aws_zone else None

ResultType = TypeVar('ResultType')
def try_providers(functions: List[Callable[..., Optional[ResultType]]], kwargs: Dict[str, Any] = {}) -> Optional[ResultType]:
    """
    Call the given list of functions in order, passing any of the given kwargs
    that each function can accept. If any function returns a non-None answer,
    stop and return that answer. Otherwise, return None.
    """

    for callback in functions:
        # Call all the passed functions in order

        # We need to work out what arguments are accepted and out them all in here
        call_args: Dict[str, Any] = {}
        sig = inspect.signature(callback)
        for param_name, param in sig.parameters:
            # For each accepted parameter
            if param.kind in [inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.VAR_POSITIONAL]:
                # These can't be kwargs, so ignore them.
                pass
            elif param.kind == inspect.Parameter.VAR_KEYWORD:
                # Function can take any kwarg, so dump them all in.
                call_args.update(kwargs)
                break
            elif param_name in kwargs:
                # This is just one parameter that can be a kwarg, and we have a value for it
                call_args[param_name] = kwargs[param_name]

        # Now pass the subset of kwargs along to the function
        result = callback(**kwargs)
        if result is not None:
            # And return the first non-None result
            return result
    # Otherwise return None
    return None

def get_aws_zone_from_environment() -> Optional[str]:
    """
    Get the AWS zone from TOIL_AWS_ZONE if set.
    """
    return os.environ.get('TOIL_AWS_ZONE', None)

def get_aws_zone_from_metadata() -> Optional[str]:
    """
    Get the AWS zone from instance metadata, if on EC2 and the boto module is
    available.
    """
    if running_on_ec2():
        try:
            import boto
            from boto.utils import get_instance_metadata
            return get_instance_metadata()['placement']['availability-zone']
        except (KeyError, ImportError):
            pass
    return None

def get_aws_zone_from_boto() -> Optional[str]:
    """
    Get the AWS zone from the Boto config file, if it is configured and the
    boto module is avbailable.
    """
    try:
        import boto
        zone = boto.config.get('Boto', 'ec2_region_name')
        if zone is not None:
            zone += 'a'  # derive an availability zone in the region
        return zone
    except ImportError:
        pass
    return None


def get_current_aws_zone() -> Optional[str]:
    """
    Get the currently configured or occupied AWS zone to use.

    Reports the TOIL_AWS_ZONE environment variable if set.

    Otherwise, if we have boto and are running on EC2, reports the zone we are
    running in.

    Finally, if we have boto2, and a default region is configured in Boto 2,
    chooses a zone in that region.

    Returns None if no method can produce a zone to use.
    """
    return try_providers([get_aws_zone_from_environment,
                          get_aws_zone_from_metadata,
                          get_aws_zone_from_boto])

def zone_to_region(zone: str) -> str:
    """Get a region (e.g. us-west-2) from a zone (e.g. us-west-1c)."""
    # re.compile() caches the regex internally so we don't have to
    availability_zone = re.compile(r'^([a-z]{2}-[a-z]+-[1-9][0-9]*)([a-z])$')
    m = availability_zone.match(zone)
    if not m:
        raise ValueError(f"Can't extract region from availability zone '{zone}'")
    return m.group(1)

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
