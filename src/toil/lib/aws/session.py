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
import logging
import os
import threading
from typing import TYPE_CHECKING, Literal, Optional, cast, overload

import boto3
import boto3.resources.base
import botocore
from boto3 import Session
from botocore.client import Config
from botocore.session import get_session
from botocore.utils import JSONFileCache

if TYPE_CHECKING:
    from mypy_boto3_autoscaling import AutoScalingClient
    from mypy_boto3_ec2 import EC2Client, EC2ServiceResource
    from mypy_boto3_iam import IAMClient, IAMServiceResource
    from mypy_boto3_s3 import S3Client, S3ServiceResource
    from mypy_boto3_sdb import SimpleDBClient
    from mypy_boto3_sts import STSClient

logger = logging.getLogger(__name__)

# A note on thread safety:
#
# Boto3 Session: Not thread safe, 1 per thread is required.
#
# Boto3 Resources: Not thread safe, one per thread is required.
#
# Boto3 Client: Thread safe after initialization, but initialization is *not*
# thread safe and only one can be being made at a time. They also are
# restricted to a single Python *process*.
#
# See: <https://stackoverflow.com/questions/52820971/is-boto3-client-thread-safe>

# We use this lock to control initialization so only one thread can be
# initializing Boto3 (or Boto2) things at a time.
_init_lock = threading.RLock()


def _new_boto3_session(region_name: Optional[str] = None) -> Session:
    """
    This is the One True Place where new Boto3 sessions should be made, and
    prepares them with the necessary credential caching. Does *not* cache
    sessions, because each thread needs its own caching.

    :param region_name: If given, the session will be associated with the given AWS region.
    """

    # Make sure to use credential caching when talking to Amazon via boto3
    # See https://github.com/boto/botocore/pull/1338/
    # And https://github.com/boto/botocore/commit/2dae76f52ae63db3304b5933730ea5efaaaf2bfc

    with _init_lock:
        botocore_session = get_session()
        botocore_session.get_component("credential_provider").get_provider(
            "assume-role"
        ).cache = JSONFileCache()

        return Session(
            botocore_session=botocore_session,
            region_name=region_name,
            profile_name=os.environ.get("TOIL_AWS_PROFILE", None),
        )


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

    We also support None for a region, in which case no region will be
    passed to Boto/Boto3. The caller is responsible for implementing e.g.
    TOIL_AWS_REGION support.

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
        self.sessions_by_region: dict[Optional[str], threading.local] = (
            collections.defaultdict(threading.local)
        )
        # This stores Boto3 resources in .item of a thread-local storage, by
        # (region, service name, endpoint URL) tuples
        self.resource_cache: dict[
            tuple[Optional[str], str, Optional[str]], threading.local
        ] = collections.defaultdict(threading.local)
        # This stores Boto3 clients in .item of a thread-local storage, by
        # (region, service name, endpoint URL) tuples
        self.client_cache: dict[
            tuple[Optional[str], str, Optional[str]], threading.local
        ] = collections.defaultdict(threading.local)
        # This stores Boto 2 connections in .item of a thread-local storage, by
        # (region, service name) tuples.
        self.boto2_cache: dict[tuple[Optional[str], str], threading.local] = (
            collections.defaultdict(threading.local)
        )

    def session(self, region: Optional[str]) -> boto3.session.Session:
        """
        Get the Boto3 Session to use for the given region.
        """
        storage = self.sessions_by_region[region]
        if not hasattr(storage, "item"):
            # This is the first time this thread wants to talk to this region
            # through this manager
            storage.item = _new_boto3_session(region_name=region)
        return cast(boto3.session.Session, storage.item)

    @overload
    def resource(
        self,
        region: Optional[str],
        service_name: Literal["s3"],
        endpoint_url: Optional[str] = None,
    ) -> "S3ServiceResource": ...
    @overload
    def resource(
        self,
        region: Optional[str],
        service_name: Literal["iam"],
        endpoint_url: Optional[str] = None,
    ) -> "IAMServiceResource": ...
    @overload
    def resource(
        self,
        region: Optional[str],
        service_name: Literal["ec2"],
        endpoint_url: Optional[str] = None,
    ) -> "EC2ServiceResource": ...

    def resource(
        self,
        region: Optional[str],
        service_name: str,
        endpoint_url: Optional[str] = None,
    ) -> boto3.resources.base.ServiceResource:
        """
        Get the Boto3 Resource to use with the given service (like 'ec2') in the given region.

        :param endpoint_url: AWS endpoint URL to use for the client. If not
               specified, a default is used.
        """
        key = (region, service_name, endpoint_url)
        storage = self.resource_cache[key]
        if not hasattr(storage, "item"):
            with _init_lock:
                # We lock inside the if check; we don't care if the memoization
                # sometimes results in multiple different copies leaking out.
                # We lock because we call .resource()

                if endpoint_url is not None:
                    # The Boto3 stubs are missing an overload for `resource` that takes
                    # a non-literal string. See
                    # <https://github.com/vemel/mypy_boto3_builder/issues/121#issuecomment-1011322636>
                    storage.item = self.session(region).resource(service_name, endpoint_url=endpoint_url)  # type: ignore
                else:
                    # We might not be able to pass None to Boto3 and have it be the same as no argument.
                    storage.item = self.session(region).resource(service_name)  # type: ignore

        return cast(boto3.resources.base.ServiceResource, storage.item)

    @overload
    def client(
        self,
        region: Optional[str],
        service_name: Literal["ec2"],
        endpoint_url: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> "EC2Client": ...
    @overload
    def client(
        self,
        region: Optional[str],
        service_name: Literal["iam"],
        endpoint_url: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> "IAMClient": ...
    @overload
    def client(
        self,
        region: Optional[str],
        service_name: Literal["s3"],
        endpoint_url: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> "S3Client": ...
    @overload
    def client(
        self,
        region: Optional[str],
        service_name: Literal["sts"],
        endpoint_url: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> "STSClient": ...
    @overload
    def client(
        self,
        region: Optional[str],
        service_name: Literal["sdb"],
        endpoint_url: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> "SimpleDBClient": ...
    @overload
    def client(
        self,
        region: Optional[str],
        service_name: Literal["autoscaling"],
        endpoint_url: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> "AutoScalingClient": ...

    def client(
        self,
        region: Optional[str],
        service_name: Literal["ec2", "iam", "s3", "sts", "sdb", "autoscaling"],
        endpoint_url: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> botocore.client.BaseClient:
        """
        Get the Boto3 Client to use with the given service (like 'ec2') in the given region.

        :param endpoint_url: AWS endpoint URL to use for the client. If not
               specified, a default is used.
        :param config: Custom configuration to use for the client.
        """

        if config is not None:
            # Don't try and memoize if a custom config is used
            with _init_lock:
                if endpoint_url is not None:
                    return self.session(region).client(
                        service_name, endpoint_url=endpoint_url, config=config
                    )
                else:
                    return self.session(region).client(service_name, config=config)

        key = (region, service_name, endpoint_url)
        storage = self.client_cache[key]
        if not hasattr(storage, "item"):
            with _init_lock:
                # We lock because we call .client()

                if endpoint_url is not None:
                    # The Boto3 stubs are probably missing an overload here too. See:
                    # <https://github.com/vemel/mypy_boto3_builder/issues/121#issuecomment-1011322636>
                    storage.item = self.session(region).client(
                        service_name, endpoint_url=endpoint_url
                    )
                else:
                    # We might not be able to pass None to Boto3 and have it be the same as no argument.
                    storage.item = self.session(region).client(service_name)
        return cast(botocore.client.BaseClient, storage.item)


# If you don't want your own AWSConnectionManager, we have a global one and some global functions
_global_manager = AWSConnectionManager()


def establish_boto3_session(region_name: Optional[str] = None) -> Session:
    """
    Get a Boto 3 session usable by the current thread.

    This function may not always establish a *new* session; it can be memoized.
    """

    # Just use a global version of the manager. Note that we change the argument order!
    return _global_manager.session(region_name)


@overload
def client(
    service_name: Literal["ec2"],
    region_name: Optional[str] = None,
    endpoint_url: Optional[str] = None,
    config: Optional[Config] = None,
) -> "EC2Client": ...
@overload
def client(
    service_name: Literal["iam"],
    region_name: Optional[str] = None,
    endpoint_url: Optional[str] = None,
    config: Optional[Config] = None,
) -> "IAMClient": ...
@overload
def client(
    service_name: Literal["s3"],
    region_name: Optional[str] = None,
    endpoint_url: Optional[str] = None,
    config: Optional[Config] = None,
) -> "S3Client": ...
@overload
def client(
    service_name: Literal["sts"],
    region_name: Optional[str] = None,
    endpoint_url: Optional[str] = None,
    config: Optional[Config] = None,
) -> "STSClient": ...
@overload
def client(
    service_name: Literal["sdb"],
    region_name: Optional[str] = None,
    endpoint_url: Optional[str] = None,
    config: Optional[Config] = None,
) -> "SimpleDBClient": ...
@overload
def client(
    service_name: Literal["autoscaling"],
    region_name: Optional[str] = None,
    endpoint_url: Optional[str] = None,
    config: Optional[Config] = None,
) -> "AutoScalingClient": ...


def client(
    service_name: Literal["ec2", "iam", "s3", "sts", "sdb", "autoscaling"],
    region_name: Optional[str] = None,
    endpoint_url: Optional[str] = None,
    config: Optional[Config] = None,
) -> botocore.client.BaseClient:
    """
    Get a Boto 3 client for a particular AWS service, usable by the current thread.

    Global alternative to AWSConnectionManager.
    """

    # Just use a global version of the manager. Note that we change the argument order!
    return _global_manager.client(
        region_name, service_name, endpoint_url=endpoint_url, config=config
    )


@overload
def resource(
    service_name: Literal["s3"],
    region_name: Optional[str] = None,
    endpoint_url: Optional[str] = None,
) -> "S3ServiceResource": ...
@overload
def resource(
    service_name: Literal["iam"],
    region_name: Optional[str] = None,
    endpoint_url: Optional[str] = None,
) -> "IAMServiceResource": ...
@overload
def resource(
    service_name: Literal["ec2"],
    region_name: Optional[str] = None,
    endpoint_url: Optional[str] = None,
) -> "EC2ServiceResource": ...


def resource(
    service_name: Literal["s3", "iam", "ec2"],
    region_name: Optional[str] = None,
    endpoint_url: Optional[str] = None,
) -> boto3.resources.base.ServiceResource:
    """
    Get a Boto 3 resource for a particular AWS service, usable by the current thread.

    Global alternative to AWSConnectionManager.
    """

    # Just use a global version of the manager. Note that we change the argument order!
    return _global_manager.resource(
        region_name, service_name, endpoint_url=endpoint_url
    )
