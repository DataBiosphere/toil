# Copyright (C) 2015-2021 UCSC Computational Genomics Lab
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
from __future__ import annotations

import json
import logging
import os
import platform
import socket
import string
import textwrap
import time
import uuid
from collections.abc import Collection, Iterable
from functools import wraps
from shlex import quote
from typing import TYPE_CHECKING, Any, Callable, TypeVar, cast

# We need these to exist as attributes we can get off of the boto object
from botocore.exceptions import ClientError

from toil.lib.aws import AWSRegionName, AWSServerErrors, zone_to_region
from toil.lib.aws.ami import get_flatcar_ami
from toil.lib.aws.iam import (
    CLUSTER_LAUNCHING_PERMISSIONS,
    create_iam_role,
    get_policy_permissions,
    policy_permissions_allow,
)
from toil.lib.aws.session import AWSConnectionManager
from toil.lib.aws.session import client as get_client
from toil.lib.aws.utils import boto3_pager, create_s3_bucket
from toil.lib.conversions import human2bytes
from toil.lib.ec2 import (
    a_short_time,
    create_auto_scaling_group,
    create_instances,
    create_launch_template,
    create_ondemand_instances,
    create_spot_instances,
    increase_instance_hop_limit,
    wait_instances_running,
    wait_transition,
    wait_until_instance_profile_arn_exists,
)
from toil.lib.ec2nodes import InstanceType
from toil.lib.generatedEC2Lists import E2Instances
from toil.lib.memoize import memoize
from toil.lib.misc import truncExpBackoff
from toil.lib.retry import (
    get_error_body,
    get_error_code,
    get_error_message,
    get_error_status,
    old_retry,
    retry,
)
from toil.provisioners import (
    ClusterCombinationNotSupportedException,
    NoSuchClusterException,
    NoSuchZoneException,
)
from toil.provisioners.abstractProvisioner import (
    AbstractProvisioner,
    ManagedNodesNotSupportedException,
    Shape,
)
from toil.provisioners.aws import get_best_aws_zone
from toil.provisioners.node import Node

if TYPE_CHECKING:
    from mypy_boto3_autoscaling.client import AutoScalingClient
    from mypy_boto3_ec2.client import EC2Client
    from mypy_boto3_ec2.service_resource import Instance
    from mypy_boto3_ec2.type_defs import (
        BlockDeviceMappingTypeDef,
        CreateSecurityGroupResultTypeDef,
        DescribeInstancesResultTypeDef,
        EbsBlockDeviceTypeDef,
        FilterTypeDef,
        InstanceTypeDef,
        IpPermissionTypeDef,
        ReservationTypeDef,
        SecurityGroupTypeDef,
        SpotInstanceRequestTypeDef,
        TagDescriptionTypeDef,
        TagTypeDef,
    )
    from mypy_boto3_iam.client import IAMClient
    from mypy_boto3_iam.type_defs import InstanceProfileTypeDef, RoleTypeDef

logger = logging.getLogger(__name__)
logging.getLogger("boto").setLevel(logging.CRITICAL)
# Role name (used as the suffix) for EC2 instance profiles that are automatically created by Toil.
_INSTANCE_PROFILE_ROLE_NAME = "toil"
# The tag key that specifies the Toil node type ("leader" or "worker") so that
# leader vs. worker nodes can be robustly identified.
_TAG_KEY_TOIL_NODE_TYPE = "ToilNodeType"
# The tag that specifies the cluster name on all nodes
_TAG_KEY_TOIL_CLUSTER_NAME = "clusterName"
# How much storage on the root volume is expected to go to overhead and be
# unavailable to jobs when the node comes up?
# TODO: measure
_STORAGE_ROOT_OVERHEAD_GIGS = 4
# The maximum length of a S3 bucket
_S3_BUCKET_MAX_NAME_LEN = 63
# The suffix of the S3 bucket associated with the cluster
_S3_BUCKET_INTERNAL_SUFFIX = "--internal"


def awsRetryPredicate(e: Exception) -> bool:
    if isinstance(e, socket.gaierror):
        # Could be a DNS outage:
        # socket.gaierror: [Errno -2] Name or service not known
        return True
    # boto/AWS gives multiple messages for the same error...
    if get_error_status(e) == 503 and "Request limit exceeded" in get_error_body(e):
        return True
    elif get_error_status(e) == 400 and "Rate exceeded" in get_error_body(e):
        return True
    elif get_error_status(e) == 400 and "NotFound" in get_error_body(e):
        # EC2 can take a while to propagate instance IDs to all servers.
        return True
    elif get_error_status(e) == 400 and get_error_code(e) == "Throttling":
        return True
    return False


def expectedShutdownErrors(e: Exception) -> bool:
    """
    Matches errors that we expect to occur during shutdown, and which indicate
    that we need to wait or try again.

    Should *not* match any errors which indicate that an operation is
    impossible or unnecessary (such as errors resulting from a thing not
    existing to be deleted).
    """
    return get_error_status(e) == 400 and "dependent object" in get_error_body(e)


F = TypeVar("F")  # so mypy understands passed through types


def awsRetry(f: Callable[..., F]) -> Callable[..., F]:
    """
    This decorator retries the wrapped function if aws throws unexpected errors.

    It should wrap any function that makes use of boto
    """

    @wraps(f)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        for attempt in old_retry(
            delays=truncExpBackoff(), timeout=300, predicate=awsRetryPredicate
        ):
            with attempt:
                return f(*args, **kwargs)

    return wrapper


def awsFilterImpairedNodes(
    nodes: list[InstanceTypeDef], boto3_ec2: EC2Client
) -> list[InstanceTypeDef]:
    # if TOIL_AWS_NODE_DEBUG is set don't terminate nodes with
    # failing status checks so they can be debugged
    nodeDebug = os.environ.get("TOIL_AWS_NODE_DEBUG") in ("True", "TRUE", "true", True)
    if not nodeDebug:
        return nodes
    nodeIDs = [node["InstanceId"] for node in nodes]
    statuses = boto3_ec2.describe_instance_status(InstanceIds=nodeIDs)
    statusMap = {
        status["InstanceId"]: status["InstanceStatus"]["Status"]
        for status in statuses["InstanceStatuses"]
    }
    healthyNodes = [
        node for node in nodes if statusMap.get(node["InstanceId"], None) != "impaired"
    ]
    impairedNodes = [
        node["InstanceId"]
        for node in nodes
        if statusMap.get(node["InstanceId"], None) == "impaired"
    ]
    logger.warning(
        "TOIL_AWS_NODE_DEBUG is set and nodes %s have failed EC2 status checks so "
        "will not be terminated.",
        " ".join(impairedNodes),
    )
    return healthyNodes


class InvalidClusterStateException(Exception):
    pass


def collapse_tags(instance_tags: list[TagTypeDef]) -> dict[str, str]:
    """
    Collapse tags from boto3 format to node format
    :param instance_tags: tags as a list
    :return: Dict of tags
    """
    collapsed_tags: dict[str, str] = dict()
    for tag in instance_tags:
        if tag.get("Key") is not None:
            collapsed_tags[tag["Key"]] = tag["Value"]
    return collapsed_tags


class AWSProvisioner(AbstractProvisioner):
    def __init__(
        self,
        clusterName: str | None,
        clusterType: str | None,
        zone: str | None,
        nodeStorage: int,
        nodeStorageOverrides: list[str] | None,
        sseKey: str | None,
        enable_fuse: bool,
    ):
        self.cloud = "aws"
        self._sseKey = sseKey
        # self._zone will be filled in by base class constructor
        # We will use it as the leader zone.
        zone = zone if zone else get_best_aws_zone()

        if zone is None:
            # Can't proceed without a real zone
            raise RuntimeError(
                "No AWS availability zone specified. Configure in Boto "
                "configuration file, TOIL_AWS_ZONE environment variable, or "
                "on the command line."
            )

        # Determine our region to work in, before readClusterSettings() which
        # might need it. TODO: support multiple regions in one cluster
        self._region: AWSRegionName = zone_to_region(zone)

        # Set up our connections to AWS
        self.aws = AWSConnectionManager()

        # Set our architecture to the current machine architecture
        # Assume the same architecture unless specified differently in launchCluster()
        self._architecture = (
            "amd64" if platform.machine() in ["x86_64", "amd64"] else "arm64"
        )

        # Call base class constructor, which will call createClusterSettings()
        # or readClusterSettings()
        super().__init__(
            clusterName,
            clusterType,
            zone,
            nodeStorage,
            nodeStorageOverrides,
            enable_fuse,
        )

        if self._zone is None:
            logger.warning(
                "Leader zone was never initialized before creating AWS provisioner. Defaulting to cluster zone."
            )

        self._leader_subnet: str = self._get_default_subnet(self._zone or zone)
        self._tags: dict[str, Any] = {}

        # After self.clusterName is set, generate a valid name for the S3 bucket associated with this cluster
        suffix = _S3_BUCKET_INTERNAL_SUFFIX
        self.s3_bucket_name = (
            self.clusterName[: _S3_BUCKET_MAX_NAME_LEN - len(suffix)] + suffix
        )

    def supportedClusterTypes(self) -> set[str]:
        return {"mesos", "kubernetes"}

    def createClusterSettings(self) -> None:
        """
        Create a new set of cluster settings for a cluster to be deployed into
        AWS.
        """

        # Nothing needs to happen here; self._zone is always filled by the
        # constructor.
        assert self._zone is not None

    def readClusterSettings(self) -> None:
        """
        Reads the cluster settings from the instance metadata, which assumes
        the instance is the leader.
        """
        from ec2_metadata import ec2_metadata

        boto3_ec2 = self.aws.client(self._region, "ec2")
        instance: InstanceTypeDef = boto3_ec2.describe_instances(
            InstanceIds=[ec2_metadata.instance_id]
        )["Reservations"][0]["Instances"][0]
        # The cluster name is the same as the name of the leader.
        self.clusterName: str = "default-toil-cluster-name"
        for tag in instance["Tags"]:
            if tag.get("Key") == "Name":
                self.clusterName = tag["Value"]
        # Determine what subnet we, the leader, are in
        self._leader_subnet = instance["SubnetId"]
        # Determine where to deploy workers.
        self._worker_subnets_by_zone = self._get_good_subnets_like(self._leader_subnet)

        self._leaderPrivateIP = ec2_metadata.private_ipv4  # this is PRIVATE IP
        self._tags = {
            k: v
            for k, v in (self.getLeader().tags or {}).items()
            if k != _TAG_KEY_TOIL_NODE_TYPE
        }
        # Grab the ARN name of the instance profile (a str) to apply to workers
        leader_info = None
        for attempt in old_retry(timeout=300, predicate=lambda e: True):
            with attempt:
                leader_info = ec2_metadata.iam_info
                if leader_info is None:
                    raise RuntimeError("Could not get EC2 metadata IAM info")
        if leader_info is None:
            # This is more for mypy as it is unable to see that the retry will guarantee this is not None
            # and that this is not reachable
            raise RuntimeError(f"Leader IAM metadata is unreachable.")
        self._leaderProfileArn = leader_info["InstanceProfileArn"]

        # The existing metadata API returns a single string if there is one security group, but
        # a list when there are multiple: change the format to always be a list.
        rawSecurityGroups = ec2_metadata.security_groups
        self._leaderSecurityGroupNames: set[str] = set(rawSecurityGroups)
        # Since we have access to the names, we don't also need to use any IDs
        self._leaderSecurityGroupIDs: set[str] = set()

        # Let the base provisioner work out how to deploy duly authorized
        # workers for this leader.
        self._setLeaderWorkerAuthentication()

    @retry(errors=[AWSServerErrors])
    def _write_file_to_cloud(self, key: str, contents: bytes) -> str:
        bucket_name = self.s3_bucket_name

        # Connect to S3
        s3 = self.aws.resource(self._region, "s3")
        s3_client = self.aws.client(self._region, "s3")

        # create bucket if needed, then write file to S3
        try:
            # the head_bucket() call makes sure that the bucket exists and the user can access it
            s3_client.head_bucket(Bucket=bucket_name)
            bucket = s3.Bucket(bucket_name)
        except ClientError as err:
            if get_error_status(err) == 404:
                bucket = create_s3_bucket(
                    s3, bucket_name=bucket_name, region=self._region
                )
                bucket.wait_until_exists()
                bucket.Versioning().enable()

                owner_tag = os.environ.get("TOIL_OWNER_TAG")
                if owner_tag:
                    bucket_tagging = s3.BucketTagging(bucket_name)
                    bucket_tagging.put(
                        Tagging={"TagSet": [{"Key": "Owner", "Value": owner_tag}]}
                    )
            else:
                raise

        # write file to bucket
        logger.debug(f'Writing "{key}" to bucket "{bucket_name}"...')
        obj = bucket.Object(key=key)
        obj.put(Body=contents)

        obj.wait_until_exists()
        return f"s3://{bucket_name}/{key}"

    def _read_file_from_cloud(self, key: str) -> bytes:
        bucket_name = self.s3_bucket_name
        obj = self.aws.resource(self._region, "s3").Object(bucket_name, key)

        try:
            return obj.get()["Body"].read()
        except ClientError as e:
            if get_error_status(e) == 404:
                logger.warning(
                    f'Trying to read non-existent file "{key}" from {bucket_name}.'
                )
            raise

    def _get_user_data_limit(self) -> int:
        # See: https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/instancedata-add-user-data.html
        return human2bytes("16KB")

    def launchCluster(
        self,
        leaderNodeType: str,
        leaderStorage: int,
        owner: str,
        keyName: str,
        botoPath: str,
        userTags: dict[str, str] | None,
        vpcSubnet: str | None,
        awsEc2ProfileArn: str | None,
        awsEc2ExtraSecurityGroupIds: list[str] | None,
        **kwargs: dict[str, Any],
    ) -> None:
        """
        Starts a single leader node and populates this class with the leader's metadata.

        :param leaderNodeType: An AWS instance type, like "t2.medium", for example.
        :param leaderStorage: An integer number of gigabytes to provide the leader instance with.
        :param owner: Resources will be tagged with this owner string.
        :param keyName: The ssh key to use to access the leader node.
        :param botoPath: The path to the boto credentials directory.
        :param userTags: Optionally provided user tags to put on the cluster.
        :param vpcSubnet: Optionally specify the VPC subnet for the leader.
        :param awsEc2ProfileArn: Optionally provide the profile ARN.
        :param awsEc2ExtraSecurityGroupIds: Optionally provide additional security group IDs.
        :return: None
        """

        if "network" in kwargs:
            logger.warning(
                "AWS provisioner does not support a network parameter. Ignoring %s!",
                kwargs["network"],
            )

        # First, pre-flight-check our permissions before making anything.
        if not policy_permissions_allow(
            get_policy_permissions(region=self._region), CLUSTER_LAUNCHING_PERMISSIONS
        ):
            # Function prints a more specific warning to the log, but give some context.
            logger.warning(
                "Toil may not be able to properly launch (or destroy!) your cluster."
            )

        leader_type = E2Instances[leaderNodeType]

        if self.clusterType == "kubernetes":
            if leader_type.cores < 2:
                # Kubernetes won't run here.
                raise RuntimeError(
                    "Kubernetes requires 2 or more cores, and %s is too small"
                    % leaderNodeType
                )
        self._keyName = keyName
        self._architecture = leader_type.architecture

        if self.clusterType == "mesos" and self._architecture != "amd64":
            # Mesos images aren't currently available for this architecture, so we can't start a Mesos cluster.
            # Complain about this before we create anything.
            raise ClusterCombinationNotSupportedException(
                type(self),
                self.clusterType,
                self._architecture,
                reason="Mesos is only available for amd64.",
            )

        if vpcSubnet:
            # This is where we put the leader
            self._leader_subnet = vpcSubnet

        profileArn = awsEc2ProfileArn or self._createProfileArn()

        # the security group name is used as the cluster identifier
        createdSGs = self._createSecurityGroups()
        bdms = self._getBoto3BlockDeviceMappings(leader_type, rootVolSize=leaderStorage)

        # Make up the tags
        self._tags = {
            "Name": self.clusterName,
            "Owner": owner,
            _TAG_KEY_TOIL_CLUSTER_NAME: self.clusterName,
        }

        if userTags is not None:
            self._tags.update(userTags)

        # All user specified tags have been set
        userData = self._getIgnitionUserData("leader", architecture=self._architecture)

        if self.clusterType == "kubernetes":
            # All nodes need a tag putting them in the cluster.
            # This tag needs to be on there before the a leader can finish its startup.
            self._tags["kubernetes.io/cluster/" + self.clusterName] = ""

        # Make tags for the leader specifically
        leader_tags = dict(self._tags)
        leader_tags[_TAG_KEY_TOIL_NODE_TYPE] = "leader"
        logger.debug("Launching leader with tags: %s", leader_tags)

        instances: list[Instance] = create_instances(
            self.aws.resource(self._region, "ec2"),
            image_id=self._discoverAMI(),
            num_instances=1,
            key_name=self._keyName,
            security_group_ids=createdSGs + (awsEc2ExtraSecurityGroupIds or []),
            instance_type=leader_type.name,
            user_data=userData,
            block_device_map=bdms,
            instance_profile_arn=profileArn,
            placement_az=self._zone,
            subnet_id=self._leader_subnet,
            tags=leader_tags,
        )

        # wait for the leader to exist at all
        leader = instances[0]
        leader.wait_until_exists()

        # Don't go on until the leader is started
        logger.info("Waiting for leader instance %s to be running", leader)
        leader.wait_until_running()

        # Now reload it to make sure all the IPs are set.
        leader.reload()

        if leader.public_ip_address is None:
            # Sometimes AWS just fails to assign a public IP when we really need one.
            # But sometimes people intend to use private IPs only in Toil-managed clusters.
            # TODO: detect if we have a route to the private IP and fail fast if not.
            logger.warning(
                "AWS did not assign a public IP to the cluster leader. If you aren't "
                "connected to the private subnet, cluster setup will fail!"
            )

        # Remember enough about the leader to let us launch workers in its
        # cluster.
        self._leaderPrivateIP = leader.private_ip_address
        self._worker_subnets_by_zone = self._get_good_subnets_like(self._leader_subnet)
        self._leaderSecurityGroupNames = set()
        self._leaderSecurityGroupIDs = set(
            createdSGs + (awsEc2ExtraSecurityGroupIds or [])
        )
        self._leaderProfileArn = profileArn

        leaderNode = Node(
            publicIP=leader.public_ip_address,
            privateIP=leader.private_ip_address,
            name=leader.id,
            launchTime=leader.launch_time,
            nodeType=leader_type.name,
            preemptible=False,
            tags=collapse_tags(leader.tags),
        )
        leaderNode.waitForNode("toil_leader")

        # Download credentials
        self._setLeaderWorkerAuthentication(leaderNode)

    def toil_service_env_options(self) -> str:
        """
        Set AWS tags in user docker container
        """
        config = super().toil_service_env_options()
        instance_base_tags = json.dumps(self._tags)
        return config + " -e TOIL_AWS_TAGS=" + quote(instance_base_tags)

    def _get_worker_subnets(self) -> list[str]:
        """
        Get all worker subnets we should balance across, as a flat list.
        """
        # TODO: When we get multi-region clusters, scope this by region

        # This will hold the collected list of subnet IDs.
        collected = []
        for subnets in self._worker_subnets_by_zone.values():
            # We assume all zones are in the same region here.
            for subnet in subnets:
                # We don't need to deduplicate because each subnet is in only one region
                collected.append(subnet)
        return collected

    @awsRetry
    def _get_good_subnets_like(self, base_subnet_id: str) -> dict[str, list[str]]:
        """
        Given a subnet ID, get all the similar subnets (including it),
        organized by availability zone.

        The input subnet must be in the available state.

        Similar subnets are ones with the same default-ness and ACLs.
        """

        # Grab the ec2 resource we need to make queries
        ec2 = self.aws.resource(self._region, "ec2")
        # And the client
        ec2_client = self.aws.client(self._region, "ec2")

        # What subnet are we basing this on?
        base_subnet = ec2.Subnet(base_subnet_id)

        # What VPC is it in?
        vpc_id = base_subnet.vpc_id

        # Is it default for its VPC?
        is_default = base_subnet.default_for_az

        # What ACLs does it have?
        acls = set(self._get_subnet_acls(base_subnet_id))

        # Compose a filter that selects the subnets we might want
        filters: list[FilterTypeDef] = [
            {"Name": "vpc-id", "Values": [vpc_id]},
            {"Name": "default-for-az", "Values": ["true" if is_default else "false"]},
            {"Name": "state", "Values": ["available"]},
        ]

        # Fill in this collection
        by_az: dict[str, list[str]] = {}

        # Go get all the subnets. There's no way to page manually here so it
        # must page automatically.
        for subnet in self.aws.resource(self._region, "ec2").subnets.filter(
            Filters=filters
        ):
            # For each subnet in the VPC

            # See if it has the right ACLs
            subnet_acls = set(self._get_subnet_acls(subnet.subnet_id))
            if subnet_acls != acls:
                # Reject this subnet because it has different ACLs
                logger.debug(
                    "Subnet %s is a lot like subnet %s but has ACLs of %s instead of %s; skipping",
                    subnet.subnet_id,
                    base_subnet_id,
                    subnet_acls,
                    acls,
                )
                continue

            if subnet.availability_zone not in by_az:
                # Make sure we have a bucket of subnets for this AZ
                by_az[subnet.availability_zone] = []
            # Bucket the IDs by availability zone
            by_az[subnet.availability_zone].append(subnet.subnet_id)

        return by_az

    @awsRetry
    def _get_subnet_acls(self, subnet: str) -> list[str]:
        """
        Get all Network ACL IDs associated with a given subnet ID.
        """

        # Grab the connection we need to use for this operation.
        ec2 = self.aws.client(self._region, "ec2")

        # Compose a filter that selects the default subnet in the AZ
        filters = [{"Name": "association.subnet-id", "Values": [subnet]}]

        # TODO: Can't we use the resource's network_acls.filter(Filters=)?
        return [
            item["NetworkAclId"]
            for item in boto3_pager(
                ec2.describe_network_acls, "NetworkAcls", Filters=filters
            )
        ]

    @awsRetry
    def _get_default_subnet(self, zone: str) -> str:
        """
        Given an availability zone, get the default subnet for the default VPC
        in that zone.
        """

        # Compose a filter that selects the default subnet in the AZ
        filters: list[FilterTypeDef] = [
            {"Name": "default-for-az", "Values": ["true"]},
            {"Name": "availability-zone", "Values": [zone]},
        ]

        for subnet in self.aws.resource(zone_to_region(zone), "ec2").subnets.filter(
            Filters=filters
        ):
            # There should only be one result, so when we see it, return it
            return subnet.subnet_id
        # If we don't find a subnet, something is wrong. Maybe this zone was
        # added after your account?
        raise RuntimeError(
            f"No default subnet found in availability zone {zone}. "
            f"Note that Amazon does not add default subnets for new "
            f"zones to old accounts. Specify a VPC subnet ID to use, "
            f"or create a default subnet in the zone."
        )

    def getKubernetesAutoscalerSetupCommands(self, values: dict[str, str]) -> str:
        """
        Get the Bash commands necessary to configure the Kubernetes Cluster Autoscaler for AWS.
        """

        return textwrap.dedent(
            """\
            curl -sSL https://raw.githubusercontent.com/kubernetes/autoscaler/cluster-autoscaler-{AUTOSCALER_VERSION}/cluster-autoscaler/cloudprovider/aws/examples/cluster-autoscaler-run-on-master.yaml | \\
                sed "s|--nodes={{{{ node_asg_min }}}}:{{{{ node_asg_max }}}}:{{{{ name }}}}|--node-group-auto-discovery=asg:tag=k8s.io/cluster-autoscaler/enabled,k8s.io/cluster-autoscaler/{CLUSTER_NAME}|" | \\
                sed 's|kubernetes.io/role: master|node-role.kubernetes.io/master: ""|' | \\
                sed 's|operator: "Equal"|operator: "Exists"|' | \\
                sed '/value: "true"/d' | \\
                sed 's|path: "/etc/ssl/certs/ca-bundle.crt"|path: "/usr/share/ca-certificates/ca-certificates.crt"|' | \\
                kubectl apply -f -
            """
        ).format(**values)

    def getKubernetesCloudProvider(self) -> str | None:
        """
        Use the "aws" Kubernetes cloud provider when setting up Kubernetes.
        """

        return "aws"

    def getNodeShape(self, instance_type: str, preemptible: bool = False) -> Shape:
        """
        Get the Shape for the given instance type (e.g. 't2.medium').
        """
        type_info = E2Instances[instance_type]

        disk = type_info.disks * type_info.disk_capacity * 2**30
        if disk == 0:
            # This is an EBS-backed instance. We will use the root
            # volume, so add the amount of EBS storage requested for
            # the root volume
            disk = (
                self._nodeStorageOverrides.get(instance_type, self._nodeStorage) * 2**30
            )

        # Underestimate memory by 100M to prevent autoscaler from disagreeing with
        # mesos about whether a job can run on a particular node type
        memory = (type_info.memory - 0.1) * 2**30
        return Shape(
            wallTime=60 * 60,
            memory=int(memory),
            cores=type_info.cores,
            disk=int(disk),
            preemptible=preemptible,
        )

    @staticmethod
    def retryPredicate(e: Exception) -> bool:
        return awsRetryPredicate(e)

    def destroyCluster(self) -> None:
        """Terminate instances and delete the profile and security group."""

        # We should terminate the leader first in case a workflow is still running in the cluster.
        # The leader may create more instances while we're terminating the workers.
        vpcId = None
        try:
            leader = self._getLeaderInstanceBoto3()
            vpcId = leader.get("VpcId")
            logger.info("Terminating the leader first ...")
            self._terminateInstances([leader])
        except (NoSuchClusterException, InvalidClusterStateException):
            # It's ok if the leader is not found. We'll terminate any remaining
            # instances below anyway.
            pass

        logger.debug("Deleting autoscaling groups ...")
        removed = False

        for attempt in old_retry(timeout=300, predicate=expectedShutdownErrors):
            with attempt:
                for asgName in self._getAutoScalingGroupNames():
                    try:
                        # We delete the group and all the instances via ForceDelete.
                        self.aws.client(
                            self._region, "autoscaling"
                        ).delete_auto_scaling_group(
                            AutoScalingGroupName=asgName, ForceDelete=True
                        )
                        removed = True
                    except ClientError as e:
                        if get_error_code(
                            e
                        ) == "ValidationError" and "AutoScalingGroup name not found" in get_error_message(
                            e
                        ):
                            # This ASG does not need to be removed (or a
                            # previous delete returned an error but also
                            # succeeded).
                            pass

        if removed:
            logger.debug("... Successfully deleted autoscaling groups")

        # Do the workers after the ASGs because some may belong to ASGs
        logger.info("Terminating any remaining workers ...")
        removed = False
        instances = self._get_nodes_in_cluster_boto3(include_stopped_nodes=True)
        spotIDs = self._getSpotRequestIDs()
        boto3_ec2: EC2Client = self.aws.client(region=self._region, service_name="ec2")
        if spotIDs:
            boto3_ec2.cancel_spot_instance_requests(SpotInstanceRequestIds=spotIDs)
            # self.aws.boto2(self._region, 'ec2').cancel_spot_instance_requests(request_ids=spotIDs)
            removed = True
        instancesToTerminate = awsFilterImpairedNodes(
            instances, self.aws.client(self._region, "ec2")
        )
        if instancesToTerminate:
            vpcId = vpcId or instancesToTerminate[0].get("VpcId")
            self._terminateInstances(instancesToTerminate)
            removed = True
        if removed:
            logger.debug("... Successfully terminated workers")

        logger.info("Deleting launch templates ...")
        removed = False
        for attempt in old_retry(timeout=300, predicate=expectedShutdownErrors):
            with attempt:
                # We'll set this to True if we don't get a proper response
                # for some LuanchTemplate.
                mistake = False
                for ltID in self._get_launch_template_ids():
                    response = boto3_ec2.delete_launch_template(LaunchTemplateId=ltID)
                    if "LaunchTemplate" not in response:
                        mistake = True
                    else:
                        removed = True
        if mistake:
            # We missed something
            removed = False
        if removed:
            logger.debug("... Successfully deleted launch templates")

        if len(instances) == len(instancesToTerminate):
            # All nodes are gone now.

            logger.info("Deleting IAM roles ...")
            self._deleteRoles(self._getRoleNames())
            self._deleteInstanceProfiles(self._getInstanceProfileNames())

            logger.info("Deleting security group ...")
            removed = False
            for attempt in old_retry(timeout=300, predicate=expectedShutdownErrors):
                with attempt:
                    security_groups: list[SecurityGroupTypeDef] = (
                        boto3_ec2.describe_security_groups()["SecurityGroups"]
                    )
                    for security_group in security_groups:
                        # TODO: If we terminate the leader and the workers but
                        # miss the security group, we won't find it now because
                        # we won't have vpcId set.
                        if (
                            security_group.get("GroupName") == self.clusterName
                            and vpcId
                            and security_group.get("VpcId") == vpcId
                        ):
                            try:
                                boto3_ec2.delete_security_group(
                                    GroupId=security_group["GroupId"]
                                )
                                removed = True
                            except ClientError as e:
                                if get_error_code(e) == "InvalidGroup.NotFound":
                                    pass
                                else:
                                    raise
            if removed:
                logger.debug("... Successfully deleted security group")
        else:
            assert len(instances) > len(instancesToTerminate)
            # the security group can't be deleted until all nodes are terminated
            logger.warning(
                "The TOIL_AWS_NODE_DEBUG environment variable is set and some nodes "
                "have failed health checks. As a result, the security group & IAM "
                "roles will not be deleted."
            )

        # delete S3 buckets that might have been created by `self._write_file_to_cloud()`
        logger.info("Deleting S3 buckets ...")
        removed = False
        for attempt in old_retry(timeout=300, predicate=awsRetryPredicate):
            with attempt:
                # Grab the S3 resource to use
                s3 = self.aws.resource(self._region, "s3")
                try:
                    bucket = s3.Bucket(self.s3_bucket_name)

                    bucket.objects.all().delete()
                    bucket.object_versions.delete()
                    bucket.delete()
                    removed = True
                except s3.meta.client.exceptions.NoSuchBucket:
                    pass
                except ClientError as e:
                    if get_error_status(e) == 404:
                        pass
                    else:
                        raise  # retry this
        if removed:
            print("... Successfully deleted S3 buckets")

    def terminateNodes(self, nodes: list[Node]) -> None:
        if nodes:
            self._terminateIDs([x.name for x in nodes])

    def _recover_node_type_bid(
        self, node_type: set[str], spot_bid: float | None
    ) -> float | None:
        """
        The old Toil-managed autoscaler will tell us to make some nodes of
        particular instance types, and to just work out a bid, but it doesn't
        know anything about instance type equivalence classes within a node
        type. So we need to do some work to infer how much to bid by guessing
        some node type an instance type could belong to.

        If we get a set of instance types corresponding to a known node type
        with a bid, we use that bid instead.

        :return: the guessed spot bid
        """
        if spot_bid is None:
            if self._spotBidsMap and frozenset(node_type) in self._spotBidsMap:
                spot_bid = self._spotBidsMap[frozenset(node_type)]
            elif len(node_type) == 1:
                # The Toil autoscaler forgets the equivalence classes. Find
                # some plausible equivalence class.
                instance_type = next(iter(node_type))
                for types, bid in self._spotBidsMap.items():
                    if instance_type in types:
                        # We bid on a class that includes this type
                        spot_bid = bid
                        break
                if spot_bid is None:
                    # We didn't bid on any class including this type either
                    raise RuntimeError(
                        "No spot bid given for a preemptible node request."
                    )
            else:
                raise RuntimeError("No spot bid given for a preemptible node request.")

        return spot_bid

    def addNodes(
        self,
        nodeTypes: set[str],
        numNodes: int,
        preemptible: bool,
        spotBid: float | None = None,
    ) -> int:
        # Grab the AWS connection we need
        boto3_ec2 = get_client(service_name="ec2", region_name=self._region)
        assert self._leaderPrivateIP

        if preemptible:
            # May need to provide a spot bid
            spotBid = self._recover_node_type_bid(nodeTypes, spotBid)

        # We don't support any balancing here so just pick one of the
        # equivalent node types
        node_type = next(iter(nodeTypes))
        type_info = E2Instances[node_type]
        root_vol_size = self._nodeStorageOverrides.get(node_type, self._nodeStorage)
        bdm = self._getBoto3BlockDeviceMapping(type_info, rootVolSize=root_vol_size)

        # Pick a zone and subnet_id to launch into
        if preemptible:
            # We may need to balance preemptible instances across zones, which
            # then affects the subnets they can use.

            # We're allowed to pick from any of these zones.
            zone_options = list(self._worker_subnets_by_zone.keys())

            zone = get_best_aws_zone(spotBid, type_info.name, boto3_ec2, zone_options)
        else:
            # We don't need to ever do any balancing across zones for on-demand
            # instances. Just pick a zone.
            if self._zone in self._worker_subnets_by_zone:
                # We can launch into the same zone as the leader
                zone = self._zone
            else:
                # The workers aren't allowed in the leader's zone.
                # Pick an arbitrary zone we can use.
                zone = next(iter(self._worker_subnets_by_zone.keys()))
        if zone is None:
            logger.exception(
                "Could not find a valid zone. Make sure TOIL_AWS_ZONE is set or spot bids are not too low."
            )
            raise NoSuchZoneException()
        if self._leader_subnet in self._worker_subnets_by_zone.get(zone, []):
            # The leader's subnet is an option for this zone, so use it.
            subnet_id = self._leader_subnet
        else:
            # Use an arbitrary subnet from the zone
            subnet_id = next(iter(self._worker_subnets_by_zone[zone]))

        keyPath = self._sseKey if self._sseKey else None
        userData: str = self._getIgnitionUserData(
            "worker", keyPath, preemptible, self._architecture
        )
        userDataBytes: bytes = b""
        if isinstance(userData, str):
            # Spot-market provisioning requires bytes for user data.
            userDataBytes = userData.encode("utf-8")

        spot_kwargs = {
            "KeyName": self._keyName,
            "LaunchSpecification": {
                "SecurityGroupIds": self._getSecurityGroupIDs(),
                "InstanceType": type_info.name,
                "UserData": userDataBytes,
                "BlockDeviceMappings": bdm,
                "IamInstanceProfile": {"Arn": self._leaderProfileArn},
                "Placement": {"AvailabilityZone": zone},
                "SubnetId": subnet_id,
            },
        }
        on_demand_kwargs = {
            "KeyName": self._keyName,
            "SecurityGroupIds": self._getSecurityGroupIDs(),
            "InstanceType": type_info.name,
            "UserData": userDataBytes,
            "BlockDeviceMappings": bdm,
            "IamInstanceProfile": {"Arn": self._leaderProfileArn},
            "Placement": {"AvailabilityZone": zone},
            "SubnetId": subnet_id,
        }

        instancesLaunched: list[InstanceTypeDef] = []

        for attempt in old_retry(predicate=awsRetryPredicate):
            with attempt:
                # after we start launching instances we want to ensure the full setup is done
                # the biggest obstacle is AWS request throttling, so we retry on these errors at
                # every request in this method
                if not preemptible:
                    logger.debug("Launching %s non-preemptible nodes", numNodes)
                    instancesLaunched = create_ondemand_instances(
                        boto3_ec2=boto3_ec2,
                        image_id=self._discoverAMI(),
                        spec=on_demand_kwargs,
                        num_instances=numNodes,
                    )
                else:
                    logger.debug("Launching %s preemptible nodes", numNodes)
                    # force generator to evaluate
                    generatedInstancesLaunched: list[DescribeInstancesResultTypeDef] = (
                        list(
                            create_spot_instances(
                                boto3_ec2=boto3_ec2,
                                price=spotBid,
                                image_id=self._discoverAMI(),
                                tags={_TAG_KEY_TOIL_CLUSTER_NAME: self.clusterName},
                                spec=spot_kwargs,
                                num_instances=numNodes,
                                tentative=True,
                            )
                        )
                    )
                    # flatten the list
                    flatten_reservations: list[ReservationTypeDef] = [
                        reservation
                        for subdict in generatedInstancesLaunched
                        for reservation in subdict["Reservations"]
                        for key, value in subdict.items()
                    ]
                    # get a flattened list of all requested instances, as before instancesLaunched is a dict of reservations which is a dict of instance requests
                    instancesLaunched = [
                        instance
                        for instances in flatten_reservations
                        for instance in instances["Instances"]
                    ]

        for attempt in old_retry(predicate=awsRetryPredicate):
            with attempt:
                list(
                    wait_instances_running(boto3_ec2, instancesLaunched)
                )  # ensure all instances are running

        increase_instance_hop_limit(boto3_ec2, instancesLaunched)

        self._tags[_TAG_KEY_TOIL_NODE_TYPE] = "worker"
        AWSProvisioner._addTags(boto3_ec2, instancesLaunched, self._tags)
        if self._sseKey:
            for i in instancesLaunched:
                self._waitForIP(i)
                node = Node(
                    publicIP=i["PublicIpAddress"],
                    privateIP=i["PrivateIpAddress"],
                    name=i["InstanceId"],
                    launchTime=i["LaunchTime"],
                    nodeType=i["InstanceType"],
                    preemptible=preemptible,
                    tags=collapse_tags(i["Tags"]),
                )
                node.waitForNode("toil_worker")
                node.coreRsync(
                    [self._sseKey, ":" + self._sseKey], applianceName="toil_worker"
                )
        logger.debug("Launched %s new instance(s)", numNodes)
        return len(instancesLaunched)

    def addManagedNodes(
        self,
        nodeTypes: set[str],
        minNodes: int,
        maxNodes: int,
        preemptible: bool,
        spotBid: float | None = None,
    ) -> None:

        if self.clusterType != "kubernetes":
            raise ManagedNodesNotSupportedException(
                "Managed nodes only supported for Kubernetes clusters"
            )

        assert self._leaderPrivateIP

        if preemptible:
            # May need to provide a spot bid
            spotBid = self._recover_node_type_bid(nodeTypes, spotBid)

        # TODO: We assume we only ever do this once per node type...

        # Make one template per node type, so we can apply storage overrides correctly
        # TODO: deduplicate these if the same instance type appears in multiple sets?
        launch_template_ids = {
            n: self._get_worker_launch_template(n, preemptible=preemptible)
            for n in nodeTypes
        }
        # Make the ASG across all of them
        self._createWorkerAutoScalingGroup(
            launch_template_ids, nodeTypes, minNodes, maxNodes, spot_bid=spotBid
        )

    def getProvisionedWorkers(
        self, instance_type: str | None = None, preemptible: bool | None = None
    ) -> list[Node]:
        assert self._leaderPrivateIP
        entireCluster = self._get_nodes_in_cluster_boto3(instance_type=instance_type)
        logger.debug("All nodes in cluster: %s", entireCluster)
        workerInstances: list[InstanceTypeDef] = [
            i for i in entireCluster if i["PrivateIpAddress"] != self._leaderPrivateIP
        ]
        logger.debug("All workers found in cluster: %s", workerInstances)
        if preemptible is not None:
            workerInstances = [
                i
                for i in workerInstances
                if preemptible == (i["SpotInstanceRequestId"] is not None)
            ]
            logger.debug(
                "%spreemptible workers found in cluster: %s",
                "non-" if not preemptible else "",
                workerInstances,
            )
        workerInstances = awsFilterImpairedNodes(
            workerInstances, self.aws.client(self._region, "ec2")
        )
        return [
            Node(
                publicIP=i["PublicIpAddress"],
                privateIP=i["PrivateIpAddress"],
                name=i["InstanceId"],
                launchTime=i["LaunchTime"],
                nodeType=i["InstanceType"],
                preemptible=i["SpotInstanceRequestId"] is not None,
                tags=collapse_tags(i["Tags"]),
            )
            for i in workerInstances
        ]

    @memoize
    def _discoverAMI(self) -> str:
        """
        :return: The AMI ID (a string like 'ami-0a9a5d2b65cce04eb') for Flatcar.
        :rtype: str
        """
        return get_flatcar_ami(self.aws.client(self._region, "ec2"), self._architecture)

    def _toNameSpace(self) -> str:
        assert isinstance(self.clusterName, (str, bytes))
        if any(char.isupper() for char in self.clusterName) or "_" in self.clusterName:
            raise RuntimeError(
                "The cluster name must be lowercase and cannot contain the '_' "
                "character."
            )
        namespace = self.clusterName
        if not namespace.startswith("/"):
            namespace = "/" + namespace + "/"
        return namespace.replace("-", "/")

    def _namespace_name(self, name: str) -> str:
        """
        Given a name for a thing, add our cluster name to it in a way that
        results in an acceptable name for something on AWS.
        """

        # This logic is a bit weird, but it's what Boto2Context used to use.
        # Drop the leading / from the absolute-path-style "namespace" name and
        # then encode underscores and slashes.
        return (self._toNameSpace() + name)[1:].replace("_", "__").replace("/", "_")

    def _is_our_namespaced_name(self, namespaced_name: str) -> bool:
        """
        Return True if the given AWS object name looks like it belongs to us
        and was generated by _namespace_name().
        """

        denamespaced = "/" + "_".join(
            s.replace("_", "/") for s in namespaced_name.split("__")
        )
        return denamespaced.startswith(self._toNameSpace())

    def _getLeaderInstanceBoto3(self) -> InstanceTypeDef:
        """
        Get the Boto 3 instance for the cluster's leader.
        """
        # Tags are stored differently in Boto 3
        instances: list[InstanceTypeDef] = self._get_nodes_in_cluster_boto3(
            include_stopped_nodes=True
        )
        instances.sort(key=lambda x: x["LaunchTime"])
        try:
            leader = instances[0]  # assume leader was launched first
        except IndexError:
            raise NoSuchClusterException(self.clusterName)
        if leader.get("Tags") is not None:
            tag_value = next(
                item["Value"]
                for item in leader["Tags"]
                if item["Key"] == _TAG_KEY_TOIL_NODE_TYPE
            )
        else:
            tag_value = None
        if (tag_value or "leader") != "leader":
            raise InvalidClusterStateException(
                "Invalid cluster state! The first launched instance appears not to be the leader "
                'as it is missing the "leader" tag. The safest recovery is to destroy the cluster '
                "and restart the job. Incorrect Leader ID: %s" % leader["InstanceId"]
            )
        return leader

    def _getLeaderInstance(self) -> InstanceTypeDef:
        """
        Get the Boto 2 instance for the cluster's leader.
        """
        instances = self._get_nodes_in_cluster_boto3(include_stopped_nodes=True)
        instances.sort(key=lambda x: x["LaunchTime"])
        try:
            leader: InstanceTypeDef = instances[0]  # assume leader was launched first
        except IndexError:
            raise NoSuchClusterException(self.clusterName)
        tagged_node_type: str = "leader"
        for tag in leader["Tags"]:
            # If a tag specifying node type exists,
            if tag.get("Key") is not None and tag["Key"] == _TAG_KEY_TOIL_NODE_TYPE:
                tagged_node_type = tag["Value"]
        if tagged_node_type != "leader":
            raise InvalidClusterStateException(
                "Invalid cluster state! The first launched instance appears not to be the leader "
                'as it is missing the "leader" tag. The safest recovery is to destroy the cluster '
                "and restart the job. Incorrect Leader ID: %s" % leader["InstanceId"]
            )
        return leader

    def getLeader(self, wait: bool = False) -> Node:
        """
        Get the leader for the cluster as a Toil Node object.
        """
        leader: InstanceTypeDef = self._getLeaderInstanceBoto3()

        leaderNode = Node(
            publicIP=leader["PublicIpAddress"],
            privateIP=leader["PrivateIpAddress"],
            name=leader["InstanceId"],
            launchTime=leader["LaunchTime"],
            nodeType=None,
            preemptible=False,
            tags=collapse_tags(leader["Tags"]),
        )
        if wait:
            logger.debug("Waiting for toil_leader to enter 'running' state...")
            wait_instances_running(self.aws.client(self._region, "ec2"), [leader])
            logger.debug("... toil_leader is running")
            self._waitForIP(leader)
            leaderNode.waitForNode("toil_leader")

        return leaderNode

    @classmethod
    @awsRetry
    def _addTag(
        cls, boto3_ec2: EC2Client, instance: InstanceTypeDef, key: str, value: str
    ) -> None:
        if instance.get("Tags") is None:
            instance["Tags"] = []
        new_tag: TagTypeDef = {"Key": key, "Value": value}
        boto3_ec2.create_tags(Resources=[instance["InstanceId"]], Tags=[new_tag])

    @classmethod
    def _addTags(
        cls,
        boto3_ec2: EC2Client,
        instances: list[InstanceTypeDef],
        tags: dict[str, str],
    ) -> None:
        for instance in instances:
            for key, value in tags.items():
                cls._addTag(boto3_ec2, instance, key, value)

    @classmethod
    def _waitForIP(cls, instance: InstanceTypeDef) -> None:
        """
        Wait until the instances has a public IP address assigned to it.

        :type instance: boto.ec2.instance.Instance
        """
        logger.debug("Waiting for ip...")
        while True:
            time.sleep(a_short_time)
            if (
                instance.get("PublicIpAddress")
                or instance.get("PublicDnsName")
                or instance.get("PrivateIpAddress")
            ):
                logger.debug("...got ip")
                break

    def _terminateInstances(self, instances: list[InstanceTypeDef]) -> None:
        instanceIDs = [x["InstanceId"] for x in instances]
        self._terminateIDs(instanceIDs)
        logger.info("... Waiting for instance(s) to shut down...")
        for instance in instances:
            wait_transition(
                self.aws.client(region=self._region, service_name="ec2"),
                instance,
                {"pending", "running", "shutting-down", "stopping", "stopped"},
                "terminated",
            )
        logger.info("Instance(s) terminated.")

    @awsRetry
    def _terminateIDs(self, instanceIDs: list[str]) -> None:
        logger.info("Terminating instance(s): %s", instanceIDs)
        boto3_ec2 = self.aws.client(region=self._region, service_name="ec2")
        boto3_ec2.terminate_instances(InstanceIds=instanceIDs)
        logger.info("Instance(s) terminated.")

    @awsRetry
    def _deleteRoles(self, names: list[str]) -> None:
        """
        Delete all the given named IAM roles.
        Detatches but does not delete associated instance profiles.
        """
        boto3_iam = self.aws.client(region=self._region, service_name="iam")
        for role_name in names:
            for profile_name in self._getRoleInstanceProfileNames(role_name):
                # We can't delete either the role or the profile while they
                # are attached.

                for attempt in old_retry(timeout=300, predicate=expectedShutdownErrors):
                    with attempt:
                        boto3_iam.remove_role_from_instance_profile(
                            InstanceProfileName=profile_name, RoleName=role_name
                        )
            # We also need to drop all inline policies
            for policy_name in self._getRoleInlinePolicyNames(role_name):
                for attempt in old_retry(timeout=300, predicate=expectedShutdownErrors):
                    with attempt:
                        boto3_iam.delete_role_policy(
                            PolicyName=policy_name, RoleName=role_name
                        )

            for attempt in old_retry(timeout=300, predicate=expectedShutdownErrors):
                with attempt:
                    boto3_iam.delete_role(RoleName=role_name)
                    logger.debug("... Successfully deleted IAM role %s", role_name)

    @awsRetry
    def _deleteInstanceProfiles(self, names: list[str]) -> None:
        """
        Delete all the given named IAM instance profiles.
        All roles must already be detached.
        """
        boto3_iam = self.aws.client(region=self._region, service_name="iam")
        for profile_name in names:
            for attempt in old_retry(timeout=300, predicate=expectedShutdownErrors):
                with attempt:
                    boto3_iam.delete_instance_profile(InstanceProfileName=profile_name)
                    logger.debug(
                        "... Succesfully deleted instance profile %s", profile_name
                    )

    @classmethod
    def _getBoto3BlockDeviceMapping(
        cls, type_info: InstanceType, rootVolSize: int = 50
    ) -> list[BlockDeviceMappingTypeDef]:
        # determine number of ephemeral drives via cgcloud-lib (actually this is moved into toil's lib
        bdtKeys = [""] + [f"/dev/xvd{c}" for c in string.ascii_lowercase[1:]]
        bdm_list: list[BlockDeviceMappingTypeDef] = []
        # Change root volume size to allow for bigger Docker instances
        root_vol: EbsBlockDeviceTypeDef = {
            "DeleteOnTermination": True,
            "VolumeSize": rootVolSize,
        }
        bdm: BlockDeviceMappingTypeDef = {"DeviceName": "/dev/xvda", "Ebs": root_vol}
        bdm_list.append(bdm)
        # The first disk is already attached for us so start with 2nd.
        # Disk count is weirdly a float in our instance database, so make it an int here.
        for disk in range(1, int(type_info.disks) + 1):
            bdm = {}
            bdm["DeviceName"] = bdtKeys[disk]
            bdm["VirtualName"] = f"ephemeral{disk - 1}"  # ephemeral counts start at 0
            bdm["Ebs"] = root_vol  # default
            # bdm["Ebs"] = root_vol.update({"VirtualName": f"ephemeral{disk - 1}"})
            bdm_list.append(bdm)

        logger.debug("Device mapping: %s", bdm_list)
        return bdm_list

    @classmethod
    def _getBoto3BlockDeviceMappings(
        cls, type_info: InstanceType, rootVolSize: int = 50
    ) -> list[BlockDeviceMappingTypeDef]:
        """
        Get block device mappings for the root volume for a worker.
        """

        # Start with the root
        bdms: list[BlockDeviceMappingTypeDef] = [
            {
                "DeviceName": "/dev/xvda",
                "Ebs": {
                    "DeleteOnTermination": True,
                    "VolumeSize": rootVolSize,
                    "VolumeType": "gp2",
                },
            }
        ]

        # Get all the virtual drives we might have
        bdtKeys = [f"/dev/xvd{c}" for c in string.ascii_lowercase]

        # The first disk is already attached for us so start with 2nd.
        # Disk count is weirdly a float in our instance database, so make it an int here.
        for disk in range(1, int(type_info.disks) + 1):
            # Make a block device mapping to attach the ephemeral disk to a
            # virtual block device in the VM
            bdms.append(
                {
                    "DeviceName": bdtKeys[disk],
                    "VirtualName": f"ephemeral{disk - 1}",  # ephemeral counts start at 0
                }
            )
        logger.debug("Device mapping: %s", bdms)
        return bdms

    @awsRetry
    def _get_nodes_in_cluster_boto3(
        self, instance_type: str | None = None, include_stopped_nodes: bool = False
    ) -> list[InstanceTypeDef]:
        """
        Get Boto3 instance objects for all nodes in the cluster.
        """
        boto3_ec2: EC2Client = self.aws.client(region=self._region, service_name="ec2")
        instance_filter: FilterTypeDef = {
            "Name": "instance.group-name",
            "Values": [self.clusterName],
        }
        describe_response: DescribeInstancesResultTypeDef = (
            boto3_ec2.describe_instances(Filters=[instance_filter])
        )
        all_instances: list[InstanceTypeDef] = []
        for reservation in describe_response["Reservations"]:
            instances = reservation["Instances"]
            all_instances.extend(instances)

        # all_instances = self.aws.boto2(self._region, 'ec2').get_only_instances(filters={'instance.group-name': self.clusterName})

        def instanceFilter(i: InstanceTypeDef) -> bool:
            # filter by type only if nodeType is true
            rightType = not instance_type or i["InstanceType"] == instance_type
            rightState = (
                i["State"]["Name"] == "running" or i["State"]["Name"] == "pending"
            )
            if include_stopped_nodes:
                rightState = (
                    rightState
                    or i["State"]["Name"] == "stopping"
                    or i["State"]["Name"] == "stopped"
                )
            return rightType and rightState

        return [i for i in all_instances if instanceFilter(i)]

    def _getSpotRequestIDs(self) -> list[str]:
        """
        Get the IDs of all spot requests associated with the cluster.
        """

        # Grab the connection we need to use for this operation.
        ec2: EC2Client = self.aws.client(self._region, "ec2")

        requests: list[SpotInstanceRequestTypeDef] = (
            ec2.describe_spot_instance_requests()["SpotInstanceRequests"]
        )
        tag_filter: FilterTypeDef = {
            "Name": "tag:" + _TAG_KEY_TOIL_CLUSTER_NAME,
            "Values": [self.clusterName],
        }
        tags: list[TagDescriptionTypeDef] = ec2.describe_tags(Filters=[tag_filter])[
            "Tags"
        ]
        idsToCancel = [tag["ResourceId"] for tag in tags]
        return [
            request["SpotInstanceRequestId"]
            for request in requests
            if request["InstanceId"] in idsToCancel
        ]

    def _createSecurityGroups(self) -> list[str]:
        """
        Create security groups for the cluster. Returns a list of their IDs.
        """

        def group_not_found(e: ClientError) -> bool:
            retry = get_error_status(
                e
            ) == 400 and "does not exist in default VPC" in get_error_body(e)
            return retry

        # Grab the connection we need to use for this operation.
        # The VPC connection can do anything the EC2 one can do, but also look at subnets.
        boto3_ec2: EC2Client = self.aws.client(region=self._region, service_name="ec2")

        vpc_id = None
        if self._leader_subnet:
            subnets = boto3_ec2.describe_subnets(SubnetIds=[self._leader_subnet])[
                "Subnets"
            ]
            if len(subnets) > 0:
                vpc_id = subnets[0]["VpcId"]
        try:
            # Security groups need to belong to the same VPC as the leader. If we
            # put the leader in a particular non-default subnet, it may be in a
            # particular non-default VPC, which we need to know about.
            other = {
                "GroupName": self.clusterName,
                "Description": "Toil appliance security group",
            }
            if vpc_id is not None:
                other["VpcId"] = vpc_id
            # mypy stubs don't explicitly state kwargs even though documentation allows it, and mypy gets confused
            web_response: CreateSecurityGroupResultTypeDef = boto3_ec2.create_security_group(**other)  # type: ignore[arg-type]
        except ClientError as e:
            if get_error_status(e) == 400 and "already exists" in get_error_body(e):
                pass
            else:
                raise
        else:
            for attempt in old_retry(predicate=group_not_found, timeout=300):
                with attempt:
                    ip_permissions: list[IpPermissionTypeDef] = [
                        {
                            "IpProtocol": "tcp",
                            "FromPort": 22,
                            "ToPort": 22,
                            "IpRanges": [{"CidrIp": "0.0.0.0/0"}],
                            "Ipv6Ranges": [{"CidrIpv6": "::/0"}],
                        }
                    ]
                    for protocol in ("tcp", "udp"):
                        ip_permissions.append(
                            {
                                "IpProtocol": protocol,
                                "FromPort": 0,
                                "ToPort": 65535,
                                "UserIdGroupPairs": [
                                    {
                                        "GroupId": web_response["GroupId"],
                                        "GroupName": self.clusterName,
                                    }
                                ],
                            }
                        )
                    boto3_ec2.authorize_security_group_ingress(
                        IpPermissions=ip_permissions,
                        GroupName=self.clusterName,
                        GroupId=web_response["GroupId"],
                    )
        out = []
        for sg in boto3_ec2.describe_security_groups()["SecurityGroups"]:
            if sg["GroupName"] == self.clusterName and (
                vpc_id is None or sg["VpcId"] == vpc_id
            ):
                out.append(sg["GroupId"])
        return out

    @awsRetry
    def _getSecurityGroupIDs(self) -> list[str]:
        """
        Get all the security group IDs to apply to leaders and workers.
        """

        # TODO: memoize to save requests.

        # Depending on if we enumerated them on the leader or locally, we might
        # know the required security groups by name, ID, or both.
        boto3_ec2 = self.aws.client(region=self._region, service_name="ec2")
        return [
            sg["GroupId"]
            for sg in boto3_ec2.describe_security_groups()["SecurityGroups"]
            if (
                sg["GroupName"] in self._leaderSecurityGroupNames
                or sg["GroupId"] in self._leaderSecurityGroupIDs
            )
        ]

    @awsRetry
    def _get_launch_template_ids(
        self, filters: list[FilterTypeDef] | None = None
    ) -> list[str]:
        """
        Find all launch templates associated with the cluster.

        Returns a list of launch template IDs.
        """

        # Grab the connection we need to use for this operation.
        ec2: EC2Client = self.aws.client(self._region, "ec2")

        # How do we match the right templates?
        combined_filters: list[FilterTypeDef] = [
            {"Name": "tag:" + _TAG_KEY_TOIL_CLUSTER_NAME, "Values": [self.clusterName]}
        ]

        if filters:
            # Add any user-specified filters
            combined_filters += filters

        allTemplateIDs = []
        # Get the first page with no NextToken
        response = ec2.describe_launch_templates(
            Filters=combined_filters, MaxResults=200
        )
        while True:
            # Process the current page
            allTemplateIDs += [
                item["LaunchTemplateId"] for item in response.get("LaunchTemplates", [])
            ]
            if "NextToken" in response:
                # There are more pages. Get the next one, supplying the token.
                response = ec2.describe_launch_templates(
                    Filters=filters or [],
                    NextToken=response["NextToken"],
                    MaxResults=200,
                )
            else:
                # No more pages
                break

        return allTemplateIDs

    @awsRetry
    def _get_worker_launch_template(
        self, instance_type: str, preemptible: bool = False, backoff: float = 1.0
    ) -> str:
        """
        Get a launch template for instances with the given parameters. Only one
        such launch template will be created, no matter how many times the
        function is called.

        Not thread safe.

        :param instance_type: Type of node to use in the template. May be overridden
                              by an ASG that uses the template.

        :param preemptible: When the node comes up, does it think it is a spot instance?

        :param backoff: How long to wait if it seems like we aren't reading our
                        own writes before trying again.

        :return: The ID of the template.
        """

        lt_name = self._name_worker_launch_template(
            instance_type, preemptible=preemptible
        )

        # How do we match the right templates?
        filters: list[FilterTypeDef] = [
            {"Name": "launch-template-name", "Values": [lt_name]}
        ]

        # Get the templates
        templates: list[str] = self._get_launch_template_ids(filters=filters)

        if len(templates) > 1:
            # There shouldn't ever be multiple templates with our reserved name
            raise RuntimeError(
                f"Multiple launch templates already exist named {lt_name}; "
                "something else is operating in our cluster namespace."
            )
        elif len(templates) == 0:
            # Template doesn't exist so we can create it.
            try:
                return self._create_worker_launch_template(
                    instance_type, preemptible=preemptible
                )
            except ClientError as e:
                if (
                    get_error_code(e)
                    == "InvalidLaunchTemplateName.AlreadyExistsException"
                ):
                    # Someone got to it before us (or we couldn't read our own
                    # writes). Recurse to try again, because now it exists.
                    logger.info(
                        "Waiting %f seconds for template %s to be available",
                        backoff,
                        lt_name,
                    )
                    time.sleep(backoff)
                    return self._get_worker_launch_template(
                        instance_type, preemptible=preemptible, backoff=backoff * 2
                    )
                else:
                    raise
        else:
            # There must be exactly one template
            return templates[0]

    def _name_worker_launch_template(
        self, instance_type: str, preemptible: bool = False
    ) -> str:
        """
        Get the name we should use for the launch template with the given parameters.

        :param instance_type: Type of node to use in the template. May be overridden
                              by an ASG that uses the template.

        :param preemptible: When the node comes up, does it think it is a spot instance?
        """

        # The name has the cluster name in it
        lt_name = f"{self.clusterName}-lt-{instance_type}"
        if preemptible:
            lt_name += "-spot"

        return lt_name

    def _create_worker_launch_template(
        self, instance_type: str, preemptible: bool = False
    ) -> str:
        """
        Create the launch template for launching worker instances for the cluster.

        :param instance_type: Type of node to use in the template. May be overridden
                              by an ASG that uses the template.

        :param preemptible: When the node comes up, does it think it is a spot instance?

        :return: The ID of the template created.
        """

        # TODO: If we already have one like this, set its storage and/or remake it.

        assert self._leaderPrivateIP
        type_info = E2Instances[instance_type]
        rootVolSize = self._nodeStorageOverrides.get(instance_type, self._nodeStorage)
        bdms = self._getBoto3BlockDeviceMappings(type_info, rootVolSize=rootVolSize)

        keyPath = self._sseKey if self._sseKey else None
        userData = self._getIgnitionUserData(
            "worker", keyPath, preemptible, self._architecture
        )

        lt_name = self._name_worker_launch_template(
            instance_type, preemptible=preemptible
        )

        # But really we find it by tag
        tags = dict(self._tags)
        tags[_TAG_KEY_TOIL_NODE_TYPE] = "worker"

        return create_launch_template(
            self.aws.client(self._region, "ec2"),
            template_name=lt_name,
            image_id=self._discoverAMI(),
            key_name=self._keyName,
            security_group_ids=self._getSecurityGroupIDs(),
            instance_type=instance_type,
            user_data=userData,
            block_device_map=bdms,
            instance_profile_arn=self._leaderProfileArn,
            tags=tags,
        )

    @awsRetry
    def _getAutoScalingGroupNames(self) -> list[str]:
        """
        Find all auto-scaling groups associated with the cluster.

        Returns a list of ASG IDs. ASG IDs and ASG names are the same things.
        """

        # Grab the connection we need to use for this operation.
        autoscaling: AutoScalingClient = self.aws.client(self._region, "autoscaling")

        # AWS won't filter ASGs server-side for us in describe_auto_scaling_groups.
        # So we search instances of applied tags for the ASGs they are on.
        # The ASGs tagged with our cluster are our ASGs.
        # The filtering is on different fields of the tag object itself.
        filters: list[FilterTypeDef] = [
            {"Name": "key", "Values": [_TAG_KEY_TOIL_CLUSTER_NAME]},
            {"Name": "value", "Values": [self.clusterName]},
        ]

        matchedASGs = []
        # Get the first page with no NextToken
        response = autoscaling.describe_tags(Filters=filters)
        while True:
            # Process the current page
            matchedASGs += [
                item["ResourceId"]
                for item in response.get("Tags", [])
                if item["Key"] == _TAG_KEY_TOIL_CLUSTER_NAME
                and item["Value"] == self.clusterName
            ]
            if "NextToken" in response:
                # There are more pages. Get the next one, supplying the token.
                response = autoscaling.describe_tags(
                    Filters=filters, NextToken=response["NextToken"]
                )
            else:
                # No more pages
                break

        for name in matchedASGs:
            # Double check to make sure we definitely aren't finding non-Toil
            # things
            assert name.startswith("toil-")

        return matchedASGs

    def _createWorkerAutoScalingGroup(
        self,
        launch_template_ids: dict[str, str],
        instance_types: Collection[str],
        min_size: int,
        max_size: int,
        spot_bid: float | None = None,
    ) -> str:
        """
        Create an autoscaling group.

        :param launch_template_ids: ID of the launch template to use for
               each instance type name.
        :param instance_types: Names of instance types to use. Must have
               at least one. Needed here to calculate the ephemeral storage
               provided. The instance type used to create the launch template
               must be present, for correct storage space calculation.
        :param min_size: Minimum number of instances to scale to.
        :param max_size: Maximum number of instances to scale to.
        :param spot_bid: Make this a spot ASG with the given bid.

        :return: the unique autoscaling group name.

        TODO: allow overriding launch template and pooling.
        """

        assert self._leaderPrivateIP

        assert len(instance_types) >= 1

        # Find the minimum storage any instance in the group will provide.
        # For each, we look at the root volume size we would assign it if it were the type used to make the template.
        # TODO: Work out how to apply each instance type's root volume size override independently when they're all in a pool.
        storage_gigs = []
        for instance_type in instance_types:
            spec = E2Instances[instance_type]
            spec_gigs = spec.disks * spec.disk_capacity
            rootVolSize = self._nodeStorageOverrides.get(
                instance_type, self._nodeStorage
            )
            storage_gigs.append(
                max(rootVolSize - _STORAGE_ROOT_OVERHEAD_GIGS, spec_gigs)
            )
        # Get the min storage we expect to see, but not less than 0.
        min_gigs = max(min(storage_gigs), 0)

        # Make tags. These are just for the ASG, not for the node.
        # If are a Kubernetes cluster, this includes the tag for membership.
        tags = dict(self._tags)

        # We tag the ASG with the Toil type, although nothing cares.
        tags[_TAG_KEY_TOIL_NODE_TYPE] = "worker"

        if self.clusterType == "kubernetes":
            # We also need to tag it with Kubernetes autoscaler info (empty tags)
            tags["k8s.io/cluster-autoscaler/" + self.clusterName] = ""
            assert self.clusterName != "enabled"
            tags["k8s.io/cluster-autoscaler/enabled"] = ""
            tags[
                "k8s.io/cluster-autoscaler/node-template/resources/ephemeral-storage"
            ] = f"{min_gigs}G"

        # Now we need to make up a unique name
        # TODO: can we make this more semantic without risking collisions? Maybe count up in memory?
        asg_name = "toil-" + str(uuid.uuid4())

        create_auto_scaling_group(
            self.aws.client(self._region, "autoscaling"),
            asg_name=asg_name,
            launch_template_ids=launch_template_ids,
            vpc_subnets=self._get_worker_subnets(),
            min_size=min_size,
            max_size=max_size,
            instance_types=instance_types,
            spot_bid=spot_bid,
            tags=tags,
        )

        return asg_name

    def _boto2_pager(self, requestor_callable: Callable[[...], Any], result_attribute_name: str) -> Iterable[dict[str, Any]]:  # type: ignore[misc]
        """
        Yield all the results from calling the given Boto 2 method and paging
        through all the results using the "marker" field. Results are to be
        found in the field with the given name in the AWS responses.
        """
        marker = None
        while True:
            result = requestor_callable(marker=marker)  # type: ignore[call-arg]
            yield from getattr(result, result_attribute_name)
            if result.is_truncated == "true":
                marker = result.marker
            else:
                break

    @awsRetry
    def _getRoleNames(self) -> list[str]:
        """
        Get all the IAM roles belonging to the cluster, as names.
        """

        results = []
        boto3_iam = self.aws.client(self._region, "iam")
        for result in boto3_pager(boto3_iam.list_roles, "Roles"):
            # For each Boto2 role object
            # Grab out the name
            result2 = cast("RoleTypeDef", result)
            name = result2["RoleName"]
            if self._is_our_namespaced_name(name):
                # If it looks like ours, it is ours.
                results.append(name)
        return results

    @awsRetry
    def _getInstanceProfileNames(self) -> list[str]:
        """
        Get all the instance profiles belonging to the cluster, as names.
        """

        results = []
        boto3_iam = self.aws.client(self._region, "iam")
        for result in boto3_pager(boto3_iam.list_instance_profiles, "InstanceProfiles"):
            # Grab out the name
            result2 = cast("InstanceProfileTypeDef", result)
            name = result2["InstanceProfileName"]
            if self._is_our_namespaced_name(name):
                # If it looks like ours, it is ours.
                results.append(name)
        return results

    @awsRetry
    def _getRoleInstanceProfileNames(self, role_name: str) -> list[str]:
        """
        Get all the instance profiles with the IAM role with the given name.

        Returns instance profile names.
        """

        # Grab the connection we need to use for this operation.
        boto3_iam: IAMClient = self.aws.client(self._region, "iam")

        return [
            item["InstanceProfileName"]
            for item in boto3_pager(
                boto3_iam.list_instance_profiles_for_role,
                "InstanceProfiles",
                RoleName=role_name,
            )
        ]

    @awsRetry
    def _getRolePolicyArns(self, role_name: str) -> list[str]:
        """
        Get all the policies attached to the IAM role with the given name.

        These do not include inline policies on the role.

        Returns policy ARNs.
        """

        # Grab the connection we need to use for this operation.
        boto3_iam: IAMClient = self.aws.client(self._region, "iam")

        # TODO: we don't currently use attached policies.

        return [
            item["PolicyArn"]
            for item in boto3_pager(
                boto3_iam.list_attached_role_policies,
                "AttachedPolicies",
                RoleName=role_name,
            )
        ]

    @awsRetry
    def _getRoleInlinePolicyNames(self, role_name: str) -> list[str]:
        """
        Get all the policies inline in the given IAM role.
        Returns policy names.
        """

        # Grab the connection we need to use for this operation.
        boto3_iam: IAMClient = self.aws.client(self._region, "iam")

        return list(
            boto3_pager(boto3_iam.list_role_policies, "PolicyNames", RoleName=role_name)
        )

    def full_policy(self, resource: str) -> dict[str, Any]:
        """
        Produce a dict describing the JSON form of a full-access-granting AWS
        IAM policy for the service with the given name (e.g. 's3').
        """
        return dict(
            Version="2012-10-17",
            Statement=[dict(Effect="Allow", Resource="*", Action=f"{resource}:*")],
        )

    def kubernetes_policy(self) -> dict[str, Any]:
        """
        Get the Kubernetes policy grants not provided by the full grants on EC2
        and IAM. See
        <https://github.com/DataBiosphere/toil/wiki/Manual-Autoscaling-Kubernetes-Setup#leader-policy>
        and
        <https://github.com/DataBiosphere/toil/wiki/Manual-Autoscaling-Kubernetes-Setup#worker-policy>.

        These are mostly needed to support Kubernetes' AWS CloudProvider, and
        some are for the Kubernetes Cluster Autoscaler's AWS integration.

        Some of these are really only needed on the leader.
        """

        return dict(
            Version="2012-10-17",
            Statement=[
                dict(
                    Effect="Allow",
                    Resource="*",
                    Action=[
                        "ecr:GetAuthorizationToken",
                        "ecr:BatchCheckLayerAvailability",
                        "ecr:GetDownloadUrlForLayer",
                        "ecr:GetRepositoryPolicy",
                        "ecr:DescribeRepositories",
                        "ecr:ListImages",
                        "ecr:BatchGetImage",
                        "autoscaling:DescribeAutoScalingGroups",
                        "autoscaling:DescribeAutoScalingInstances",
                        "autoscaling:DescribeLaunchConfigurations",
                        "autoscaling:DescribeTags",
                        "autoscaling:SetDesiredCapacity",
                        "autoscaling:TerminateInstanceInAutoScalingGroup",
                        "elasticloadbalancing:AddTags",
                        "elasticloadbalancing:ApplySecurityGroupsToLoadBalancer",
                        "elasticloadbalancing:AttachLoadBalancerToSubnets",
                        "elasticloadbalancing:ConfigureHealthCheck",
                        "elasticloadbalancing:CreateListener",
                        "elasticloadbalancing:CreateLoadBalancer",
                        "elasticloadbalancing:CreateLoadBalancerListeners",
                        "elasticloadbalancing:CreateLoadBalancerPolicy",
                        "elasticloadbalancing:CreateTargetGroup",
                        "elasticloadbalancing:DeleteListener",
                        "elasticloadbalancing:DeleteLoadBalancer",
                        "elasticloadbalancing:DeleteLoadBalancerListeners",
                        "elasticloadbalancing:DeleteTargetGroup",
                        "elasticloadbalancing:DeregisterInstancesFromLoadBalancer",
                        "elasticloadbalancing:DeregisterTargets",
                        "elasticloadbalancing:DescribeListeners",
                        "elasticloadbalancing:DescribeLoadBalancerAttributes",
                        "elasticloadbalancing:DescribeLoadBalancerPolicies",
                        "elasticloadbalancing:DescribeLoadBalancers",
                        "elasticloadbalancing:DescribeTargetGroups",
                        "elasticloadbalancing:DescribeTargetHealth",
                        "elasticloadbalancing:DetachLoadBalancerFromSubnets",
                        "elasticloadbalancing:ModifyListener",
                        "elasticloadbalancing:ModifyLoadBalancerAttributes",
                        "elasticloadbalancing:ModifyTargetGroup",
                        "elasticloadbalancing:RegisterInstancesWithLoadBalancer",
                        "elasticloadbalancing:RegisterTargets",
                        "elasticloadbalancing:SetLoadBalancerPoliciesForBackendServer",
                        "elasticloadbalancing:SetLoadBalancerPoliciesOfListener",
                        "kms:DescribeKey",
                    ],
                )
            ],
        )

    def _setup_iam_ec2_role(
        self, local_role_name: str, policies: dict[str, Any]
    ) -> str:
        """
        Create an IAM role with the given policies, using the given name in
        addition to the cluster name, and return its full name.
        """
        ec2_role_policy_document = json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {"Service": ["ec2.amazonaws.com"]},
                        "Action": ["sts:AssumeRole"],
                    }
                ],
            }
        )
        return create_iam_role(
            role_name=self._namespace_name(local_role_name),
            assume_role_policy_document=ec2_role_policy_document,
            policies=policies,
            region=self._region,
        )

    @awsRetry
    def _createProfileArn(self) -> str:
        """
        Create an IAM role and instance profile that grants needed permissions
        for cluster leaders and workers. Naming is specific to the cluster.

        Returns its ARN.
        """

        # Grab the connection we need to use for this operation.
        boto3_iam: IAMClient = self.aws.client(self._region, "iam")

        policy = dict(
            iam_full=self.full_policy("iam"),
            ec2_full=self.full_policy("ec2"),
            s3_full=self.full_policy("s3"),
            sbd_full=self.full_policy("sdb"),
        )
        if self.clusterType == "kubernetes":
            # We also need autoscaling groups and some other stuff for AWS-Kubernetes integrations.
            # TODO: We use one merged policy for leader and worker, but we could be more specific.
            policy["kubernetes_merged"] = self.kubernetes_policy()
        iamRoleName = self._setup_iam_ec2_role(_INSTANCE_PROFILE_ROLE_NAME, policy)

        try:
            profile_result = boto3_iam.get_instance_profile(
                InstanceProfileName=iamRoleName
            )
            profile: InstanceProfileTypeDef = profile_result["InstanceProfile"]
            logger.debug("Have preexisting instance profile: %s", profile)
        except boto3_iam.exceptions.NoSuchEntityException:
            profile_result = boto3_iam.create_instance_profile(
                InstanceProfileName=iamRoleName
            )
            profile = profile_result["InstanceProfile"]
            logger.debug("Created new instance profile: %s", profile)
        else:
            profile = profile_result["InstanceProfile"]

        profile_arn: str = profile["Arn"]

        # Now we have the profile ARN, but we want to make sure it really is
        # visible by name in a different session.
        wait_until_instance_profile_arn_exists(profile_arn)

        if len(profile["Roles"]) > 1:
            # This is too many roles. We probably grabbed something we should
            # not have by mistake, and this is some important profile for
            # something else.
            raise RuntimeError(
                f"Did not expect instance profile {profile_arn} to contain "
                f"more than one role; is it really a Toil-managed profile?"
            )
        elif len(profile["Roles"]) == 1:
            if profile["Roles"][0]["RoleName"] == iamRoleName:
                return profile_arn
            else:
                # Drop this wrong role and use the fallback code for 0 roles
                boto3_iam.remove_role_from_instance_profile(
                    InstanceProfileName=iamRoleName,
                    RoleName=profile["Roles"][0]["RoleName"],
                )

        # If we get here, we had 0 roles on the profile, or we had 1 but we removed it.
        for attempt in old_retry(predicate=lambda err: get_error_status(err) == 404):
            with attempt:
                # Put the IAM role on the profile
                boto3_iam.add_role_to_instance_profile(
                    InstanceProfileName=profile["InstanceProfileName"],
                    RoleName=iamRoleName,
                )
                logger.debug("Associated role %s with profile", iamRoleName)

        return profile_arn
