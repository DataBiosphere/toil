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
import logging
import sys
from typing import Optional, Union, cast

from toil.lib.aws import session
from toil.lib.misc import printq
from toil.lib.retry import retry

if sys.version_info >= (3, 8):
    from typing import Literal
else:
    from typing_extensions import Literal

try:
    from boto.exception import BotoServerError
    from mypy_boto3_s3 import S3Client, S3ServiceResource
    from mypy_boto3_s3.literals import BucketLocationConstraintType
    from mypy_boto3_s3.service_resource import Bucket
    from mypy_boto3_sdb import SimpleDBClient
    from mypy_boto3_iam import IAMClient, IAMServiceResource
except ImportError:
    BotoServerError = None  # type: ignore
    # AWS/boto extra is not installed

logger = logging.getLogger(__name__)


@retry(errors=[BotoServerError])
def delete_iam_role(
    role_name: str, region: Optional[str] = None, quiet: bool = True
) -> None:
    from boto.iam.connection import IAMConnection
    # TODO: the Boto3 type hints are a bit oversealous here; they want hundreds
    # of overloads of the client-getting methods to exist based on the literal
    # string passed in, to return exactly the right kind of client or resource.
    # So we end up having to wrap all the calls in casts, which kind of defeats
    # the point of a nice fluent method you can call with the name of the thing
    # you want; we should have been calling iam_client() and so on all along if
    # we wanted MyPy to be able to understand us. So at some point we should
    # consider revising our API here to be less annoying to explain to the type
    # checker.
    iam_client = cast(IAMClient, session.client('iam', region_name=region))
    iam_resource = cast(IAMServiceResource, session.resource('iam', region_name=region))
    boto_iam_connection = IAMConnection()
    role = iam_resource.Role(role_name)
    # normal policies
    for attached_policy in role.attached_policies.all():
        printq(f'Now dissociating policy: {attached_policy.policy_name} from role {role.name}', quiet)
        role.detach_policy(PolicyArn=attached_policy.arn)
    # inline policies
    for inline_policy in role.policies.all():
        printq(f'Deleting inline policy: {inline_policy.policy_name} from role {role.name}', quiet)
        # couldn't find an easy way to remove inline policies with boto3; use boto
        boto_iam_connection.delete_role_policy(role.name, inline_policy.policy_name)
    iam_client.delete_role(RoleName=role_name)
    printq(f'Role {role_name} successfully deleted.', quiet)


@retry(errors=[BotoServerError])
def delete_iam_instance_profile(
    instance_profile_name: str, region: Optional[str] = None, quiet: bool = True
) -> None:
    iam_resource = cast(IAMServiceResource, session.resource("iam", region_name=region))
    instance_profile = iam_resource.InstanceProfile(instance_profile_name)
    if instance_profile.roles is not None:
        # The type stubs think this is somehow just one role, when it's a
        # collection. See <https://github.com/vemel/mypy_boto3_builder/issues/122>.
        for role in instance_profile.roles: #type: ignore
            printq(f'Now dissociating role: {role.name} from instance profile {instance_profile_name}', quiet)
            instance_profile.remove_role(RoleName=role.name)
    instance_profile.delete()
    printq(f'Instance profile "{instance_profile_name}" successfully deleted.', quiet)


@retry(errors=[BotoServerError])
def delete_sdb_domain(
    sdb_domain_name: str, region: Optional[str] = None, quiet: bool = True
) -> None:
    sdb_client = cast(SimpleDBClient, session.client("sdb", region_name=region))
    sdb_client.delete_domain(DomainName=sdb_domain_name)
    printq(f'SBD Domain: "{sdb_domain_name}" successfully deleted.', quiet)


@retry(errors=[BotoServerError])
def delete_s3_bucket(bucket: str, region: Optional[str], quiet: bool = True) -> None:
    printq(f'Deleting s3 bucket in region "{region}": {bucket}', quiet)
    s3_client = cast(S3Client, session.client('s3', region_name=region))
    s3_resource = cast(S3ServiceResource, session.resource('s3', region_name=region))

    paginator = s3_client.get_paginator('list_object_versions')
    try:
        for response in paginator.paginate(Bucket=bucket):
            versions = response.get('Versions', []) + response.get('DeleteMarkers', [])
            for version in versions:
                printq(f"    Deleting {version['Key']} version {version['VersionId']}", quiet)
                s3_client.delete_object(Bucket=bucket, Key=version['Key'], VersionId=version['VersionId'])
        s3_resource.Bucket(bucket).delete()
        printq(f'\n * Deleted s3 bucket successfully: {bucket}\n\n', quiet)
    except s3_client.exceptions.NoSuchBucket:
        printq(f'\n * S3 bucket no longer exists: {bucket}\n\n', quiet)


def create_s3_bucket(
    s3_session: "S3ServiceResource",
    bucket_name: str,
    region: Union["BucketLocationConstraintType", Literal["us-east-1"]],
) -> "Bucket":
    """
    Create an AWS S3 bucket, using the given Boto3 S3 session, with the
    given name, in the given region.

    Supports the us-east-1 region, where bucket creation is special.

    *ALL* S3 bucket creation should use this function.
    """
    logger.debug("Creating bucket '%s' in region %s.", bucket_name, region)
    if region == "us-east-1":  # see https://github.com/boto/boto3/issues/125
        bucket = s3_session.create_bucket(Bucket=bucket_name)
    else:
        bucket = s3_session.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": region},
        )
    return bucket
