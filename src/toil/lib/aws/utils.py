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
import errno
import logging
import os
import socket
from collections.abc import Iterable, Iterator
from typing import TYPE_CHECKING, Any, Callable, ContextManager, Optional, cast
from urllib.parse import ParseResult

from toil.lib.aws import AWSRegionName, AWSServerErrors, session
from toil.lib.misc import printq
from toil.lib.retry import (
    DEFAULT_DELAYS,
    DEFAULT_TIMEOUT,
    get_error_code,
    get_error_status,
    old_retry,
    retry,
)

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3ServiceResource
    from mypy_boto3_s3.service_resource import Bucket
    from mypy_boto3_s3.service_resource import Object as S3Object
    from mypy_boto3_sdb.type_defs import AttributeTypeDef

try:
    from botocore.exceptions import ClientError, EndpointConnectionError
except ImportError:
    ClientError = None  # type: ignore
    EndpointConnectionError = None  # type: ignore
    # AWS/boto extra is not installed

logger = logging.getLogger(__name__)

# These are error codes we expect from AWS if we are making requests too fast.
# https://github.com/boto/botocore/blob/49f87350d54f55b687969ec8bf204df785975077/botocore/retries/standard.py#L316
THROTTLED_ERROR_CODES = [
    "Throttling",
    "ThrottlingException",
    "ThrottledException",
    "RequestThrottledException",
    "TooManyRequestsException",
    "ProvisionedThroughputExceededException",
    "TransactionInProgressException",
    "RequestLimitExceeded",
    "BandwidthLimitExceeded",
    "LimitExceededException",
    "RequestThrottled",
    "SlowDown",
    "PriorRequestNotComplete",
    "EC2ThrottledException",
]


@retry(errors=[AWSServerErrors])
def delete_sdb_domain(
    sdb_domain_name: str, region: Optional[str] = None, quiet: bool = True
) -> None:
    sdb_client = session.client("sdb", region_name=region)
    sdb_client.delete_domain(DomainName=sdb_domain_name)
    printq(f'SBD Domain: "{sdb_domain_name}" successfully deleted.', quiet)


def connection_reset(e: Exception) -> bool:
    """
    Return true if an error is a connection reset error.
    """
    # For some reason we get 'error: [Errno 104] Connection reset by peer' where the
    # English description suggests that errno is 54 (ECONNRESET) while the actual
    # errno is listed as 104. To be safe, we check for both:
    return isinstance(e, socket.error) and e.errno in (errno.ECONNRESET, 104)


def connection_error(e: Exception) -> bool:
    """
    Return True if an error represents a failure to make a network connection.
    """
    return connection_reset(e) or isinstance(e, EndpointConnectionError)


# TODO: Replace with: @retry and ErrorCondition
def retryable_s3_errors(e: Exception) -> bool:
    """
    Return true if this is an error from S3 that looks like we ought to retry our request.
    """
    return (
        connection_error(e)
        or (isinstance(e, ClientError) and get_error_status(e) in (429, 500))
        or (isinstance(e, ClientError) and get_error_code(e) in THROTTLED_ERROR_CODES)
        # boto3 errors
        or (isinstance(e, ClientError) and get_error_code(e) in THROTTLED_ERROR_CODES)
        or (isinstance(e, ClientError) and "BucketNotEmpty" in str(e))
        or (
            isinstance(e, ClientError)
            and e.response.get("ResponseMetadata", {}).get("HTTPStatusCode") == 409
            and "try again" in str(e)
        )
        or (
            isinstance(e, ClientError)
            and e.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            in (404, 429, 500, 502, 503, 504)
        )
    )


def retry_s3(
    delays: Iterable[float] = DEFAULT_DELAYS,
    timeout: float = DEFAULT_TIMEOUT,
    predicate: Callable[[Exception], bool] = retryable_s3_errors,
) -> Iterator[ContextManager[None]]:
    """
    Retry iterator of context managers specifically for S3 operations.
    """
    return old_retry(delays=delays, timeout=timeout, predicate=predicate)


@retry(errors=[AWSServerErrors])
def delete_s3_bucket(
    s3_resource: "S3ServiceResource", bucket: str, quiet: bool = True
) -> None:
    """
    Delete the given S3 bucket.
    """
    printq(f"Deleting s3 bucket: {bucket}", quiet)

    paginator = s3_resource.meta.client.get_paginator("list_object_versions")
    try:
        for response in paginator.paginate(Bucket=bucket):
            # Versions and delete markers can both go in here to be deleted.
            # They both have Key and VersionId, but there's no shared base type
            # defined for them in the stubs to express that. See
            # <https://github.com/vemel/mypy_boto3_builder/issues/123>. So we
            # have to do gymnastics to get them into the same list.
            to_delete: list[dict[str, Any]] = cast(
                list[dict[str, Any]], response.get("Versions", [])
            ) + cast(list[dict[str, Any]], response.get("DeleteMarkers", []))
            for entry in to_delete:
                printq(
                    f"    Deleting {entry['Key']} version {entry['VersionId']}", quiet
                )
                s3_resource.meta.client.delete_object(
                    Bucket=bucket, Key=entry["Key"], VersionId=entry["VersionId"]
                )
        s3_resource.Bucket(bucket).delete()
        printq(f"\n * Deleted s3 bucket successfully: {bucket}\n\n", quiet)
    except s3_resource.meta.client.exceptions.NoSuchBucket:
        printq(f"\n * S3 bucket no longer exists: {bucket}\n\n", quiet)


def create_s3_bucket(
    s3_resource: "S3ServiceResource",
    bucket_name: str,
    region: AWSRegionName,
) -> "Bucket":
    """
    Create an AWS S3 bucket, using the given Boto3 S3 session, with the
    given name, in the given region.

    Supports the us-east-1 region, where bucket creation is special.

    *ALL* S3 bucket creation should use this function.
    """
    logger.info("Creating bucket '%s' in region %s.", bucket_name, region)
    if region == "us-east-1":  # see https://github.com/boto/boto3/issues/125
        bucket = s3_resource.create_bucket(Bucket=bucket_name)
    else:
        bucket = s3_resource.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": region},
        )
    return bucket


@retry(errors=[ClientError])
def enable_public_objects(bucket_name: str) -> None:
    """
    Enable a bucket to contain objects which are public.

    This adjusts the bucket's Public Access Block setting to not block all
    public access, and also adjusts the bucket's Object Ownership setting to a
    setting which enables object ACLs.

    Does *not* touch the *account*'s Public Access Block setting, which can
    also interfere here. That is probably best left to the account
    administrator.

    This configuration used to be the default, and is what most of Toil's code
    is written to expect, but it was changed so that new buckets default to the
    more restrictive setting
    <https://aws.amazon.com/about-aws/whats-new/2022/12/amazon-s3-automatically-enable-block-public-access-disable-access-control-lists-buckets-april-2023/>,
    with the expectation that people would write IAM policies for the buckets
    to allow public access if needed. Toil expects to be able to make arbitrary
    objects in arbitrary places public, and naming them all in an IAM policy
    would be a very awkward way to do it. So we restore the old behavior.
    """

    s3_client = session.client("s3")

    # Even though the new default is for public access to be prohibited, this
    # is implemented by adding new things attached to the bucket. If we remove
    # those things the bucket will default to the old defaults. See
    # <https://aws.amazon.com/blogs/aws/heads-up-amazon-s3-security-changes-are-coming-in-april-of-2023/>.

    # Stop blocking public access
    s3_client.delete_public_access_block(Bucket=bucket_name)

    # Stop using an ownership controls setting that prohibits ACLs.
    s3_client.delete_bucket_ownership_controls(Bucket=bucket_name)


class NoBucketLocationError(Exception):
    """
    Error to represent that we could not get a location for a bucket.
    """


def get_bucket_region(
    bucket_name: str,
    endpoint_url: Optional[str] = None,
    only_strategies: Optional[set[int]] = None,
) -> str:
    """
    Get the AWS region name associated with the given S3 bucket, or raise NoBucketLocationError.

    Does not log at info level or above when this does not work; failures are expected in some contexts.

    Takes an optional S3 API URL override.

    :param only_strategies: For testing, use only strategies with 1-based numbers in this set.
    """

    s3_client = session.client("s3", endpoint_url=endpoint_url)

    def attempt_get_bucket_location() -> Optional[str]:
        """
        Try and get the bucket location from the normal API call.
        """
        return s3_client.get_bucket_location(Bucket=bucket_name).get(
            "LocationConstraint", None
        )

    def attempt_get_bucket_location_from_us_east_1() -> Optional[str]:
        """
        Try and get the bucket location from the normal API call, but against us-east-1
        """
        # Sometimes we aren't allowed to GetBucketLocation. At least some of
        # the time, that's only true when we talk to whatever S3 API servers we
        # usually use, and we can get around this lack of permission by talking
        # to us-east-1 instead. We've been told that this is because us-east-1
        # is special and will answer the question when other regions won't.
        # See:
        # <https://ucsc-gi.slack.com/archives/C027D41M6UA/p1652819831740169?thread_ts=1652817377.594539&cid=C027D41M6UA>
        # It could also be because AWS open data buckets (which we tend to
        # encounter this problem for) tend to actually themselves be in
        # us-east-1.
        backup_s3_client = session.client("s3", region_name="us-east-1")
        return backup_s3_client.get_bucket_location(Bucket=bucket_name).get(
            "LocationConstraint", None
        )

    def attempt_head_bucket() -> Optional[str]:
        """
        Try and get the bucket location from calling HeadBucket and inspecting
        the headers.
        """
        # If that also doesn't work, we can try HEAD-ing the bucket and looking
        # for an 'x-amz-bucket-region' header on the response, which can tell
        # us where the bucket is. See
        # <https://github.com/aws/aws-sdk-cpp/issues/844#issuecomment-383747871>
        info = s3_client.head_bucket(Bucket=bucket_name)
        return info["ResponseMetadata"]["HTTPHeaders"]["x-amz-bucket-region"]

    # Compose a list of strategies we want to try in order, which may work.
    # None is an acceptable return type that actually means something.
    strategies: list[Callable[[], Optional[str]]] = []
    strategies.append(attempt_get_bucket_location)
    if not endpoint_url:
        # We should only try to talk to us-east-1 if we don't have a custom
        # URL.
        strategies.append(attempt_get_bucket_location_from_us_east_1)
    strategies.append(attempt_head_bucket)

    error_logs: list[tuple[int, str]] = []
    for attempt in retry_s3():
        with attempt:
            for i, strategy in enumerate(strategies):
                if only_strategies is not None and i + 1 not in only_strategies:
                    # We want to test running without this strategy.
                    continue
                try:
                    location = bucket_location_to_region(strategy())
                    logger.debug("Got bucket location from strategy %d", i + 1)
                    return location
                except ClientError as e:
                    if get_error_code(e) == "AccessDenied" and not endpoint_url:
                        logger.debug(
                            "Strategy %d to get bucket location did not work: %s",
                            i + 1,
                            e,
                        )
                        error_logs.append((i + 1, str(e)))
                        last_error: Exception = e
                        # We were blocked with this strategy. Move on to the
                        # next strategy which might work.
                        continue
                    else:
                        raise
                except KeyError as e:
                    # If we get a weird head response we will have a KeyError
                    logger.debug(
                        "Strategy %d to get bucket location did not work: %s", i + 1, e
                    )
                    error_logs.append((i + 1, str(e)))
                    last_error = e

    error_messages = []
    for rank, message in error_logs:
        error_messages.append(
            f"Strategy {rank} failed to get bucket location because: {message}"
        )
    # If we get here we ran out of attempts.
    raise NoBucketLocationError(
        "Could not get bucket location: " + "\n".join(error_messages)
    ) from last_error


def region_to_bucket_location(region: str) -> str:
    return "" if region == "us-east-1" else region


def bucket_location_to_region(location: Optional[str]) -> str:
    return "us-east-1" if location == "" or location is None else location


def get_object_for_url(url: ParseResult, existing: Optional[bool] = None) -> "S3Object":
    """
    Extracts a key (object) from a given parsed s3:// URL.

    If existing is true and the object does not exist, raises FileNotFoundError.

    :param bool existing: If True, key is expected to exist. If False, key is expected not to
            exists and it will be created. If None, the key will be created if it doesn't exist.
    """

    key_name = url.path[1:]
    bucket_name = url.netloc

    # Decide if we need to override Boto's built-in URL here.
    endpoint_url: Optional[str] = None
    host = os.environ.get("TOIL_S3_HOST", None)
    port = os.environ.get("TOIL_S3_PORT", None)
    protocol = "https"
    if os.environ.get("TOIL_S3_USE_SSL", True) == "False":
        protocol = "http"
    if host:
        endpoint_url = f"{protocol}://{host}" + f":{port}" if port else ""

    # TODO: OrdinaryCallingFormat equivalent in boto3?
    # if botoargs:
    #     botoargs['calling_format'] = boto.s3.connection.OrdinaryCallingFormat()

    try:
        # Get the bucket's region to avoid a redirect per request
        region = get_bucket_region(bucket_name, endpoint_url=endpoint_url)
        s3 = session.resource("s3", region_name=region, endpoint_url=endpoint_url)
    except NoBucketLocationError as e:
        # Probably don't have permission.
        # TODO: check if it is that
        logger.debug("Couldn't get bucket location: %s", e)
        logger.debug("Fall back to not specifying location")
        s3 = session.resource("s3", endpoint_url=endpoint_url)

    obj = s3.Object(bucket_name, key_name)
    objExists = True

    try:
        obj.load()
    except ClientError as e:
        if get_error_status(e) == 404:
            objExists = False
        else:
            raise
    if existing is True and not objExists:
        raise FileNotFoundError(
            f"Key '{key_name}' does not exist in bucket '{bucket_name}'."
        )
    elif existing is False and objExists:
        raise RuntimeError(f"Key '{key_name}' exists in bucket '{bucket_name}'.")

    if not objExists:
        obj.put()  # write an empty file
    return obj


@retry(errors=[AWSServerErrors])
def list_objects_for_url(url: ParseResult) -> list[str]:
    """
    Extracts a key (object) from a given parsed s3:// URL. The URL will be
    supplemented with a trailing slash if it is missing.
    """
    key_name = url.path[1:]
    bucket_name = url.netloc

    if key_name != "" and not key_name.endswith("/"):
        # Make sure to put the trailing slash on the key, or else we'll see
        # a prefix of just it.
        key_name = key_name + "/"

    # Decide if we need to override Boto's built-in URL here.
    # TODO: Deduplicate with get_object_for_url, or push down into session module
    endpoint_url: Optional[str] = None
    host = os.environ.get("TOIL_S3_HOST", None)
    port = os.environ.get("TOIL_S3_PORT", None)
    protocol = "https"
    if os.environ.get("TOIL_S3_USE_SSL", True) == "False":
        protocol = "http"
    if host:
        endpoint_url = f"{protocol}://{host}" + f":{port}" if port else ""

    client = session.client("s3", endpoint_url=endpoint_url)

    listing = []

    paginator = client.get_paginator("list_objects_v2")
    result = paginator.paginate(Bucket=bucket_name, Prefix=key_name, Delimiter="/")
    for page in result:
        if "CommonPrefixes" in page:
            for prefix_item in page["CommonPrefixes"]:
                listing.append(prefix_item["Prefix"][len(key_name) :])
        if "Contents" in page:
            for content_item in page["Contents"]:
                if content_item["Key"] == key_name:
                    # Ignore folder name itself
                    continue
                listing.append(content_item["Key"][len(key_name) :])

    logger.debug("Found in %s items: %s", url, listing)
    return listing


def flatten_tags(tags: dict[str, str]) -> list[dict[str, str]]:
    """
    Convert tags from a key to value dict into a list of 'Key': xxx, 'Value': xxx dicts.
    """
    return [{"Key": k, "Value": v} for k, v in tags.items()]


def boto3_pager(
    requestor_callable: Callable[..., Any], result_attribute_name: str, **kwargs: Any
) -> Iterable[Any]:
    """
    Yield all the results from calling the given Boto 3 method with the
    given keyword arguments, paging through the results using the Marker or
    NextToken, and fetching out and looping over the list in the response
    with the given attribute name.
    """

    # Recover the Boto3 client, and the name of the operation
    client = requestor_callable.__self__  # type: ignore[attr-defined]
    op_name = requestor_callable.__name__

    # grab a Boto 3 built-in paginator. See
    # <https://boto3.amazonaws.com/v1/documentation/api/latest/guide/paginators.html>
    paginator = client.get_paginator(op_name)

    for page in paginator.paginate(**kwargs):
        # Invoke it and go through the pages, yielding from them
        yield from page.get(result_attribute_name, [])


def get_item_from_attributes(attributes: list["AttributeTypeDef"], name: str) -> Any:
    """
    Given a list of attributes, find the attribute associated with the name and return its corresponding value.

    The `attribute_list` will be a list of TypedDict's (which boto3 SDB functions commonly return),
    where each TypedDict has a "Name" and "Value" key value pair.
    This function grabs the value out of the associated TypedDict.

    If the attribute with the name does not exist, the function will return None.

    :param attributes: list of attributes
    :param name: name of the attribute
    :return: value of the attribute
    """
    return next(
        (attribute["Value"] for attribute in attributes if attribute["Name"] == name),
        None,
    )
