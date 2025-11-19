import json
import logging
import os
import time
import urllib.request
from collections.abc import Iterator
from typing import Optional, cast
from urllib.error import HTTPError, URLError

from botocore.client import BaseClient
from botocore.exceptions import ClientError, EndpointConnectionError

from toil.lib.retry import retry

logger = logging.getLogger(__name__)


class ReleaseFeedUnavailableError(RuntimeError):
    """Raised when a Flatcar releases can't be located."""

    pass


@retry(errors=[ReleaseFeedUnavailableError])
def get_flatcar_ami(ec2_client: BaseClient, architecture: str = "amd64") -> str:
    """
    Retrieve the flatcar AMI image to use as the base for all Toil autoscaling instances.

    AMI must be available to the user on AWS (attempting to launch will return a 403 otherwise).

    Priority is:
      1. User specified AMI via TOIL_AWS_AMI
      2. Official AMI from stable.release.flatcar-linux.net
      3. Search the AWS Marketplace

    :raises ReleaseFeedUnavailableError: if all of these sources fail.

    :param ec2_client: Boto3 EC2 Client
    :param architecture: The architecture type for the new AWS machine. Can be either amd64 or arm64
    """
    # Take a user override
    ami = os.environ.get("TOIL_AWS_AMI")
    if not ami:
        logger.debug(
            "No AMI found in TOIL_AWS_AMI; checking stable Flatcar release feed"
        )
        ami = feed_flatcar_ami_release(
            ec2_client=ec2_client, architecture=architecture, source="stable"
        )
    if not ami:
        logger.warning(
            "No available AMI found in Flatcar release feed; checking marketplace"
        )
        ami = aws_marketplace_flatcar_ami_search(
            ec2_client=ec2_client, architecture=architecture
        )
    if not ami:
        logger.debug(
            "No AMI found in Toil project feed; checking beta Flatcar release feed"
        )
        ami = feed_flatcar_ami_release(
            ec2_client=ec2_client, architecture=architecture, source="beta"
        )
    if not ami:
        logger.debug(
            "No AMI found in beta Flatcar release feed; checking archived Flatcar release feed"
        )
        ami = feed_flatcar_ami_release(
            ec2_client=ec2_client, architecture=architecture, source="archive"
        )
    if not ami:
        logger.critical("No available Flatcar AMI in any source!")
        raise ReleaseFeedUnavailableError(
            f"Unable to fetch the latest flatcar image. Upload "
            f"https://stable.release.flatcar-linux.net/{architecture}-usr/current/flatcar_production_ami_image.bin.bz2 "
            f"to AWS as am AMI and set TOIL_AWS_AMI in the environment to its AMI ID."
        )
    logger.info("Selected Flatcar AMI: %s", ami)
    return ami


def _fetch_flatcar_feed(architecture: str = "amd64", source: str = "stable") -> bytes:
    """
    Get the binary data of the Flatcar release feed for the given architecture.

    :param source: can be set to a Flatcar release channel ('stable', 'beta',
           or 'alpha'), 'archive' to check the Internet Archive for a feed,
           and 'toil' to check if the Toil project has put up a feed.

    :raises HTTPError: if the feed cannot be fetched.
    """

    # We have a few places we know to get the feed from.
    JSON_FEED_URL = {
        "stable": f"https://stable.release.flatcar-linux.net/{architecture}-usr/current/flatcar_production_ami_all.json",
        "beta": f"https://beta.release.flatcar-linux.net/{architecture}-usr/current/flatcar_production_ami_all.json",
        # "alpha": f"https://alpha.release.flatcar-linux.net/{architecture}-usr/current/flatcar_production_ami_all.json",
        "archive": f"https://web.archive.org/web/20220625112618if_/https://stable.release.flatcar-linux.net/{architecture}-usr/current/flatcar_production_ami_all.json",
    }[source]
    return cast(bytes, urllib.request.urlopen(JSON_FEED_URL).read())


def flatcar_release_feed_ami(
    region: str, architecture: str = "amd64", source: str = "stable"
) -> str | None:
    """
    Yield AMI IDs for the given architecture from the Flatcar release feed.

    :param source: can be set to a Flatcar release channel ('stable', 'beta',
           or 'alpha'), 'archive' to check the Internet Archive for a feed,
           and 'toil' to check if the Toil project has put up a feed.

    Retries if the release feed cannot be fetched. If the release feed has a
    permanent error, yields nothing. If some entries in the release feed are
    unparseable, yields the others.
    """

    # If we get non-JSON content we want to retry.
    MAX_TRIES = 3

    try_number = 0
    while try_number < MAX_TRIES:
        if try_number != 0:
            time.sleep(1)
        try:
            feed = json.loads(_fetch_flatcar_feed(architecture, source))
            break
        except HTTPError:
            # Flatcar servers did not return the feed
            logger.exception(f"Could not retrieve {source} Flatcar release feed JSON")
            # This is probably a permanent error, or at least unlikely to go away immediately.
            return None
        except json.JSONDecodeError:
            # Feed is not JSON
            logger.exception(f"Could not decode {source} Flatcar release feed JSON")
            # Try again
            try_number += 1
            continue
        except URLError:
            # Could be a connection timeout
            logger.exception(f"Failed to retrieve {source} Flatcar release feed JSON")
            # Try again
            try_number += 1
            continue
    if try_number == MAX_TRIES:
        # We could not get the JSON
        logger.error(f"Could not get a readable {source} Flatcar release feed JSON")
        # Bail on this method
        return None

    for ami_record in feed.get("amis", []):
        # Scan the list of regions
        if ami_record.get("name") == region:
            return str(ami_record.get("hvm")) if ami_record.get("hvm") else None
    # We didn't find our region
    logger.warning(
        f"Flatcar {source} release feed does not have an image for region {region}"
    )


def feed_flatcar_ami_release(
    ec2_client: BaseClient, architecture: str = "amd64", source: str = "stable"
) -> str | None:
    """
    Check a Flatcar release feed for the latest flatcar AMI.

    Verify it's on AWS.

    Does not raise exceptions.

    :param ec2_client: Boto3 EC2 Client
    :param architecture: The architecture type for the new AWS machine. Can be either amd64 or arm64
    :param source: can be set to a Flatcar release channel ('stable', 'beta',
           or 'alpha'), 'archive' to check the Internet Archive for a feed,
           and 'toil' to check if the Toil project has put up a feed.
    """
    # Flatcar images only live for 9 months.
    # Rather than hardcode a list of AMIs by region that will die, we use
    # their JSON feed of the current ones.

    region = ec2_client._client_config.region_name  # type: ignore

    if ami := flatcar_release_feed_ami(region, architecture, source):
        # verify it exists on AWS
        try:
            response = ec2_client.describe_images(Filters=[{"Name": "image-id", "Values": [ami]}])  # type: ignore
            if (
                len(response["Images"]) == 1
                and response["Images"][0]["State"] == "available"
            ):
                return ami
            else:
                logger.warning(
                    f"Flatcar release feed suggests image {ami} which does not exist on AWS in {region}"
                )
        except (ClientError, EndpointConnectionError):
            # Sometimes we get back nonsense like:
            # botocore.exceptions.ClientError: An error occurred (AuthFailure) when calling the DescribeImages operation: AWS was not able to validate the provided access credentials
            # Don't hold that against the AMI.
            logger.exception(
                f"Unable to check if AMI {ami} exists on AWS in {region}; assuming it does"
            )
            return ami
    # We didn't find it
    logger.warning(
        f"Flatcar release feed does not have an image for region {region} that exists on AWS"
    )


def aws_marketplace_flatcar_ami_search(
    ec2_client: BaseClient, architecture: str = "amd64"
) -> str | None:
    """
    Query AWS for all AMI names matching ``Flatcar-stable-*`` and return the most recent one.

    Does not raise exceptions.

    :returns: An AMI name, or None if no matching AMI was found or we could not talk to AWS.
    """

    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ec2.html#EC2.Client.describe_images
    # Possible arch choices on AWS: 'i386'|'x86_64'|'arm64'|'x86_64_mac'
    architecture_mapping = {"amd64": "x86_64", "arm64": "arm64"}
    try:
        response = ec2_client.describe_images(  # type: ignore[attr-defined]
            Owners=["aws-marketplace"],
            Filters=[{"Name": "name", "Values": ["Flatcar-stable-*"]}],
        )
    except (ClientError, EndpointConnectionError):
        logger.exception("Unable to search AWS marketplace")
        return None
    latest: dict[str, str] = {"CreationDate": "0lder than atoms."}
    for image in response["Images"]:
        if (
            image["Architecture"] == architecture_mapping[architecture]
            and image["State"] == "available"
        ):
            if image["CreationDate"] > latest["CreationDate"]:
                latest = image
    return latest.get("ImageId", None)
