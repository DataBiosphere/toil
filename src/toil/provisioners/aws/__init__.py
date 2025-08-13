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
import datetime
import logging
from collections import namedtuple
from operator import attrgetter
from statistics import mean, stdev
from typing import Optional, TYPE_CHECKING

from botocore.client import BaseClient

from toil.lib.aws import (
    get_aws_zone_from_boto,
    get_aws_zone_from_environment,
    get_aws_zone_from_environment_region,
    get_aws_zone_from_metadata,
)
from toil.lib.aws.utils import boto3_pager

logger = logging.getLogger(__name__)

ZoneTuple = namedtuple("ZoneTuple", ["name", "price_deviation"])

if TYPE_CHECKING:
    from mypy_boto3_ec2.type_defs import SpotPriceTypeDef


def get_aws_zone_from_spot_market(
    spotBid: Optional[float],
    nodeType: Optional[str],
    boto3_ec2: Optional[BaseClient],
    zone_options: Optional[list[str]],
) -> Optional[str]:
    """
    If a spot bid, node type, and Boto2 EC2 connection are specified, picks a
    zone where instances are easy to buy from the zones in the region of the
    Boto2 connection. These parameters must always be specified together, or
    not at all.

    In this case, zone_options can be used to restrict to a subset of the zones
    in the region.
    """
    if spotBid:
        # if spot bid is present, all the other parameters must be as well
        assert bool(spotBid) == bool(nodeType) == bool(boto3_ec2)
        # if the zone is unset and we are using the spot market, optimize our
        # choice based on the spot history

        if zone_options is None:
            # We can use all the zones in the region
            zone_options = [z.name for z in boto3_ec2.describe_availability_zones()]

        return optimize_spot_bid(
            boto3_ec2,
            instance_type=nodeType,
            spot_bid=float(spotBid),
            zone_options=zone_options,
        )
    else:
        return None


def get_best_aws_zone(
    spotBid: Optional[float] = None,
    nodeType: Optional[str] = None,
    boto3_ec2: Optional[BaseClient] = None,
    zone_options: Optional[list[str]] = None,
) -> Optional[str]:
    """
    Get the right AWS zone to use.

    Reports the TOIL_AWS_ZONE environment variable if set.

    Otherwise, if we are running on EC2 or ECS, reports the zone we are running
    in.

    Otherwise, if a spot bid, node type, and Boto2 EC2 connection are
    specified, picks a zone where instances are easy to buy from the zones in
    the region of the Boto2 connection. These parameters must always be
    specified together, or not at all.

    In this case, zone_options can be used to restrict to a subset of the zones
    in the region.

    Otherwise, if we have the TOIL_AWS_REGION variable set, chooses a zone in
    that region.

    Finally, if a default region is configured in Boto 2, chooses a zone in
    that region.

    Returns None if no method can produce a zone to use.
    """
    return (
        get_aws_zone_from_environment()
        or get_aws_zone_from_metadata()
        or get_aws_zone_from_spot_market(spotBid, nodeType, boto3_ec2, zone_options)
        or get_aws_zone_from_environment_region()
        or get_aws_zone_from_boto()
    )


def choose_spot_zone(
    zones: list[str],
    bid: float,
    spot_history: list["SpotPriceTypeDef"],
) -> str:
    """
    Returns the zone to put the spot request based on, in order of priority:

       1) zones with prices currently under the bid

       2) zones with the most stable price

    :return: the name of the selected zone

    >>> zones = ['us-west-2a', 'us-west-2b']
    >>> FauxHistory = lambda p, z: {"SpotPrice": p, "AvailabilityZone": z}
    >>> spot_history = [FauxHistory("0.1", 'us-west-2a'), \
                        FauxHistory("0.2", 'us-west-2a'), \
                        FauxHistory("0.3", 'us-west-2b'), \
                        FauxHistory("0.6", 'us-west-2b')]
    >>> choose_spot_zone(zones, 0.15, spot_history)
    'us-west-2a'

    >>> spot_history=[FauxHistory("0.3", 'us-west-2a'), \
                      FauxHistory("0.2", 'us-west-2a'), \
                      FauxHistory("0.1", 'us-west-2b'), \
                      FauxHistory("0.6", 'us-west-2b')]
    >>> choose_spot_zone(zones, 0.15, spot_history)
    'us-west-2b'

    >>> spot_history=[FauxHistory("0.1", 'us-west-2a'), \
                      FauxHistory("0.7", 'us-west-2a'), \
                      FauxHistory("0.1", 'us-west-2b'), \
                      FauxHistory("0.6", 'us-west-2b')]
    >>> choose_spot_zone(zones, 0.15, spot_history)
    'us-west-2b'
    """
    # Create two lists of tuples of form: [(zone.name, stdeviation), ...] one for zones
    # over the bid price and one for zones under bid price. Each are sorted by increasing
    # standard deviation values.
    markets_under_bid, markets_over_bid = [], []
    for zone in zones:
        zone_histories = [
            zone_history
            for zone_history in spot_history
            if zone_history["AvailabilityZone"] == zone
        ]
        if zone_histories:
            price_deviation = stdev([float(history["SpotPrice"]) for history in zone_histories])
            recent_price = float(zone_histories[0]["SpotPrice"])
        else:
            price_deviation, recent_price = 0.0, bid
        zone_tuple = ZoneTuple(name=zone, price_deviation=price_deviation)
        (markets_over_bid, markets_under_bid)[recent_price < bid].append(zone_tuple)

    return min(
        markets_under_bid or markets_over_bid, key=attrgetter("price_deviation")
    ).name


def optimize_spot_bid(
    boto3_ec2: BaseClient, instance_type: str, spot_bid: float, zone_options: list[str]
) -> str:
    """
    Check whether the bid is in line with history and makes an effort to place
    the instance in a sensible zone.

    :param zone_options: The collection of allowed zones to consider, within
        the region associated with the Boto2 connection.
    """
    spot_history = _get_spot_history(boto3_ec2, instance_type)
    if spot_history:
        _check_spot_bid(spot_bid, spot_history, name=instance_type)
    most_stable_zone = choose_spot_zone(zone_options, spot_bid, spot_history)
    logger.debug("Placing spot instances in zone %s.", most_stable_zone)
    return most_stable_zone


def _check_spot_bid(spot_bid: float, spot_history: list["SpotPriceTypeDef"], name: Optional[str] = None) -> None:
    """
    Prevents users from potentially over-paying for instances

    Note: this checks over the whole region, not a particular zone

    :param spot_bid: The proposed bid in dollars per hour.

    :type spot_history: The recent history of the spot price

    :raises UserError: if bid is > 2X the spot price's average

    >>> FauxHistory = lambda p, z: {"SpotPrice": p, "AvailabilityZone": z}
    >>> spot_data = [ FauxHistory( "0.1", "us-west-2a" ), \
                      FauxHistory( "0.2", "us-west-2a" ), \
                      FauxHistory( "0.3", "us-west-2b" ), \
                      FauxHistory( "0.6", "us-west-2b" ) ]
    >>> # noinspection PyProtectedMember
    >>> _check_spot_bid( 0.1, spot_data )
    >>> # noinspection PyProtectedMember

    # >>> Box._check_spot_bid( 2, spot_data )
    Traceback (most recent call last):
    ...
    UserError: Your bid $ 2.000000 is more than double this instance type's average spot price ($ 0.300000) over the last week
    """
    if name is None:
        # Describe the instance as something
        name = "this instance type"
    average = mean([float(datum["SpotPrice"]) for datum in spot_history])
    if spot_bid > average * 2:
        logger.warning(
            "Your bid $ %f is more than double %s's average "
            "spot price ($ %f) over the last week",
            spot_bid,
            name,
            average,
        )


def _get_spot_history(boto3_ec2: BaseClient, instance_type: str) -> list["SpotPriceTypeDef"]:
    """
    Returns list of 1,000 most recent spot market data points represented as SpotPriceHistory
    objects. Note: The most recent object/data point will be first in the list.

    :rtype: list[SpotPriceHistory]
    """
    one_week_ago = datetime.datetime.now() - datetime.timedelta(days=7)
    spot_data = boto3_pager(
        boto3_ec2.describe_spot_price_history,
        "SpotPriceHistory",
        StartTime=one_week_ago.isoformat(),
        InstanceTypes=[instance_type],
        ProductDescriptions=["Linux/UNIX"],
    )
    return sorted(spot_data, key=lambda d: d["Timestamp"], reverse=True)
