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
import os
import socket
from collections import namedtuple
from operator import attrgetter
from statistics import mean, stdev
from urllib.error import URLError
from urllib.request import urlopen

from toil.lib.aws import zone_to_region

logger = logging.getLogger(__name__)

ZoneTuple = namedtuple('ZoneTuple', ['name', 'price_deviation'])


def running_on_ec2():
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

def get_current_aws_region():
    aws_zone = get_current_aws_zone()
    return zone_to_region(aws_zone) if aws_zone else None


def get_current_aws_zone(spotBid=None, nodeType=None, ctx=None):
    zone = os.environ.get('TOIL_AWS_ZONE', None)
    if not zone and running_on_ec2():
        try:
            import boto
            from boto.utils import get_instance_metadata
            zone = get_instance_metadata()['placement']['availability-zone']
        except (KeyError, ImportError):
            pass
    if not zone and spotBid:
        # if spot bid is present, all the other parameters must be as well
        assert bool(spotBid) == bool(nodeType) == bool(ctx)
        # if the zone is unset and we are using the spot market, optimize our
        # choice based on the spot history
        return optimize_spot_bid(ctx=ctx, instance_type=nodeType, spot_bid=float(spotBid))
    if not zone:
        try:
            import boto
            zone = boto.config.get('Boto', 'ec2_region_name')
            if zone is not None:
                zone += 'a'  # derive an availability zone in the region
        except ImportError:
            pass

    return zone


def choose_spot_zone(zones, bid, spot_history):
    """
    Returns the zone to put the spot request based on, in order of priority:

       1) zones with prices currently under the bid

       2) zones with the most stable price

    :param list[boto.ec2.zone.Zone] zones:
    :param float bid:
    :param list[boto.ec2.spotpricehistory.SpotPriceHistory] spot_history:

    :rtype: str
    :return: the name of the selected zone

    >>> from collections import namedtuple
    >>> FauxHistory = namedtuple('FauxHistory', ['price', 'availability_zone'])
    >>> ZoneTuple = namedtuple('ZoneTuple', ['name'])
    >>> zones = [ZoneTuple('us-west-2a'), ZoneTuple('us-west-2b')]
    >>> spot_history = [FauxHistory(0.1, 'us-west-2a'), \
                        FauxHistory(0.2, 'us-west-2a'), \
                        FauxHistory(0.3, 'us-west-2b'), \
                        FauxHistory(0.6, 'us-west-2b')]
    >>> choose_spot_zone(zones, 0.15, spot_history)
    'us-west-2a'

    >>> spot_history=[FauxHistory(0.3, 'us-west-2a'), \
                      FauxHistory(0.2, 'us-west-2a'), \
                      FauxHistory(0.1, 'us-west-2b'), \
                      FauxHistory(0.6, 'us-west-2b')]
    >>> choose_spot_zone(zones, 0.15, spot_history)
    'us-west-2b'

    >>> spot_history=[FauxHistory(0.1, 'us-west-2a'), \
                      FauxHistory(0.7, 'us-west-2a'), \
                      FauxHistory(0.1, 'us-west-2b'), \
                      FauxHistory(0.6, 'us-west-2b')]
    >>> choose_spot_zone(zones, 0.15, spot_history)
    'us-west-2b'
    """
    # Create two lists of tuples of form: [(zone.name, stdeviation), ...] one for zones
    # over the bid price and one for zones under bid price. Each are sorted by increasing
    # standard deviation values.
    markets_under_bid, markets_over_bid = [], []
    for zone in zones:
        zone_histories = [zone_history for zone_history in spot_history if zone_history.availability_zone == zone.name]
        if zone_histories:
            price_deviation = stdev([history.price for history in zone_histories])
            recent_price = zone_histories[0].price
        else:
            price_deviation, recent_price = 0.0, bid
        zone_tuple = ZoneTuple(name=zone.name, price_deviation=price_deviation)
        (markets_over_bid, markets_under_bid)[recent_price < bid].append(zone_tuple)

    return min(markets_under_bid or markets_over_bid, key=attrgetter('price_deviation')).name


def optimize_spot_bid(ctx, instance_type, spot_bid):
    """
    Check whether the bid is sane and makes an effort to place the instance in a sensible zone.
    """
    spot_history = _get_spot_history(ctx, instance_type)
    if spot_history:
        _check_spot_bid(spot_bid, spot_history)
    zones = ctx.ec2.get_all_zones()
    most_stable_zone = choose_spot_zone(zones, spot_bid, spot_history)
    logger.debug("Placing spot instances in zone %s.", most_stable_zone)
    return most_stable_zone


def _check_spot_bid(spot_bid, spot_history):
    """
    Prevents users from potentially over-paying for instances

    Note: this checks over the whole region, not a particular zone

    :param spot_bid: float

    :type spot_history: list[SpotPriceHistory]

    :raises UserError: if bid is > 2X the spot price's average

    >>> from collections import namedtuple
    >>> FauxHistory = namedtuple( "FauxHistory", [ "price", "availability_zone" ] )
    >>> spot_data = [ FauxHistory( 0.1, "us-west-2a" ), \
                      FauxHistory( 0.2, "us-west-2a" ), \
                      FauxHistory( 0.3, "us-west-2b" ), \
                      FauxHistory( 0.6, "us-west-2b" ) ]
    >>> # noinspection PyProtectedMember
    >>> _check_spot_bid( 0.1, spot_data )
    >>> # noinspection PyProtectedMember

    # >>> Box._check_spot_bid( 2, spot_data )
    Traceback (most recent call last):
    ...
    UserError: Your bid $ 2.000000 is more than double this instance type's average spot price ($ 0.300000) over the last week
    """
    average = mean([datum.price for datum in spot_history])
    if spot_bid > average * 2:
        logger.warning("Your bid $ %f is more than double this instance type's average "
                 "spot price ($ %f) over the last week", spot_bid, average)


def _get_spot_history(ctx, instance_type):
    """
    Returns list of 1,000 most recent spot market data points represented as SpotPriceHistory
    objects. Note: The most recent object/data point will be first in the list.

    :rtype: list[SpotPriceHistory]
    """

    one_week_ago = datetime.datetime.now() - datetime.timedelta(days=7)
    spot_data = ctx.ec2.get_spot_price_history(start_time=one_week_ago.isoformat(),
                                               instance_type=instance_type,
                                               product_description="Linux/UNIX")
    spot_data.sort(key=attrgetter("timestamp"), reverse=True)
    return spot_data
