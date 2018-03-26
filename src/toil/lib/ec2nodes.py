# Copyright (C) 2015-2018 Regents of the University of California
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
from __future__ import absolute_import
import requests
import json
import datetime
from toil.wdl.toilwdl import heredoc_wdl
from toil.lib.generatedEC2Lists import ec2InstancesByRegion


EC2Regions = {'us-west-1': 'US West (N. California)',
              'us-west-2': 'US West (Oregon)',
              'us-east-1': 'US East (N. Virginia)',
              'us-east-2': 'US East (Ohio)',
              'us-gov-west-1': 'AWS GovCloud (US)',
              'ca-central-1': 'Canada (Central)',
              'ap-northeast-1': 'Asia Pacific (Tokyo)',
              'ap-northeast-2': 'Asia Pacific (Seoul)',
              'ap-northeast-3': 'Asia Pacific (Osaka-Local)',
              'ap-southeast-1': 'Asia Pacific (Singapore)',
              'ap-southeast-2': 'Asia Pacific (Sydney)',
              'ap-south-1': 'Asia Pacific (Mumbai)',
              'eu-west-1': 'EU (Ireland)',
              'eu-west-2': 'EU (London)',
              'eu-west-3': 'EU (Paris)',
              'eu-central-1': 'EU (Frankfurt)',
              'sa-east-1': 'South America (Sao Paulo)'}


class InstanceType(object):
    __slots__ = ('name', 'cores', 'memory', 'disks', 'disk_capacity', 'cost')

    def __init__(self, name, cores, memory, disks, disk_capacity, cost):
        self.name = name  # the API name of the instance type
        self.cores = cores  # the number of cores
        self.memory = memory  # RAM in GB
        self.disks = disks  # the number of ephemeral (aka 'instance store') volumes
        self.disk_capacity = disk_capacity  # the capacity of each ephemeral volume in GB
        self.cost = cost

    def __str__(self):
        return ("Type: {}\n"
                "Cores: {}\n"
                "Disks: {}\n"
                "Memory: {}\n"
                "Disk Capacity: {}\n"
                "Cost: {}\n".format(self.name,
                                    self.cores,
                                    self.disks,
                                    self.memory,
                                    self.disk_capacity,
                                    self.cost))

    def __eq__(self, other):
        # exclude cost
        if (self.name == other.name and
                    self.cores == other.cores and
                    self.memory == other.memory and
                    self.disks == other.disks and
                    self.disk_capacity == other.disk_capacity):
            return True
        return False


def is_number(s):
    """
    Determines if a unicode string (that may include commas) is a number.

    :param s: Any unicode string.
    :return: True if s represents a number, False otherwise.
    """
    s = s.replace(',', '')
    try:
        float(s)
        return True
    except ValueError:
        pass
    try:
        import unicodedata
        unicodedata.numeric(s)
        return True
    except (TypeError, ValueError) as e:
        pass
    return False


def parseStorage(storageData):
    """
    Parses EC2 JSON storage param string into a number.

    Examples:
        "2 x 160 SSD"
        "3 x 2000 HDD"
        "EBS only"
        "1 x 410"
        "8 x 1.9 NVMe SSD"

    :param str storageData: EC2 JSON storage param string.
    :return: Two floats representing: (# of disks), and (disk_capacity in GiB of each disk).
    """
    if storageData == "EBS only":
        return [0, 0]
    else:
        specs = storageData.strip().split()
        if is_number(specs[0]) and specs[1] == 'x' and is_number(specs[2]):
            return float(specs[0].replace(',', '')), float(specs[2].replace(',', ''))
        else:
            raise RuntimeError('EC2 JSON format has likely changed.  Error parsing disk specs.')


def parseMemory(memAttribute):
    """
    Returns EC2 'memory' string as a float.

    Format should always be '#' GiB (example: '244 GiB' or '1,952 GiB').
    Amazon loves to put commas in their numbers, so we have to accommodate that.
    If the syntax ever changes, this will raise.

    :param memAttribute: EC2 JSON memory param string.
    :return: A float representing memory in GiB.
    """
    mem = memAttribute.replace(',', '').split()
    if mem[1] == 'GiB':
        return float(mem[0])
    else:
        raise RuntimeError('EC2 JSON format has likely changed.  Error parsing memory.')


def fetchEC2InstanceList(regionNickname=None, latest=False):
    """
    Fetches EC2 instances types by region programmatically using the AWS pricing API.

    See: https://aws.amazon.com/blogs/aws/new-aws-price-list-api/

    :return: A list of InstanceType objects.
    """
    ec2Source = 'https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonEC2/current/index.json'
    if regionNickname is None:
        regionNickname = 'us-west-2'
    region = EC2Regions[regionNickname]  # JSON uses verbose region names as keys

    ec2InstanceList = []

    if latest:
        # summon the API to grab the latest instance types/prices/specs
        response = requests.get(ec2Source)
        if response.ok:
            ec2InstanceList = parseEC2Json2List(jsontext=response.text, region=region)

    if ec2InstanceList:
        return dict((_.name, _) for _ in ec2InstanceList)
    else:
        return dict((_.name, _) for _ in ec2InstancesByRegion[regionNickname])


def parseEC2Json2List(jsontext, region):
    """
    Takes a JSON and returns a list of InstanceType objects representing EC2 instance params.

    :param jsontext:
    :param region:
    :return:
    """
    currentList = json.loads(jsontext)
    ec2InstanceList = []
    for k, v in currentList["products"].iteritems():
        if "location" in v["attributes"] and v["attributes"]["location"] == region:
            # 3 tenant types: 'Host' (always $0.00; just a template?)
            #                 'Dedicated' (toil does not support; these are pricier)
            #                 'Shared' (AWS default and what toil uses)
            if "tenancy" in v["attributes"] and v["attributes"]["tenancy"] == "Shared":
                if v["attributes"]["operatingSystem"] == "Linux":
                    disks, disk_capacity = parseStorage(v["attributes"]["storage"])
                    memory = parseMemory(v["attributes"]["memory"])
                    cost = fetchE2Cost(k, currentList["terms"]["OnDemand"])
                    instance = InstanceType(name=v["attributes"]["instanceType"],
                                            cores=v["attributes"]["vcpu"],
                                            memory=memory,
                                            disks=disks,
                                            disk_capacity=disk_capacity,
                                            cost=cost)
                    assert cost >= 0  # seems good to check
                    if instance not in ec2InstanceList:
                        ec2InstanceList.append(instance)
                    else:
                        raise RuntimeError('EC2 JSON format has likely changed.  '
                                           'Duplicate instances found.')
    return ec2InstanceList


def fetchE2Cost(productID, priceDict):
    """
    Returns the current price of an EC2 instance in USD given an EC2 json dictionary.

    Requires a unique key hash identifier matching an instance type from
    the "products" section.

    Example:

      "terms" : { <----- Start of the entire Pricing Section.
    "OnDemand" : {
      "9ZEEN7WWWQKAG292" : {
        "9ZEEN7WWWQKAG292.JRTCKXETXF" : {
          "priceDimensions" : {
            "9ZEEN7WWWQKAG292.JRTCKXETXF.6YS6EN2CT7" : {
              "pricePerUnit" : {
                "USD" : "12.2400000000"},
              "unit": "Hrs"}},}}

    :param str k: Unique hash key paired to an instance type.  Example: "9ZEEN7WWWQKAG292"
    :param dict priceDict: A subsection of the EC2 JSON representing the pricing section ("terms").
    :return:
    """
    for thisProduct in priceDict:
        if productID == thisProduct:
            for k, v in priceDict[thisProduct].iteritems():
                # each verboseHashID has a different "unit" of price
                for verboseHashID in v['priceDimensions']:
                    # only fetch hourly rates
                    if v['priceDimensions'][verboseHashID]["unit"] == "Hrs":
                        return float(v['priceDimensions'][verboseHashID]['pricePerUnit']['USD'])
    raise RuntimeError('EC2 JSON format has likely changed.  No price found for this instance.')


def updateStaticEC2Instances():
    """
    Generates a new python file of fetchable EC2 Instances by region with current prices and specs.

    Takes a few (~3+) minutes to run (you'll need decent internet).

    :return: Nothing.  Writes a file ('generatedEC2Lists_{date}.py') in the cwd.
    """
    genFile = 'generatedEC2Lists_{date}.py'.format(date=str(datetime.datetime.now()))

    # copyright and imports
    with open(genFile, 'a+') as f:
        f.write(heredoc_wdl('''
        # Copyright (C) 2015-{year} UCSC Computational Genomics Lab
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
        # limitations under the License.\n''',
                            dictionary={'year': datetime.date.today().strftime("%Y")}))
        f.write('from toil.lib.ec2nodes import InstanceType\n\n\n')

    # generate appropriate instance list structures
    instancetypeNums = {}
    for k, v in EC2Regions.iteritems():
        currentEC2List = fetchEC2InstanceList(regionNickname=k, latest=True)
        instancetypeNums[str(k)] = len(currentEC2List)
        genString = "# {num} Instance Types.  Generated {date}.\n".format(
            num=str(len(currentEC2List)), date=str(datetime.datetime.now()))
        genString = genString + k.replace('-', '_') + "_E2Instances = [\n"
        for j, i in currentEC2List.iteritems():
            x = "    InstanceType(name='{name}', cores={cores}, memory={memory}, disks={disks}, disk_capacity={disk_capacity}, cost={cost})," \
                "\n".format(name=i.name, cores=i.cores, memory=i.memory,
                            disks=i.disks, disk_capacity=i.disk_capacity, cost=i.cost)
            genString = genString + x
        genString = genString + ']\n\n'
        with open(genFile, 'a+') as f:
            f.write(genString)

    # append key for fetching at the end
    RegionKey = '''
    ec2InstancesByRegion = {'us-west-1': us_west_1_E2Instances,
                            'us-west-2': us_west_2_E2Instances,
                            'us-east-1': us_east_1_E2Instances,
                            'us-east-2': us_east_2_E2Instances,
                            'us-gov-west-1': us_gov_west_1_E2Instances,
                            'ca-central-1': ca_central_1_E2Instances,
                            'ap-northeast-1': ap_northeast_1_E2Instances,
                            'ap-northeast-2': ap_northeast_2_E2Instances,
                            'ap-northeast-3': ap_northeast_3_E2Instances,
                            'ap-southeast-1': ap_southeast_1_E2Instances,
                            'ap-southeast-2': ap_southeast_2_E2Instances,
                            'ap-south-1': ap_south_1_E2Instances,
                            'eu-west-1': eu_west_1_E2Instances,
                            'eu-west-2': eu_west_2_E2Instances,
                            'eu-west-3': eu_west_3_E2Instances,
                            'eu-central-1': eu_central_1_E2Instances,
                            'sa-east-1': sa_east_1_E2Instances}\n\n'''
    with open(genFile, 'a+') as f:
        f.write(heredoc_wdl(RegionKey))


updateStaticEC2Instances()