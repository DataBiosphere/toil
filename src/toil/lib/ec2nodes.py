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
import os
import json
import datetime
import logging
from toil.wdl.wdl_functions import heredoc_wdl

logger = logging.getLogger( __name__ )


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
    __slots__ = ('name', 'cores', 'memory', 'disks', 'disk_capacity')

    def __init__(self, name, cores, memory, disks, disk_capacity):
        self.name = name  # the API name of the instance type
        self.cores = cores  # the number of cores
        self.memory = memory  # RAM in GiB
        self.disks = disks  # the number of ephemeral (aka 'instance store') volumes
        self.disk_capacity = disk_capacity  # the capacity of each ephemeral volume in GiB

    def __str__(self):
        return ("Type: {}\n"
                "Cores: {}\n"
                "Disks: {}\n"
                "Memory: {}\n"
                "Disk Capacity: {}\n"
                "".format(
                self.name,
                self.cores,
                self.disks,
                self.memory,
                self.disk_capacity))

    def __eq__(self, other):
        if (self.name == other.name and
            self.cores == other.cores and
            self.memory == other.memory and
            self.disks == other.disks and
            self.disk_capacity == other.disk_capacity):
            return True
        return False


def isNumber(s):
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
        if isNumber(specs[0]) and specs[1] == 'x' and isNumber(specs[2]):
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


def fetchEC2InstanceDict(regionNickname=None):
    """
    Fetches EC2 instances types by region programmatically using the AWS pricing API.

    See: https://aws.amazon.com/blogs/aws/new-aws-price-list-api/

    :return: A dict of InstanceType objects, where the key is the string:
             aws instance name (example: 't2.micro'), and the value is an
             InstanceType object representing that aws instance name.
    """
    ec2Source = 'https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonEC2/current/index.json'
    if regionNickname is None:
        regionNickname = 'us-west-2'
    region = EC2Regions[regionNickname]  # JSON uses verbose region names as keys

    ec2InstanceList = []

    # summon the API to grab the latest instance types/prices/specs
    response = requests.get(ec2Source)
    if response.ok:
        ec2InstanceList = parseEC2Json2List(jsontext=response.text, region=region)

    if ec2InstanceList:
        return dict((_.name, _) for _ in ec2InstanceList)
    else:
        from toil.lib import generatedEC2Lists as defaultEC2
        return dict((_.name, _) for _ in defaultEC2.ec2InstancesByRegion[regionNickname])


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
                    instance = InstanceType(name=v["attributes"]["instanceType"],
                                            cores=v["attributes"]["vcpu"],
                                            memory=memory,
                                            disks=disks,
                                            disk_capacity=disk_capacity)
                    if instance not in ec2InstanceList:
                        ec2InstanceList.append(instance)
                    else:
                        raise RuntimeError('EC2 JSON format has likely changed.  '
                                           'Duplicate instances found.')
    return ec2InstanceList


def updateStaticEC2Instances():
    """
    Generates a new python file of fetchable EC2 Instances by region with current prices and specs.

    Takes a few (~3+) minutes to run (you'll need decent internet).

    :return: Nothing.  Writes a new 'generatedEC2Lists.py' file.
    """
    logger.info("Updating Toil's EC2 lists to the most current version from AWS's bulk API.  "
                "This may take a while, depending on your internet connection.")

    dirname = os.path.dirname(__file__)
    # the file Toil uses to get info about EC2 instance types
    origFile = os.path.join(dirname, 'generatedEC2Lists.py')
    assert os.path.exists(origFile)
    # use a temporary file until all info is fetched
    genFile = os.path.join(dirname, 'generatedEC2Lists_tmp.py')
    assert not os.path.exists(genFile)
    # will be used to save a copy of the original when finished
    oldFile = os.path.join(dirname, 'generatedEC2Lists_old.py')

    # copyright and imports
    with open(genFile, 'w') as f:
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
        # limitations under the License.
        from toil.lib.ec2nodes import InstanceType\n\n\n''',
                            dictionary={'year': datetime.date.today().strftime("%Y")}))

    currentEC2List = []
    instancesByRegion = {}
    for regionNickname, _ in EC2Regions.iteritems():
        currentEC2Dict = fetchEC2InstanceDict(regionNickname=regionNickname)
        for instanceName, instanceTypeObj in currentEC2Dict.iteritems():
            if instanceTypeObj not in currentEC2List:
                currentEC2List.append(instanceTypeObj)
            instancesByRegion.setdefault(regionNickname, []).append(instanceName)

    # write header of total EC2 instance type list
    genString = "# {num} Instance Types.  Generated {date}.\n".format(
                num=str(len(currentEC2List)), date=str(datetime.datetime.now()))
    genString = genString + "E2Instances = {\n"
    sortedCurrentEC2List = sorted(currentEC2List, key=lambda x: x.name)

    # write the list of all instances types
    for i in sortedCurrentEC2List:
        z = "    '{name}': InstanceType(name='{name}', cores={cores}, memory={memory}, disks={disks}, disk_capacity={disk_capacity})," \
            "\n".format(name=i.name, cores=i.cores, memory=i.memory, disks=i.disks, disk_capacity=i.disk_capacity)
        genString = genString + z
    genString = genString + '}\n\n'

    genString = genString + 'regionDict = {\n'
    for regionName, instanceList in instancesByRegion.iteritems():
        genString = genString + "              '{regionName}': [".format(regionName=regionName)
        for instance in sorted(instanceList):
            genString = genString + "'{instance}', ".format(instance=instance)
        if genString.endswith(', '):
            genString = genString[:-2]
        genString = genString + '],\n'
    if genString.endswith(',\n'):
        genString = genString[:-len(',\n')]
    genString = genString + '}\n'
    with open(genFile, 'a+') as f:
        f.write(genString)

    # append key for fetching at the end
    regionKey = '\nec2InstancesByRegion = dict((region, [E2Instances[i] for i in instances]) for region, instances in regionDict.iteritems())\n'

    with open(genFile, 'a+') as f:
        f.write(regionKey)
    # preserve the original file unless it already exists
    if not os.path.exists(oldFile):
        os.rename(origFile, oldFile)
    # delete the original file if it's still there
    if os.path.exists(origFile):
        os.remove(origFile)
    # replace the instance list with a current list
    os.rename(genFile, origFile)
