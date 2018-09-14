# Copyright (C) 2015-2016 Regents of the University of California
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
"""Displays information about instances opened with Toil."""

import logging
import os
from argparse import ArgumentParser
import pandas as pd
from toil.provisioners.abstractProvisioner import AbstractProvisioner
import ConfigParser
from libcloud.compute.types import Provider
from libcloud.compute.providers import get_driver
import json

logger = logging.getLogger()
logger.setLevel('CRITICAL')


class instance(object):
    """An abstract class used to template commonly done operations using libcloud."""
    def __init__(self, provisioner, zone, name):
        self.provisioner = provisioner
        self.zone = zone
        self.name = name

    @property
    def driver(self):
        """Create the driver object needed for most operations."""
        raise NotImplementedError

    @property
    def exists(self):
        """Determine if a particular instance exists."""
        raise NotImplementedError


class AzureInstance(instance):
    def __init__(self, provisioner, zone, name):
        super(AzureInstance, self).__init__(provisioner, zone, name)
        azureCredentials = ConfigParser.SafeConfigParser()
        azureCredentials.read(os.path.expanduser("~/.azure/credentials"))

        self.client_id = azureCredentials.get("default", "client_id")
        self.secret = azureCredentials.get("default", "secret")
        self.tenant = azureCredentials.get("default", "tenant")
        self.subscription = azureCredentials.get("default", "subscription_id")

    @property
    def driver(self):
        d = get_driver(Provider.AZURE_ARM)
        return d(tenant_id=self.tenant, subscription_id=self.subscription, key=self.client_id, secret=self.secret)

    @property
    def list(self):
        """Get a list of all accessable resource groups."""
        return self.driver.ex_list_resource_groups()

    @property
    def exists(self):
        # TODO Find a more direct approach.
        match = [x for x in self.list if x.name == self.name and x.location == self.zone]
        return bool(match)


class AWSInstance(instance):
    def __init__(self, provisioner, zone, name):
        super(AWSInstance, self).__init__(provisioner, zone, name)
        credentials = ConfigParser.SafeConfigParser()
        credentials.read(os.path.expanduser("~/.aws/credentials"))

        self.id = credentials.get('default', 'aws_access_key_id')
        self.key = credentials.get('default', 'aws_secret_access_key')

    @property
    def driver(self):
        d = get_driver(Provider.EC2)
        return d(self.id, self.key, region=self.zone[:-1])

    @property
    def exists(self):
        match = self.driver.list_nodes(ex_filters={'instance.group-name': self.name,
                                                   'instance-state-name': ['pending', 'running']})
        return bool(match)


class GCEInstance(instance):
    def __init__(self, provisioner, zone, name):
        super(GCEInstance, self).__init__(provisioner, zone, name)
        self.credentialsPath = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
        with open(self.credentialsPath, 'r') as f:
            credentials = json.loads(f.read())

        self.email = credentials['client_email']
        self.projectID = credentials['project_id']

    @property
    def driver(self):
        d = get_driver(Provider.GCE)
        return d(self.email, self.credentialsPath, project=self.projectID, datacenter=self.zone)

    @property
    def exists(self):
        try:
            self.driver.ex_get_instancegroup(self.name, zone=self.zone)
        except:
            status = False
        else:
            status = True
        return status

def instanceExists(row):
    """Determine if an instance on one of Toil's supported platforms exists."""
    instanceType = {'aws': AWSInstance,
                    'azure': AzureInstance,
                    'gce': GCEInstance}

    prov = row['provisioner']
    zone = row['zone']
    name = row['name']

    return instanceType[prov](provisioner=prov, zone=zone, name=name).exists

def filterDeadInstances(df):
    """Remove entries of a Pandas DataFrame which carry information about cloud instances that do not exist."""
    for i, row in df.iterrows():
        if not instanceExists(row):
            AbstractProvisioner.removeClusterFromList(row['name'], row['provisioner'], row['zone'])
            df = df.drop(index=i)
    return df

def valsToList(reqs):
    """Turn the values of a dictionary into a list. (Assumes values are a string with commas or evaluates to False.)"""
    for k, v in reqs.items():
        if v:
            reqs[k] = reqs[k][0].split(',')
    return reqs

def main():
    if os.path.exists('/tmp/toilClusterList.csv'):
        parser = ArgumentParser()
        parser.add_argument('-p', '--provisioners', dest="provisioner", nargs='*', type=str,
                            help='Provisioners from which instances will be displayed.')
        parser.add_argument('-n', '--name', dest='name', nargs='*', type=str,
                            help='The name of the instance to be displayed.') # TODO allow unix-like matching of expressions ex: ab* matches abcd and abc
        parser.add_argument('-d', '--date', dest='date', nargs='*', type=str,
                            help='The date the instance was created. (YYYY-MM-DD format)')
        parser.add_argument('-s', '--status', dest='status', nargs='*', type=str,
                            help='Statuses of the instance: Initializing, Creating, Configuring, Running, and Broken.')  #TODO flush out these defs.
        parser.add_argument('-t', '--sort', dest='sort', nargs='*', type=str,
                            help='Columns to sort on.')  #TODO flush out these defs.

        nonfilteringArgs = ['sort']
        # Change 'arg1,arg2...argN' to ['arg1','arg2',...,'argN'] for easy filtering below.
        filters = valsToList(vars(parser.parse_args()))
        nonfiltering = {key : filters[key] for key in filters if key in nonfilteringArgs}
        filters = {key: filters[key] for key in filters if key not in nonfiltering}

        # While this type of checking could be done utilizing the 'choices' argument of add_argument(), it is being done
        # this way to maintain a CLI argument input style consistent with Toil.
        # (The cartesian product of 'aws', 'gce', 'azure' would have to be the value of the 'choices' argument.)
        supportedProvisioners = ['aws', 'gce', 'azure']
        if filters['provisioner']:
            invalids = [x for x in filters['provisioner'] if x not in supportedProvisioners]
            if invalids:
                filters['provisioner'] = [x for x in filters['provisioner'] if x not in invalids]
                print('Invalid provisioner(s): {}. Please choose from {}'.format(','.join(invalids), supportedProvisioners))

        columnNames = ['name', 'provisioner', 'zone', 'type', 'date', 'time', 'status', 'appliance']
        df = pd.read_csv('/tmp/toilClusterList.csv')
        df = filterDeadInstances(df)

        for k, v in filters.items():
            if v:
                df = df[df[k].isin(v)]  # Filter rows by each requirement.

        if df.empty:
            print('No matching instances...')
        else:
            # QC of columns to sort by
            if nonfiltering['sort']:
                invalids = [x for x in nonfiltering['sort'] if x not in columnNames]
                if invalids:
                    nonfiltering['sort'] = [x for x in nonfiltering['sort'] if x not in invalids]
                    print('Invalid column(s): {}. Please choose from {}'.format(','.join(invalids), columnNames))
                df = df.sort_values(by=nonfiltering['sort'])

            df.columns = [i.upper() for i in columnNames]  # Uppercase for displaying.
            print(df.to_string(justify='left', col_space=20, index=False))
    else:
        logger.debug('/tmp/toilClusterList.csv does not exist')
        print('Toil is not tracking any instances.')
