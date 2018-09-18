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
from libcloud.common.google import ResourceNotFoundError
import json

logger = logging.getLogger()
logger.setLevel('CRITICAL')


class Instance(object):
    """An abstract class used to template commonly done operations using libcloud."""
    def __init__(self, provisioner, zone, name, status):
        self.provisioner = provisioner
        self.zone = zone
        self.name = name
        self.status = status

    @property
    def driver(self):
        """Create the driver object needed for most operations."""
        raise NotImplementedError

    @property
    def exists(self):
        """Determine if a particular instance exists."""
        raise NotImplementedError


class AzureInstance(Instance):
    """A class for executing commonly done operations in the Azure cloud using libcloud."""
    def __init__(self, provisioner, zone, name, status):
        super(AzureInstance, self).__init__(provisioner, zone, name, status)
        credentials = ConfigParser.SafeConfigParser()
        credentials.read(os.path.expanduser("~/.azure/credentials"))

        self.client_id = credentials.get("default", "client_id")
        self.secret = credentials.get("default", "secret")
        self.tenant = credentials.get("default", "tenant")
        self.subscription = credentials.get("default", "subscription_id")

    @property
    def driver(self):
        """Create the driver object needed for most operations."""
        d = get_driver(Provider.AZURE_ARM)
        return d(tenant_id=self.tenant, subscription_id=self.subscription, key=self.client_id, secret=self.secret)

    @property
    def list(self):
        """Get a list of all accessable resource groups."""
        return self.driver.ex_list_resource_groups()

    @property
    def exists(self):
        """
        Determine if a particular instance exists in the Azure cloud.

        An instance with a status of 'initializing' has not been created yet. However, it should be treated as if it
        did; it should not be removed from the list while its being setup.
        """
        if self.status == 'initializing':
            match = True
        else:
            # TODO Find a more direct approach.
            match = [x for x in self.list if x.name == self.name and x.location == self.zone]
        return bool(match)


class AWSInstance(Instance):
    """A class for executing commonly done operations in the AWS cloud using libcloud."""
    def __init__(self, provisioner, zone, name, status):
        super(AWSInstance, self).__init__(provisioner, zone, name, status)
        credentials = ConfigParser.SafeConfigParser()
        credentials.read(os.path.expanduser("~/.aws/credentials"))

        self.id = credentials.get('default', 'aws_access_key_id')
        self.key = credentials.get('default', 'aws_secret_access_key')

    @property
    def driver(self):
        """Create the driver object needed for most operations."""
        d = get_driver(Provider.EC2)
        return d(self.id, self.key, region=self.zone[:-1])

    @property
    def exists(self):
        """
        Determine if a particular instance exists in the AWS cloud.

        An instance with a status of 'initializing' has not been created yet. However, it should be treated as if it
        did; it should not be removed from the list while its being setup.
        """
        if self.status == 'initializing':
            match = True
        else:
            match = self.driver.list_nodes(ex_filters={'instance.group-name': self.name,
                                                       'instance-state-name': ['pending', 'running']})
        return bool(match)


class GCEInstance(Instance):
    """A class for executing commonly done operations in the Google Compute Engine cloud using libcloud."""
    def __init__(self, provisioner, zone, name, status):
        super(GCEInstance, self).__init__(provisioner, zone, name, status)
        self.credentialsPath = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
        with open(self.credentialsPath, 'r') as f:
            credentials = json.loads(f.read())

        self.email = credentials['client_email']
        self.projectID = credentials['project_id']

    @property
    def driver(self):
        """Create the driver object needed for most operations."""
        d = get_driver(Provider.GCE)
        return d(self.email, self.credentialsPath, project=self.projectID, datacenter=self.zone)

    @property
    def exists(self):
        """
        Determine if a particular instance exists in the Google Compute Engine cloud.

        An instance with a status of 'initializing' has not been created yet. However, it should be treated as if it
        did; it should not be removed from the list while its being setup.
        """
        if self.status == 'initializing':
            status = True
        else:
            try:
                self.driver.ex_get_instancegroup(self.name, zone=self.zone)
            except ResourceNotFoundError:
                status = False
            else:
                status = True
        return status


def instanceExists(row):
    """Determine if an instance on one of Toil's supported cloud platforms exists."""
    instanceClass = {'aws': AWSInstance,
                     'azure': AzureInstance,
                     'gce': GCEInstance}

    prov = row['provisioner']
    zone = row['zone']
    name = row['clustername']
    status = row['status']

    try:
        instanceType = instanceClass[prov]
    except KeyError:
        configLoc = os.path.join(os.path.expanduser('~'), '.toilClusterList.csv')
        raise RuntimeError('{} contains an entry with an invalid provisioner: {}'.format(configLoc, prov)) # TODO Create MalformedConfig exception

    return instanceType(provisioner=prov, zone=zone, name=name, status=status).exists


def filterDeadInstances(df):
    """Remove entries of a Pandas DataFrame which carry information about cloud instances that do not exist."""
    for i, row in df.iterrows():
        if not instanceExists(row):
            AbstractProvisioner.removeClusterFromList(row['clustername'], row['provisioner'], row['zone'])
            df = df.drop(index=i)
    return df


def valsToList(reqs):
    """Turn the values of a dictionary into a list. (Assumes values are a string with commas or evaluates to False.)"""
    for k, v in reqs.items():
        if v and isinstance(v, list):
            reqs[k] = reqs[k][0].split(',')
    return reqs

def sortByArgs(df, nonfilters):
    """Sort a dataframe."""
    columnNames = AbstractProvisioner.columnNames()
    df = df.sort_values(by='created', ascending=nonfilters['chron'])
    if nonfilters['sort']:
        invalids = [x for x in nonfilters['sort'] if x not in columnNames]  # QC of column names.
        if invalids:
            nonfilters['sort'] = [x for x in nonfilters['sort'] if x not in invalids]
            print('Invalid column(s): {}. Please choose from {}'.format(','.join(invalids), columnNames))
        df = df.sort_values(by=nonfilters['sort'])

    return df

def filterBadProvisioners(provisioners):
    """
    Check that the provisioners specified are valid.

    While this type of checking could be done utilizing the 'choices' argument of add_argument(), it is being done
    this way to maintain a CLI argument input style consistent with Toil.
    (The cartesian product of 'aws', 'gce', 'azure' would have to be the value of the 'choices' argument.)
    """
    supportedProvisioners = ['aws', 'gce', 'azure']
    invalids = [x for x in provisioners if x not in supportedProvisioners]
    if invalids:
        provisioners = [x for x in provisioners if x not in invalids]
        print('Invalid provisioner(s): {}. Please choose from {}'.format(','.join(invalids), supportedProvisioners))
    return provisioners

def filterByArgs(df, filters):
    """Filter a dataframe given a dictionary of filters."""
    for k, v in filters.items():
        if v:
            df = df[df[k].isin(v)]  # Filter rows by each requirement.
    return df

def printDF(df):
    """Print a dataframe nicely. Adds a blank row for printing column headers with empty dataframes."""
    if df.empty:
        blank = pd.Series(['', '', '', '', '', '', ''], index=list(df))
        df = df.append(blank, ignore_index=True)  # Allows assigning column names for printing.

    df.columns = [i.upper() for i in list(df)]

    print(df.to_string(justify='left', col_space=20, index=False))

def main():
    listPath = AbstractProvisioner.clusterListPath()
    columnNames = AbstractProvisioner.columnNames()

    parser = ArgumentParser()
    # Filtering Args. Arugments used to filter out rows of the data frame.
    parser.add_argument('-p', '--provisioners', dest="provisioner", nargs='*', type=str,
                        help='Provisioners from which instances will be displayed.')
    parser.add_argument('-n', '--name', dest='name', nargs='*', type=str,
                        help='The name of the instance to be displayed.') # TODO allow unix-like matching of expressions ex: ab* matches abcd and abc
    parser.add_argument('-s', '--status', dest='status', nargs='*', type=str,
                        help='Statuses of the instance: Initializing, Creating, Configuring, Running, and Broken.')  #TODO flush out these defs.

    # Nonfiltering Args. Arugments not used to filter our rows of the dataframe.
    parser.add_argument('-t', '--sort', dest='sort', nargs='*', type=str,
                        help='Columns to sort on.')  #TODO flush out these defs.
    parser.add_argument('-c', '--chronological', dest='chron', action='store_true', default=False,
                        help='Display instances chronological order')
    nonfilteringArgs = ['sort', 'chron']

    # Change {arg: 'val1,val2...valN'} to {arg: ['val1','val2',...,'valN']} for easy filtering in filterByArgs().
    filters = valsToList(vars(parser.parse_args()))
    nonfilters = {key: filters[key] for key in filters if key in nonfilteringArgs}
    filters = {key: filters[key] for key in filters if key not in nonfilters}

    if 'provisioners' in filters:
        filters['provisioners'] = filterBadProvisioners(filters['provisioners'])

    if os.path.exists(listPath):
        df = pd.read_csv(listPath)
        df = filterDeadInstances(df)
        df = filterByArgs(df, filters)
        if not df.empty:
            df = sortByArgs(df, nonfilters)
    else:
        df = pd.DataFrame(columns=[i.upper() for i in columnNames])

    printDF(df)
