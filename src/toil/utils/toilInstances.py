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

# These have messages during importing.
from azure.common.credentials import ServicePrincipalCredentials
from azure.mgmt.resource import ResourceManagementClient

def azureIsDead(row):
    """Determine if an Azure instance exists."""
    azureCredentials = ConfigParser.SafeConfigParser()
    azureCredentials.read(os.path.expanduser("~/.azure/credentials"))

    client_id = azureCredentials.get("default", "client_id")
    secret = azureCredentials.get("default", "secret")
    tenant = azureCredentials.get("default", "tenant")
    subscription = azureCredentials.get("default", "subscription_id")

    credentials = ServicePrincipalCredentials(client_id=client_id, secret=secret, tenant=tenant)
    client = ResourceManagementClient(credentials, subscription)

    match = [x for x in client.resource_groups.list() if x.name == row['name'] and x.location == row['zone']]

    return not bool(match)

def awsIsDead(row):
    """Determine if an AWS instance exists"""
    awsCredentials = ConfigParser.SafeConfigParser()
    awsCredentials.read(os.path.expanduser("~/.aws/credentials"))

    d = get_driver(Provider.EC2)
    id = awsCredentials.get('default', 'aws_access_key_id')
    key = awsCredentials.get('default', 'aws_secret_access_key')

    driver = d(id, key, region=row['zone'][:-1])

    match = driver.list_nodes(ex_filters={'instance.group-name': row['name'],
                                          'instance-state-name': ['pending', 'running']})
    return not bool(match)

def gceIsDead(row):
    """Determine if a GCE instance exists."""

    jsonPath = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
    with open(jsonPath, 'r') as f:
        gceCredentials = json.loads(f.read())

    email = gceCredentials['client_email']
    projectID = gceCredentials['project_id']

    d = get_driver(Provider.GCE)
    driver = d(email, jsonPath, project=projectID, datacenter=row['zone'])

    try:
        driver.ex_get_instancegroup(row['name'],zone=row['zone'])
    except:
        match = False
    else:
        match = True
    return not match

    # create driver
    # check


def deadInstance(row):
    """Determine if an instance on one of Toil's supporter platforms exists"""
    if row['provisioner'] == 'aws':
        isDead = awsIsDead(row)
    elif row['provisioner'] == 'gce':
        isDead = gceIsDead(row)
    else:
        isDead = azureIsDead(row)
        pass
    return isDead

def filterDeadInstances(df):
    """Remove entries of a Pandas DataFrame which carry information about cloud instances that do not exist."""
    for i, row in df.iterrows():
        if deadInstance(row):
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
    #logging.basicConfig(level=logging.CRITICAL)
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
