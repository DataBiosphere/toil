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
import subprocess32
import pandas as pd
from toil.provisioners.abstractProvisioner import AbstractProvisioner

log = logging.getLogger(__name__)

def checkAzureInstance(row):
    """Determine if an azure instance exists"""
    playbook = os.path.abspath('src/toil/provisioners/azure/create-azure-resourcegroup.yml')
    cmd = 'ansible-playbook -v --tags=all --extra-vars resgrp={}, region={} {}'.format(row['name'],
                                                                                       row['zone'], playbook)
    try:
        p = subprocess32.Popen(cmd)
        p.communicate()
    except RuntimeError:
        dead = True
    else:
        dead = False
    return dead

def checkAWSInstance(row):
    pass

def checkGCEInstance(row):
    pass


def deadInstance(row):
    """Determine if an instance on one of Toil's supporter platforms exists"""
    if row['provisioner'] == 'aws':
        isDead = checkAWSInstance(row)
    elif row['provisioner'] == 'gce':
        isDead = checkGCEInstance(row)
    else:
        isDead = checkAzureInstance(row)
    return isDead

def filterDeadInstances(df):
    """Remove entries of a Pandas DataFrame which carry information about cloud instances that do not exist."""
    for i, row in df.iterrows():
        if deadInstance(row):
            AbstractProvisioner.removeClusterFromList(row['name'], row['provisioner'], row['zone'])
            df.drop(index=i)
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

        df = pd.read_csv('/tmp/toilClusterList.csv')
        #df = filterDeadInstances(df)
        columnNames = ['name', 'provisioner', 'zone', 'type', 'date', 'time', 'status', 'appliance']

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
        log.debug('/tmp/toilClusterList.csv does not exist')
        print('Toil is not tracking any instances.')
