# Copyright (C) 2018 Regents of the University of California
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
"""Tests for toilPs.py"""
from toil.test import ToilTest, needs_aws, needs_google
from toil.provisioners.abstractProvisioner import AbstractProvisioner
from toil.utils.toilPs import filterDeadInstances, filterByArgs, printDF, sortByArgs, instanceExists, GCEInstance
import os
from toil import subprocess
import uuid
from contextlib import contextmanager
import pandas as pd
from datetime import datetime
import time


class ToilPsTest(ToilTest):

    def setUp(self):
        super(ToilPsTest, self).setUp()

    def tearDown(self):
        super(ToilPsTest, self).tearDown()

    @contextmanager
    def autoFalse(self, instanceClass):
        """Force the exists method of a class inheriting toil.utils.toilPs.Instance to always return false."""
        def alwaysFalse():
            return False
        original = instanceClass.exists
        instanceClass.exists = alwaysFalse()
        yield
        instanceClass.exists = original

    @contextmanager
    def autoTrue(self, instanceClass):
        """Force the exists method of a class inheriting toil.utils.toilPs.Instance to always return true."""
        def alwaysTrue():
            return True
        original = instanceClass.exists
        instanceClass.exists = alwaysTrue()
        yield
        instanceClass.exists = original

    def testFilterDeadInstanceExists(self):
        """Test that filterDeadInstance does not remove an existant instance from a pandas DataFrame."""
        data = {0: ['test1', 'gce', 'us-west1-a', 'n1-standard-1', 'now', 'running', 'quay.io/fake']}

        df = pd.DataFrame.from_dict(data, orient='index', columns=AbstractProvisioner.columnNames())

        # filterDeadInstaces relies on libcloud to determine if an instance exists.
        # This context manager prevents you from having to create an instance just to see if it exists.
        with self.autoTrue(GCEInstance):
            df = filterDeadInstances(df)

        self.assertEqual('test1', df['clustername'][0])

    def testFilterDeadInstanceNonExistant(self):
        """Test that filterDeadInstance removes a non-existant instance."""
        data = {0: ['test1', 'gce', 'us-west1-a', 'n1-standard-1', 'now', 'running', 'quay.io/fake']}
        df = pd.DataFrame.from_dict(data, orient='index', columns=AbstractProvisioner.columnNames())

        # filterDeadInstaces relies on libcloud to determine if an instance exists.
        # This context manager prevents you from having to create an instance just to see if it exists.
        with self.autoFalse(GCEInstance):  # The instance class is irrelevant.
            df = filterDeadInstances(df)

        self.assertTrue(df.empty)

    def testInstanceExistsFalse(self):
        """Test if instanceExists correctly identifies an existant instance."""
        row = {'provisioner': 'aws', 'zone': 'us-west-2a',
               'clustername': 'test', 'status': 'running'}

        # instanceExists relies on libcloud to determine if an instance exists.
        # This context manager prevents you from having to create an instance just to see if it exists.
        with self.autoFalse(GCEInstance):
            self.assertFalse(instanceExists(row))

    def testInstanceExistsTrue(self):
        """Test if instanceExists correctly identifies an nonexistant instance."""
        row = {'provisioner': 'gce', 'zone': 'us-west-2a',
               'clustername': 'test', 'status': 'running'}

        # instanceExists relies on libcloud to determine if an instance exists.
        # This context manager prevents you from having to create an instance just to see if it exists.
        with self.autoTrue(GCEInstance):
            self.assertTrue(instanceExists(row))

    def testInstanceExistsError(self):
        """Test if instanceExists correctly throws a RuntimeError when an invalid provisioner is provided."""
        row = {'provisioner': 'fake', 'zone': 'us-west-2a',
               'clustername': 'test', 'status': 'running'}

        self.assertRaises(RuntimeError, instanceExists, row)

    def testFilterByName(self):
        """Test if the filterDeadInstances function filters by name as expected."""
        filters = {'clustername': ['pass', 'alsoPass']}

        data = {0: ['fail', 'gce', 'us-west1-a', 'n1-standard-1', 'now', 'broken', 'quay.io/fake'],
                1: ['pass', 'aws', 'us-west-2a', 't2.micro', 'now', 'broken', 'quay.io/fake'],
                2: ['fail', 'azure', 'westus', 'Standard_A2', 'now', 'running', 'quay.io/fake'],
                3: ['alsoPass', 'azure', 'westus', 'Standard_A2', 'now', 'running', 'quay.io/fake']}
        df = pd.DataFrame.from_dict(data, orient='index', columns=AbstractProvisioner.columnNames())

        df = filterByArgs(df, filters)

        self.assertNotIn('fail', list(df['clustername']))

    def testFilterByProvisioner(self):
        """Test if the filterDeadInstances function filters by provisioner correctly."""
        filters = {'provisioner': ['aws', 'gce']}

        data = {0: ['pass', 'gce', 'us-west1-a', 'n1-standard-1', 'now', 'broken', 'quay.io/fake'],
                1: ['pass', 'aws', 'us-west-2a', 't2.micro', 'now', 'broken', 'quay.io/fake'],
                2: ['fail', 'azure', 'westus', 'Standard_A2', 'now', 'running', 'quay.io/fake']}
        df = pd.DataFrame.from_dict(data, orient='index', columns=AbstractProvisioner.columnNames())

        df = filterByArgs(df, filters)

        self.assertNotIn('fail', list(df['clustername']))

    def testFilterByStatus(self):
        """Test if the filterByArgs function filters by status correctly."""
        filters = {'status': ['broken', 'initializing']}

        data = {0: ['pass', 'gce', 'us-west1-a', 'n1-standard-1', 'now', 'broken', 'quay.io/fake'],
                1: ['pass', 'aws', 'us-west-2a', 't2.micro', 'now', 'initializing', 'quay.io/fake'],
                2: ['fail', 'azure', 'westus', 'Standard_A2', 'now', 'running', 'quay.io/fake']}
        df = pd.DataFrame.from_dict(data, orient='index', columns=AbstractProvisioner.columnNames())

        df = filterByArgs(df, filters)

        self.assertNotIn('fail', list(df['clustername']))

    def testSortByOneArg(self):
        """Test if the sortByArgs function sorts by one arg correctly."""
        filters = {'chron': True,
                   'sort': ['clustername']}

        data = {0: ['2', 'gce', 'us-west1-a', 'n1-standard-1', 'now', 'initializing', 'quay.io/fake'],
                1: ['0', 'aws', 'us-west-2a', 't2.micro', 'now', 'initializing', 'quay.io/fake'],
                2: ['1', 'azure', 'westus', 'Standard_A2', 'now', 'initializing', 'quay.io/fake']}
        df = pd.DataFrame.from_dict(data, orient='index', columns=AbstractProvisioner.columnNames())

        df = sortByArgs(df, filters)

        order = list(df['clustername'])

        for i in range(3):
            self.assertEqual(str(i), order[i])

    def testSortByMultipleArgs(self):
        """Test if the sortByArgs function sorts on multiple args correctly."""
        filters = {'chron': True,
                   'sort': ['provisioner', 'zone']}

        data = {0: ['4', 'gce', 'us-west1-a', 'n1-standard-1', 'now', 'initializing', 'quay.io/fake'],
                1: ['5', 'gce', 'us-west2-b', 'n1-standard-1', 'now', 'initializing', 'quay.io/fake'],
                2: ['0', 'aws', 'us-west-2a', 't2.micro', 'now', 'initializing', 'quay.io/fake'],
                3: ['1', 'aws', 'us-west-2b', 't2.micro', 'now', 'initializing', 'quay.io/fake'],
                4: ['3', 'azure', 'westus', 'Standard_A2', 'now', 'initializing', 'quay.io/fake'],
                5: ['2', 'azure', 'eastus', 'Standard_A2', 'now', 'initializing', 'quay.io/fake']}

        df = pd.DataFrame.from_dict(data, orient='index', columns=AbstractProvisioner.columnNames())
        df = sortByArgs(df, filters)
        order = list(df['clustername'])

        for i in range(6):
            self.assertEqual(str(i), order[i])

    def testSortChronologically(self):
        """Make sure sortByArgs sorts in chronological order."""
        filters = {'chron': True,
                   'sort': None}
        # Time precision in microseconds to ensure proper order.
        data = {0: ['0', 'gce', 'us-west1-a', 'n1-standard-1', datetime.now().strftime("%H:%M:%S.%f"), 'initializing', 'quay.io/fake'],
                1: ['1', 'gce', 'us-west2-b', 'n1-standard-1', datetime.now().strftime("%H:%M:%S.%f"), 'initializing', 'quay.io/fake'],
                2: ['2', 'aws', 'us-west-2a', 't2.micro', datetime.now().strftime("%H:%M:%S.%f"), 'initializing', 'quay.io/fake'],
                3: ['3', 'aws', 'us-west-2b', 't2.micro', datetime.now().strftime("%H:%M:%S.%f"), 'initializing', 'quay.io/fake'],
                4: ['4', 'azure', 'westus', 'Standard_A2', datetime.now().strftime("%H:%M:%S.%f"), 'initializing', 'quay.io/fake'],
                5: ['5', 'azure', 'eastus', 'Standard_A2', datetime.now().strftime("%H:%M:%S.%f"), 'initializing', 'quay.io/fake']}

        df = pd.DataFrame.from_dict(data, orient='index', columns=AbstractProvisioner.columnNames())
        df = sortByArgs(df, filters)
        order = list(df['clustername'])

        for i in range(6):
            self.assertEqual(str(i), order[i])

    def testSortReverseChronologically(self):
        """Make sure sortByArgs sorts in reverse chronological order."""
        filters = {'chron': False,
                   'sort': None}
        # Time precision in microseconds to ensure proper order.
        data = {0: ['5', 'gce', 'us-west1-a', 'n1-standard-1', datetime.now().strftime("%H:%M:%S.%f"), 'initializing', 'quay.io/fake'],
                1: ['4', 'gce', 'us-west2-b', 'n1-standard-1', datetime.now().strftime("%H:%M:%S.%f"), 'initializing', 'quay.io/fake'],
                2: ['3', 'aws', 'us-west-2a', 't2.micro', datetime.now().strftime("%H:%M:%S.%f"), 'initializing', 'quay.io/fake'],
                3: ['2', 'aws', 'us-west-2b', 't2.micro', datetime.now().strftime("%H:%M:%S.%f"), 'initializing', 'quay.io/fake'],
                4: ['1', 'azure', 'westus', 'Standard_A2', datetime.now().strftime("%H:%M:%S.%f"), 'initializing', 'quay.io/fake'],
                5: ['0', 'azure', 'eastus', 'Standard_A2', datetime.now().strftime("%H:%M:%S.%f"), 'initializing', 'quay.io/fake']}

        df = pd.DataFrame.from_dict(data, orient='index', columns=AbstractProvisioner.columnNames())
        df = sortByArgs(df, filters)
        order = list(df['clustername'])

        for i in range(6):
            self.assertEqual(str(i), order[i])


    # @contextmanager
    # def gceNode(self, name, zone='us-west1-a'):
    #     """A context manager for doing an operation that requires a GCE cluster to exist for a brief time."""
    #     driver = GCEInstance('gce', zone, name, 'running').driver
    #     driver.ex_create_instancegroup(name, 'us-west1-a')
    #     yield
    #     group = driver.ex_get_instancegroup(name, zone=zone)
    #     group.destroy()
    #
    # def getPsOutput(self):
    #     p = subprocess.Popen(['toil', 'ps'], stdout=subprocess.PIPE)
    #     out, err = p.communicate()
    #     return out
    #
    # @needs_aws
    # def testLaunchedClusterAddsToListAWS(self):
    #     clusterName = 'cluster-utils-test' + str(uuid.uuid4())
    #     keyName = os.getenv('TOIL_AWS_KEYNAME')
    #
    #     cmd = ['toil', 'launch-cluster', '--leaderNodeType=t2.micro', '--zone=us-west-2a',
    #             '--keyPairName=' + keyName, clusterName]
    #     p = subprocess.Popen(cmd)
    #
    #     time.sleep(5)  # Wait for launch cluster to proceed far enough to add cluster to the list.
    #
    #     found = False
    #     with open(AbstractProvisioner.clusterListPath(), 'r') as f:
    #         for line in f:
    #             if clusterName in line:
    #                 found = True
    #                 break
    #
    #     self.assertTrue(found)
    #     p.kill()

# Do this in testAbstractProvisioner.py?
# Test addClusterToList
# Test removeClusterFromList
# Test updateStatusInList
# Test updateEntry
