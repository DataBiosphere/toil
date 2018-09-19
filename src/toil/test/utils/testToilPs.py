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
from toil.test import ToilTest, integrative
from toil.provisioners.abstractProvisioner import AbstractProvisioner
from toil.provisioners.aws.awsProvisioner import AWSProvisioner
from toil.utils.toilPs import filterDeadInstances, filterByArgs, sortByArgs, instanceExists, AWSInstance
from toil import applianceSelf, subprocess
import os
import uuid
from contextlib import contextmanager
import pandas as pd
from datetime import datetime

@integrative
class ToilPsTest(ToilTest):

    def setUp(self):
        super(ToilPsTest, self).setUp()
        self.removeLines = []
        self.listPath = AbstractProvisioner.clusterListPath()

        self.prov = 'aws'  # Arbitrary
        self.zone = 'us-west-2a'  # Must be aws valid zone.
        self.instance = 't2.micro'  # Arbitrary.
        self.clusterName = str(uuid.uuid4())  # To ensure this entry will not get mixed up with any the user has created.

        # nodeStorage arg (5), and  sseKey arg ('') are arbitrary as they will not be used in these tests
        self.testProvisioner = AWSProvisioner(self.clusterName, self.zone, 5, '')

    def tearDown(self):
        super(ToilPsTest, self).tearDown()
        for line in self.removeLines:
            self.removeEntryFromList(line)

    def removeEntryFromList(self, target):
        """
        Remove an entry which contains a target substring from the list.

        :param target: A string to search the file for.
        """
        oldPath = self.listPath
        newPath = self.listPath + '.new'
        with open(oldPath, 'r') as old:
            with open(newPath, 'w') as new:
                for line in old:
                    if target not in line:
                        new.write(line)

        os.remove(oldPath)
        os.rename(newPath, oldPath)

    @contextmanager
    def neverExists(self, instanceClass):
        """
        Force the exists method of a class inheriting toil.utils.toilPs.Instance to always return false.

        :param instanceClass: Any class with an 'exists' method.
        """
        def alwaysFalse():
            return False
        original = instanceClass.exists
        instanceClass.exists = alwaysFalse()
        yield
        instanceClass.exists = original

    @contextmanager
    def alwaysExists(self, instanceClass):
        """
        Force the exists method of a class inheriting toil.utils.toilPs.Instance to always return true.

        :param instanceClass: Any class with an 'exists' method.
        """
        def alwaysTrue():
            return True
        original = instanceClass.exists
        instanceClass.exists = alwaysTrue()
        yield
        instanceClass.exists = original

    def testFilterDeadInstanceExists(self):
        """Test that filterDeadInstance does not remove an existant instance from a pandas DataFrame."""
        data = {0: ['test1', 'aws', 'us-west-2a', 't2.micro', 'now', 'running', 'quay.io/fake']}

        df = pd.DataFrame.from_dict(data, orient='index', columns=AbstractProvisioner.columnNames())

        # filterDeadInstaces relies on libcloud to determine if an instance exists.
        # This context manager prevents you from having to create an instance just to see if it exists.
        with self.alwaysExists(AWSInstance):
            df = filterDeadInstances(df)

        self.assertEqual('test1', df['clustername'][0])

    def testFilterDeadInstanceNonExistant(self):
        """Test that filterDeadInstance removes a non-existant instance."""
        data = {0: ['test1', 'aws', 'us-west-2a', 't2.micro', 'now', 'running', 'quay.io/fake']}
        df = pd.DataFrame.from_dict(data, orient='index', columns=AbstractProvisioner.columnNames())

        # filterDeadInstaces relies on libcloud to determine if an instance exists.
        # This context manager prevents you from having to create an instance just to see if it exists.
        with self.neverExists(AWSInstance):  # The instance class is irrelevant.
            df = filterDeadInstances(df)

        self.assertTrue(df.empty)

    def testInstanceExistsFalse(self):
        """Test if instanceExists correctly identifies an existant instance."""
        row = {'provisioner': 'aws', 'zone': 'us-west-2a', 'clustername': 'test', 'status': 'running'}

        # instanceExists relies on libcloud to determine if an instance exists.
        # This context manager prevents you from having to create an instance just to see if it exists.
        with self.neverExists(AWSInstance):
            self.assertFalse(instanceExists(row))

    def testInstanceExistsTrue(self):
        """Test if instanceExists correctly identifies an nonexistant instance."""
        row = {'provisioner': 'aws', 'zone': 'us-west-2a', 'clustername': 'test', 'status': 'running'}

        # instanceExists relies on libcloud to determine if an instance exists.
        # This context manager prevents you from having to create an instance just to see if it exists.
        with self.alwaysExists(AWSInstance):
            self.assertTrue(instanceExists(row))

    def testInstanceExistsError(self):
        """Test if instanceExists correctly throws a RuntimeError when an invalid provisioner is provided."""
        row = {'provisioner': 'fake', 'zone': 'us-west-2a', 'clustername': 'test', 'status': 'running'}

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

    def entryFromList(self, clusterName):
        """
        Find the first entry which contains a given substring.

        Note that any entry beyond the first will not be found. If searching by clustername, use uuid's to ensure accuracy.
        :param clusterName: A substring
        """
        entry = ''
        with open(self.listPath, 'r') as f:
            for line in f:
                if clusterName in line:
                    entry = line
                    break
        return entry

    def testAddClusterToList(self):
        """Tests that addClusterToList adds an entry to the list as expected."""
        self.testProvisioner.addClusterToList(self.prov, self.instance)
        self.removeLines.append(self.clusterName)  # Ensure that this entry is removed despite test result.

        row = self.entryFromList(self.clusterName)
        self.assertTrue(bool(row))
        entries = row.split(',')

        # The 'created' information is inaccessable to the test and cannont be confirmed.
        key = [self.clusterName, self.prov, self.zone, self.instance, 'initializing', applianceSelf() + '\n']
        indeciesToCheck = [0, 1, 2, 3, 5, 6]
        for val, index in zip(key, indeciesToCheck):
            self.assertEqual(val, entries[index])

    def testUpdateStatusInListRetains(self):
        """Tests that updateStatusInList removes an entry transitioning between 'initilaizing' and a non-broken statuses."""
        self.testProvisioner.addClusterToList(self.prov, self.instance)
        self.removeLines.append(self.clusterName)  # Ensure that this entry is removed despite test result.
        self.testProvisioner.updateStatusInList('running', self.prov)

        entry = self.entryFromList(self.clusterName)
        self.assertIn('running', entry)

    def testUpdateStatusInListRemoves(self):
        """Tests that updateStatusInList removes an entry transitioning between 'initilaizing' and 'broken' statuses."""
        self.testProvisioner.addClusterToList(self.prov, self.instance)
        self.removeLines.append(self.clusterName)  # Ensure that this entry is removed despite test result.
        self.testProvisioner.updateStatusInList('broken', self.prov)

        entry = self.entryFromList(self.clusterName)
        self.assertFalse(bool(entry))

    def testUpdateEntry(self):
        """Test that updateEntry updates a string as expected."""
        original = 'test,azure,westus,Standard_A2,now,configuring,quay.io/fake'
        expected = 'test,azure,westus,Standard_A2,now,running,quay.io/fake'
        new = self.testProvisioner.updateEntry(original, 'running')
        self.assertEqual(expected, new)

    def testToilPsFromCommandline(self):
        """Test that the ps utility is able to be used from the commandline."""
        cmd = ['toil', 'ps']
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = p.communicate()

        self.assertFalse(err)