# Copyright (C) 2015-2022 Regents of the University of California
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
import getpass
import logging

from toil.lib.misc import get_user_name
from toil.test import ToilTest

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)


class UserNameAvailableTest(ToilTest):
    """
    Make sure we can get user names when they are available.
    """

    def test_get_user_name(self):
        # We assume we have the user in /etc/passwd when running the tests.
        real_user_name = getpass.getuser()
        apparent_user_name = get_user_name()
        self.assertEqual(apparent_user_name, real_user_name)

class UserNameUnvailableTest(ToilTest):
    """
    Make sure we can get something for a user name when user names are not
    available.
    """

    def setUp(self):
        super().setUp()
        # Monkey patch getpass.getuser to fail
        self.original_getuser = getpass.getuser
        def fake_getuser():
            raise KeyError('Fake key error')
        getpass.getuser = fake_getuser
    def tearDown(self):
        # Fix the module we hacked up
        getpass.getuser = self.original_getuser
        super().tearDown()

    def test_get_user_name(self):
        apparent_user_name = get_user_name()
        # Make sure we got something
        self.assertTrue(isinstance(apparent_user_name, str))
        self.assertNotEqual(apparent_user_name, '')

class UserNameVeryBrokenTest(ToilTest):
    """
    Make sure we can get something for a user name when user name fetching is
    broken in ways we did not expect.
    """

    def setUp(self):
        super().setUp()
        # Monkey patch getpass.getuser to fail
        self.original_getuser = getpass.getuser
        def fake_getuser():
            raise RuntimeError('Fake error that we did not anticipate')
        getpass.getuser = fake_getuser
    def tearDown(self):
        # Fix the module we hacked up
        getpass.getuser = self.original_getuser
        super().tearDown()

    def test_get_user_name(self):
        apparent_user_name = get_user_name()
        # Make sure we got something
        self.assertTrue(isinstance(apparent_user_name, str))
        self.assertNotEqual(apparent_user_name, '')

