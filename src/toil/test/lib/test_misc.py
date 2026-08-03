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
import os
from unittest.mock import patch

from toil.lib.io import ensure_dir_exists
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
            raise KeyError("Fake key error")

        getpass.getuser = fake_getuser

    def tearDown(self):
        # Fix the module we hacked up
        getpass.getuser = self.original_getuser
        super().tearDown()

    def test_get_user_name(self):
        apparent_user_name = get_user_name()
        # Make sure we got something
        self.assertTrue(isinstance(apparent_user_name, str))
        self.assertNotEqual(apparent_user_name, "")


class EnsureDirExistsTest(ToilTest):
    """
    Tests for ensure_dir_exists.
    """

    def test_none_path_is_a_noop(self):
        ensure_dir_exists(None, "--workDir")

    def test_creates_missing_directory(self):
        target = os.path.join(self._createTempDir(), "missing", "nested")
        self.assertFalse(os.path.exists(target))
        ensure_dir_exists(target, "--workDir")
        self.assertTrue(os.path.isdir(target))

    def test_existing_directory_is_left_alone(self):
        target = self._createTempDir()
        marker = os.path.join(target, "keep-me")
        with open(marker, "w") as f:
            f.write("data")
        ensure_dir_exists(target, "--coordinationDir")
        self.assertTrue(os.path.exists(marker))

    def test_exits_when_directory_cannot_be_created(self):
        target = os.path.join(self._createTempDir(), "unwritable")
        with patch("os.makedirs", side_effect=OSError("Permission denied")):
            with self.assertLogs("toil.lib.io", level="CRITICAL") as cm:
                with self.assertRaises(SystemExit) as exc_info:
                    ensure_dir_exists(target, "--workDir")
        self.assertEqual(exc_info.exception.code, 1)
        self.assertIn("--workDir", cm.output[0])
        self.assertIn(target, cm.output[0])


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
            raise RuntimeError("Fake error that we did not anticipate")

        getpass.getuser = fake_getuser

    def tearDown(self):
        # Fix the module we hacked up
        getpass.getuser = self.original_getuser
        super().tearDown()

    def test_get_user_name(self):
        apparent_user_name = get_user_name()
        # Make sure we got something
        self.assertTrue(isinstance(apparent_user_name, str))
        self.assertNotEqual(apparent_user_name, "")
