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
from toil.lib.url import URLAccess

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

class URLTest(ToilTest):
    """
    Make sure we can get something for a user name when user names are not
    available.
    """

    def test_get_url_access(self):
        url = URLAccess()
        self.assertTrue(url.url_exists("https://raw.githubusercontent.com/DataBiosphere/toil/refs/heads/master/src/toil/batchSystems/abstractBatchSystem.py"))


