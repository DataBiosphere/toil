# Copyright (C) 2024 Regents of the University of California
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

"""
Contains functions for making web requests with Toil.

All web requests should go through this module, to make sure they use the right
user agent.
>>> httpserver = getfixture("httpserver")
>>> handler = httpserver.expect_request("/path").respond_with_json({})
>>> from toil.lib.web import web_session
>>> web_session.get(httpserver.url_for("/path"))
<Response [200]>

"""

import logging
import requests
import sys

from toil.version import baseVersion

# We manage a Requests session at the module level in case we're supposed to be
# doing cookies, and to send a sensible user agent.
# We expect the Toil and Python version to not be personally identifiable even
# in theory (someone might make a new Toil version first, but there's no way
# to know for sure that nobody else did the same thing).
web_session = requests.Session()
web_session.headers.update({"User-Agent": f"Toil {baseVersion} on Python {'.'.join([str(v) for v in sys.version_info])}"})
