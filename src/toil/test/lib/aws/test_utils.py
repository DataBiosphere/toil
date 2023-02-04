# Copyright (C) 2015-2021 Regents of the University of California
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
import logging
import os
import uuid
from typing import Optional

import pytest

from toil.lib.aws import build_tag_dict_from_env
from toil.test import ToilTest

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

class TagGenerationTest(ToilTest):
    """
    Test for tag generation from environment variables
    """
    def test_build_tag(self):
        environment = dict()
        environment["TOIL_OWNER_TAG"] = "😀"
        environment["TOIL_AWS_TAGS"] = None
        tag_dict = build_tag_dict_from_env(environment)
        assert(tag_dict == {'Owner': '😀'})

    def test_empty_aws_tags(self):
        environment = dict()
        environment["TOIL_OWNER_TAG"] = None
        environment["TOIL_AWS_TAGS"] = "{}"
        tag_dict = build_tag_dict_from_env(environment)
        assert (tag_dict == dict())

    def test_incorrect_json_object(self):
        with pytest.raises(SystemExit):
            environment = dict()
            environment["TOIL_OWNER_TAG"] = None
            environment["TOIL_AWS_TAGS"] = "231"
            tag_dict = build_tag_dict_from_env(environment)

    def test_incorrect_json_emoji(self):
        with pytest.raises(SystemExit):
            environment = dict()
            environment["TOIL_OWNER_TAG"] = None
            environment["TOIL_AWS_TAGS"] = "😀"
            tag_dict = build_tag_dict_from_env(environment)

    def test_build_tag_with_tags(self):
        environment = dict()
        environment["TOIL_OWNER_TAG"] = "😀"
        environment["TOIL_AWS_TAGS"] = '{"1": "2", " ":")"}'
        tag_dict = build_tag_dict_from_env(environment)
        assert(tag_dict == {'Owner': '😀', '1': '2', ' ': ')'})



