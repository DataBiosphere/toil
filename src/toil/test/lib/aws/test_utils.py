# Copyright (C) 2015-2021 Regents of the University of California
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import os
import pytest

from unittest import mock

from toil.lib.aws import tags_from_env
from toil.test import ToilTest

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

class TagGenerationTest(ToilTest):
    """Test for tag generation from environment variables."""

    @mock.patch.dict(os.environ, {'TOIL_OWNER_TAG': '😀',
                                  'TOIL_AWS_TAGS': '{}'})
    def test_build_tag(self):
        tag_dict = tags_from_env()
        assert tag_dict == {'Owner': '😀'}

    @mock.patch.dict(os.environ, {'TOIL_AWS_TAGS': '{}'})
    def test_empty_aws_tags(self):
        tag_dict = tags_from_env()
        assert tag_dict == dict()

    @mock.patch.dict(os.environ, {'TOIL_AWS_TAGS': '😀'})
    def test_incorrect_json_emoji(self):
        with pytest.raises(SystemExit):
            tags_from_env()

    @mock.patch.dict(os.environ, {'TOIL_OWNER_TAG': '😀',
                                  'TOIL_AWS_TAGS': '{"1": "2", " ":")"}'})
    def test_build_tag_with_tags(self):
        tag_dict = tags_from_env()
        assert tag_dict == {'Owner': '😀', '1': '2', ' ': ')'}
