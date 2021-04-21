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

import pytest

from toil.test import ToilTest

from toil.provisioners import parse_node_types

log = logging.getLogger(__name__)


class ProvisionerTest(ToilTest):

    def test_node_type_parsing(self) -> None:
        assert parse_node_types(None) == []
        assert parse_node_types('') == []
        assert parse_node_types('red beans') == [({'red beans'}, None)]
        assert parse_node_types('red beans,rice') == [({'red beans'}, None), ({'rice'}, None)]
        assert parse_node_types('red beans/black beans,rice') == [({'red beans', 'black beans'}, None), ({'rice'}, None)]
        assert parse_node_types('frankfurters:0.05') == [({'frankfurters'}, 0.05)]
        assert parse_node_types('red beans/black beans:999,rice,red beans/black beans') == [({'red beans', 'black beans'}, 999), ({'rice'}, None), ({'red beans', 'black beans'}, None)]
        with pytest.raises(ValueError):
            parse_node_types('your thoughts:penny')
        with pytest.raises(ValueError) as err:
            parse_node_types(',,,')
        assert 'empty' in str(err.value)
        with pytest.raises(ValueError):
            parse_node_types('now hear this:')
        with pytest.raises(ValueError) as err:
            parse_node_types('miles I will walk:500:500')
        assert 'multiple' in str(err.value)
        with pytest.raises(ValueError):
            parse_node_types('red beans:500/black beans:500,rice')
