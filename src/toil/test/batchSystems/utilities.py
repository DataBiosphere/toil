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
import json

from toil.batchSystems.lsfHelper import convert_mb
from toil.test import ToilTest

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

class LSFUtilitiesTest(ToilTest):
    def test_convert_mb(self):
        expected_conversions_in_megabytes = {
            "0 B": 1.0,
            "0 KB": 1.0,
            "0 MB": 1.0,
            "0 GB": 1.0,
            "0 TB": 1.0,
            "0.1 B": 1.0,
            "0.1 KB": 1.0,
            "0.1 MB": 1.0,
            "0.1 GB": 100.0,
            "0.1 TB": 100000.0,
            "0.5 B": 1.0,
            "0.5 KB": 1.0,
            "0.5 MB": 1.0,
            "0.5 GB": 500.0,
            "0.5 TB": 500000.0,
            "0.9 B": 1.0,
            "0.9 KB": 1.0,
            "0.9 MB": 1.0,
            "0.9 GB": 900.0,
            "0.9 TB": 900000.0,
            "1 B": 1.0,
            "1 KB": 1.0,
            "1 MB": 1.0,
            "1 GB": 1000.0,
            "1 TB": 1000000.0,
            "7 B": 1.0,
            "7 KB": 1.0,
            "7 MB": 7.0,
            "7 GB": 7000.0,
            "7 TB": 7000000.0,
            "7.42423 B": 1.0,
            "7.42423 KB": 1.0,
            "7.42423 MB": 7.42,
            "7.42423 GB": 7424.23,
            "7.42423 TB": 7424230.0,
            "10 B": 1.0,
            "10 KB": 1.0,
            "10 MB": 10.0,
            "10 GB": 10000.0,
            "10 TB": 10000000.0,
            "100 B": 1.0,
            "100 KB": 1.0,
            "100 MB": 100.0,
            "100 GB": 100000.0,
            "100 TB": 100000000.0,
            "1000 B": 1.0,
            "1000 KB": 1.0,
            "1000 MB": 1000.0,
            "1000 GB": 1000000.0,
            "1000 TB": 1000000000.0
        }
        results = {}
        for i in (0, 0.1, 0.5, 0.9, 1, 7, 7.42423, 10, 100, 1000):
            for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
                converted = convert_mb(i, unit)
                assert converted >= 1  # always round up to at least 1 mb
                assert len(str(converted).split('.', 1)[-1]) <= 2  # always round to 2 decimals or less
                results[f'{i} {unit}'] = convert_mb(i, unit)
        assert results == expected_conversions_in_megabytes
