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

# https://pytest.org/latest/example/pythoncollection.html

import json
import logging
from io import StringIO
from typing import Any, Dict, List, Optional, Tuple

from cwltest import utils
logger = logging.getLogger(__name__)

collect_ignore = ["spec"]


# Hook into Pytest for testing CWL conformance with Toil
# https://pytest.org/en/6.2.x/writing_plugins.html?highlight=conftest#conftest-py-local-per-directory-plugins
# See cwltool's reference implementation:
# https://github.com/common-workflow-language/cwltool/blob/05af6c1357c327b3146e9f5da40e7c0aa3e6d976/tests/cwl-conformance/cwltool-conftest.py
def pytest_cwl_execute_test(
        config: utils.CWLTestConfig,
        processfile: str,
        jobfile: Optional[str]
) -> Tuple[int, Optional[Dict[str, Any]]]:
    """Use the CWL reference runner (cwltool) to execute tests."""
    from toil.cwl.cwltoil import main

    stdout = StringIO()
    argsl: List[str] = [f"--outdir={config.outdir}"]
    if config.runner_quiet:
        argsl.append("--quiet")
    elif config.verbose:
        argsl.append("--debug")
    argsl.extend(config.args)
    argsl.append(processfile)
    if jobfile:
        argsl.append(jobfile)
    try:
        result = main(args=argsl, stdout=stdout)
    except Exception as e:
        logger.error(e)
        return 1, {}
    out = stdout.getvalue()
    return result, json.loads(out) if out else {}
