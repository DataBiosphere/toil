# Copyright (C) 2015-2025 Regents of the University of California
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

from typing import Any, Optional, Protocol

class OptionSetter(Protocol):
    """
    Protocol for the setOption function we get to let us set up CLI options for
    each batch system or job store.

    Used to have more parameters for managing defaults and environment
    variables, but now that's all done by the parser.

    Actual functionality is defined in the Config class.
    
    Looks first at option_name and then at old_names to find the namespace key for the option.
    """

    def __call__(
        self,
        option_name: str,
        old_names: Optional[list[str]] = None,
    ) -> None: ...
