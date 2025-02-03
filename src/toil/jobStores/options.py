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

import logging
from argparse import ArgumentParser, _ArgumentGroup
from typing import Optional, Union

from toil.options import OptionSetter

logger = logging.getLogger(__name__)

def add_all_job_store_options(parser: Union[ArgumentParser, _ArgumentGroup]) -> None:
    """
    Add all job stores' options to the parser.
    """
    
    from toil.jobStores.abstractJobStore import AbstractJobStore

    for job_store_type in AbstractJobStore._get_job_store_classes():
        logger.debug("Add options for %s job store", job_store_type)
        job_store_type.add_options(parser)

def set_job_store_options(
    set_option: OptionSetter
) -> None:
    """
    Call set_option for all job stores' options.

    Unlike with batch systems, multiple job store classes can be used in one
    workflow (for URL access), so we don't allow restrictign to jsut one
    implementation.
    """

    from toil.jobStores.abstractJobStore import AbstractJobStore

    for job_store_type in AbstractJobStore._get_job_store_classes():
        job_store_type.set_options(set_option)



    
