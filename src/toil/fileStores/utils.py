# Copyright (C) 2015-2020 Regents of the University of California
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
import toil.jobStores.abstractJobStore
import toil.job

from typing import Callable


def create_filestore(
        jobStore: toil.jobStores.abstractJobStore.AbstractJobStore,
        jobDesc: toil.job.JobDescription,
        localTempDir: str,
        waitForPreviousCommit: Callable,
        caching: bool):
    # Defer these imports until runtime, since these classes depend on us
    from toil.fileStores.cachingFileStore import CachingFileStore
    from toil.fileStores.nonCachingFileStore import NonCachingFileStore
    fileStoreCls = CachingFileStore if caching else NonCachingFileStore
    return fileStoreCls(jobStore, jobDesc, localTempDir, waitForPreviousCommit)
