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
"""Create a config file with all default Toil options."""
import ruamel.yaml as yaml
import logging
import os

from argparse import _StoreFalseAction, _StoreTrueAction
from typing import Set, Dict, Optional

from configargparse import ArgParser, YAMLConfigFileParser # type: ignore

from toil.common import Toil, parser_with_common_options, addOptions, Config
from toil.jobStores.abstractJobStore import NoSuchJobStoreException
from toil.statsAndLogging import set_logging_from_options
logger = logging.getLogger(__name__)

# groups and arguments according to documentation
# groups = {
#     "core": {"workDir", "coordinationDir", "noStdOutErr", "stats", "clean", "cleanWorkDir", "clusterStats", "restart", "jobStore"},
#     "logging": {"logOff", "logCritical", "logError", "logWarning", "logInfo", "logDebug", "logLevel", "logFile", "rotatingLogging", "maxLogFileSize", "log-dir"},
#     "batch_systems": {"batchSystem", "disableAutoDeployment", "maxJobs", "maxLocalJobs", "manualMemArgs", "runCwlInternalJobsOnWorkers", "statePollingWait", "batchLogsDir", "parasolCommand", "parasolMaxBatches", "mesosEndpoint", "mesosFrameworkId", "mesosRole", "mesosName", "kubernetesHostPath", "kubernetesOwner", "kubernetesServiceAccount", "kubernetesPodTimeout", "tesEndpoint", "tesUser", "tesPassword", "tesBearerToken", "awsBatchRegion", "awsBatchQueue", "awsBatchJobRoleArn", "scale"},
#     "data_storage": {"linkImports", "moveExports", "disableCaching", "caching"},
#     "autoscaling": {"provisioner", "nodeTypes", "minNodes", "maxNodes", "targetTime", "betaInertia", "scaleInterval", "preemptibleCompensation", "nodeStorage", "nodeStorageOverrides", "metrics", "assumeZeroOverhead"},
#     "service": {"maxServiceJobs", "maxPreemptibleServiceJobs", "deadlockWait", "deadlockCheckInterval", "deadlockWait"},
#     "resource": {"defaultMemory", "defaultCores", "defaultDisk", "defaultAccelerators", "defaultPreemptible", "maxCores", "maxMemory", "maxDisk"},
#     "jobs": {"retryCount", "enableUnlimitedPreemptibleRetries", "retryCount", "doubleMem", "maxJobDuration", "rescueJobsFrequency"},
#     "log_management": {"maxLogFileSize", "writeLogs", "writeLogsFromAllJobs", "maxLogFileSize", "writeLogsGzip", "writeLogs", "writeMessages", "realTimeLogging"},
#     "miscellaneous": {"disableChaining", "disableJobStoreChecksumVerification", "sseKey", "setEnv", "servicePollingInterval", "forceDockerAppliance", "statusWait", "disableProgress"},
#     "debug": {"debugWorker", "disableWorkerOutputCapture", "badWorker", "badWorker", "badWorkerFailInterval", "badWorkerFailInterval", "kill_polling_interval"}
# }

# def find_group(option: str) -> str:
#     for group, options in groups.items():
#         if option in options:
#             return group
#     return "other"

def main() -> None:
    # configargparse's write_config does not write options with a None value, so do this instead

    parser = ArgParser()

    parser.add_argument("output", default="config.yaml")
    options = parser.parse_args()
    logger.info("Writing a default config file to %s.", options.output)
    generate_config(os.path.abspath(options.output))

class EmptyConfig(Config):
    def set_from_default_config(self) -> None:
        # remove set_from_default_config so it doesn't try to call toil config again if the default config file does not exist
        pass

def generate_config(filepath: str) -> None:
    omit = ("help", "config", "defaultAccelerators", "nodeTypes", "nodeStorageOverrides", "setEnv", "minNodes", "maxNodes", "logCritical", "logDebug", "logError", "logInfo", "logOff", "logWarning")
    parser = ArgParser(YAMLConfigFileParser())
    config = EmptyConfig()
    addOptions(parser, config=config)
    cfg = dict()
    for action in parser._actions:
        if any(s.replace("-", "") in omit for s in action.option_strings):
            continue
        # all StoreFalseActions will have a StoreTrueActions counterpart
        # only include True action as they are the defaults
        if isinstance(action, _StoreFalseAction):
            continue
        if len(action.option_strings) == 0:
            continue
        option_string = action.option_strings[0] if action.option_strings[0].find("--") != -1 else action.option_strings[1]
        option = option_string[2:]
        # deal with
        # "--runLocalJobsOnWorkers"
        # "--runCwlInternalJobsOnWorkers"
        # as they are the same argument
        if option_string == "--runLocalJobsOnWorkers--runCwlInternalJobsOnWorkers":
            # prefer runCwlInternalJobsOnWorkers as it is included in the documentation
            option = "runCwlInternalJobsOnWorkers"

        default = action.default

        cfg[option] = default

    with open(filepath, "w") as f:
        yaml.dump(cfg, f)