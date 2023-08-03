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
import configparser
import json
import logging
import os
from collections import OrderedDict

from argparse import _StoreFalseAction, _StoreTrueAction
from typing import Set, Dict, Optional

from configargparse import ArgParser

from toil.common import Toil, parser_with_common_options, addOptions
from toil.jobStores.abstractJobStore import NoSuchJobStoreException
from toil.statsAndLogging import set_logging_from_options
logger = logging.getLogger(__name__)

# groups and arguments according to documentation
groups = {
    "core": {"workDir", "coordinationDir", "noStdOutErr", "stats", "clean", "cleanWorkDir", "clusterStats", "restart", "jobStore"},
    "logging": {"logOff", "logCritical", "logError", "logWarning", "logInfo", "logDebug", "logLevel", "logFile", "rotatingLogging", "maxLogFileSize", "log-dir"},
    "batch_systems": {"batchSystem", "disableAutoDeployment", "maxJobs", "maxLocalJobs", "manualMemArgs", "runCwlInternalJobsOnWorkers", "statePollingWait", "batchLogsDir", "parasolCommand", "parasolMaxBatches", "mesosEndpoint", "mesosFrameworkId", "mesosRole", "mesosName", "kubernetesHostPath", "kubernetesOwner", "kubernetesServiceAccount", "kubernetesPodTimeout", "tesEndpoint", "tesUser", "tesPassword", "tesBearerToken", "awsBatchRegion", "awsBatchQueue", "awsBatchJobRoleArn", "scale"},
    "data_storage": {"linkImports", "moveExports", "disableCaching", "caching"},
    "autoscaling": {"provisioner", "nodeTypes", "minNodes", "maxNodes", "targetTime", "betaInertia", "scaleInterval", "preemptibleCompensation", "nodeStorage", "nodeStorageOverrides", "metrics", "assumeZeroOverhead"},
    "service": {"maxServiceJobs", "maxPreemptibleServiceJobs", "deadlockWait", "deadlockCheckInterval", "deadlockWait"},
    "resource": {"defaultMemory", "defaultCores", "defaultDisk", "defaultAccelerators", "defaultPreemptible", "maxCores", "maxMemory", "maxDisk"},
    "jobs": {"retryCount", "enableUnlimitedPreemptibleRetries", "retryCount", "doubleMem", "maxJobDuration", "rescueJobsFrequency"},
    "log_management": {"maxLogFileSize", "writeLogs", "writeLogsFromAllJobs", "maxLogFileSize", "writeLogsGzip", "writeLogs", "writeMessages", "realTimeLogging"},
    "miscellaneous": {"disableChaining", "disableJobStoreChecksumVerification", "sseKey", "setEnv", "servicePollingInterval", "forceDockerAppliance", "statusWait", "disableProgress"},
    "debug": {"debugWorker", "disableWorkerOutputCapture", "badWorker", "badWorker", "badWorkerFailInterval", "badWorkerFailInterval", "kill_polling_interval"}
}

def find_group(option: str) -> str:
    for group, options in groups.items():
        if option in options:
            return group
    return "other"

def main() -> None:
    parser = ArgParser()
    parser.add_argument("--output", "-o", dest="output", default=os.path.join(os.getcwd(), "config.cfg"))

    # There are 3 CLI defaults not in the documentation:
    #       "disableHotDeployment": "False",
    #       "coalesceStatusCalls": "True",
    #       "allocate_mem": "True",
    # The first two are left in due to backwards compatibility
    # allocate_mem deals with an issue on slurm
    # There isn't any reason to include these in the config file as there would be little reason to change them from their defaults
    # config is also omitted as it doesn't make sense for a config file to specify itself/another cfg
    # all log level arguments are omitted except logLevel
    omit = ("output", "help", "config", "disableHotDeployment", "coalesceStatusCalls", "allocate_mem", "logCritical", "logError", "logWarning", "logDebug", "logInfo", "logOff")

    addOptions(parser, jobstore_as_flag=True)

    cfg: Dict[str, OrderedDict[str, Optional[str]]] = {}

    for action in parser._actions:
        # all StoreFalseActions will have a StoreTrueActions counterpart
        # only include True action as they are the defaults
        if isinstance(action, _StoreFalseAction):
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

        if option in omit:
            continue

        # If the default value is None, omit from the config
        # Some type specific CLI options have None as their default, and argparse will complain about trying to set
        # those options to a None type
        if not isinstance(default, (bool, int, float)):
            if default is None or len(default) == 0:
                continue

        group = find_group(option)

        cfg.setdefault(group, OrderedDict())

        if isinstance(action, _StoreTrueAction):
            # storetrue arg with default of False means it is not included in cfg
            if action.default is False:
                continue
            cfg[group][option] = None
        else:
            cfg[group][option] = str(default) if not isinstance(default, str) else default

    config_parser_obj = configparser.ConfigParser(
        allow_no_value=True,
        inline_comment_prefixes=("#",),
        strict=True,
        empty_lines_in_values=False
    )

    # preserve case
    config_parser_obj.optionxform = str # type: ignore[method-assign, assignment]

    N = parser.parse_args()
    output_path = N.output

    with open(output_path, "w") as f:
        config_parser_obj.read_dict(cfg)
        config_parser_obj.write(f)
