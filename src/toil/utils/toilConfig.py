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
import logging
import os

from configargparse import ArgParser

from toil.common import generate_config
from toil.statsAndLogging import set_logging_from_options, add_logging_options

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

def main() -> None:
    # configargparse's write_config does not write options with a None value, so do this instead

    parser = ArgParser()

    parser.add_argument("output", default="config.yaml")
    add_logging_options(parser)
    options = parser.parse_args()
    set_logging_from_options(options)
    logger.info("Writing a default config file to %s.", os.path.abspath(options.output))
    generate_config(os.path.abspath(options.output))