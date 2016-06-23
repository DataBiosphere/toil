# Copyright (C) 2016 UCSC Computational Genomics Lab
# Copyright (C) 2016 UC Berkeley AMPLab
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
import multiprocessing
import os
import subprocess
import time

from toil.job import Job

_log = logging.getLogger(__name__)
_SPARK_MASTER_PORT = "7077"

def spawn_spark_cluster(job,
                        sudo,
                        numWorkers,
                        cores=None,
                        memory=None,
                        disk=None,
                        overrideLeaderIP=None):
    '''
    :param sudo: Whether this code should run docker containers with sudo.
    :param numWorkers: The number of worker nodes to have in the cluster. \
    Must be greater than or equal to 1.
    :param cores: Optional parameter to set the number of cores per node. \
    If not provided, we use the number of cores on the node that launches \
    the service.
    :param memory: Optional parameter to set the memory requested per node.
    :param disk: Optional parameter to set the disk requested per node.
    :type sudo: boolean
    :type leaderMemory: int or string convertable by bd2k.util.humanize.human2bytes to an int
    :type numWorkers: int
    :type cores: int
    :type memory: int or string convertable by bd2k.util.humanize.human2bytes to an int
    :type disk: int or string convertable by bd2k.util.humanize.human2bytes to an int
    '''

    if numWorkers < 1:
        raise ValueError("Must have more than one worker. %d given." % numWorkers)

    leaderService = SparkService(sudo,
                                 cores=cores,
                                 memory=memory,
                                 disk=disk,
                                 overrideLeaderIP=overrideLeaderIP)
    leaderIP = job.addService(leaderService)
    for i in range(numWorkers):
        job.addService(WorkerService(leaderIP,
                                     sudo,
                                     cores=cores,
                                     disk=disk,
                                     memory=memory),
                       parentService=leaderService)

    return leaderIP

#
# FIXME: Where should this go?
# See discussion in https://github.com/BD2KGenomics/toil-scripts/pull/190.
# @benedictpaten thinks this should go in toil. If so, where in toil?
#
# @jvivian has been tagged to resolve this at a later point in time
#
def _docker_call(work_dir,
                 tool_parameters,
                 tool,
                 java_opts=None,
                 outfile=None,
                 sudo=False,
                 docker_parameters=None,
                 check_output=False,
                 no_rm=False):
    """
    Makes subprocess call of a command to a docker container.

    tool_parameters: list   An array of the parameters to be passed to the tool
    tool: str               Name of the Docker image to be used (e.g. quay.io/ucsc_cgl/samtools)
    java_opts: str          Optional commands to pass to a java jar execution. (e.g. '-Xmx15G')
    outfile: file           Filehandle that stderr will be passed to
    sudo: bool              If the user wants the docker command executed as sudo
    """
    rm = '--rm'
    if no_rm:
        rm = ''

    base_docker_call = ('docker run %s --log-driver=none -v %s:/data' % (rm, work_dir)).split()

    if sudo:
        base_docker_call = ['sudo'] + base_docker_call
    if java_opts:
        base_docker_call = base_docker_call + ['-e', 'JAVA_OPTS={}'.format(java_opts)]
    if docker_parameters:
        base_docker_call = base_docker_call + docker_parameters

    _log.warn("Calling docker with %s." % " ".join(base_docker_call + [tool] + tool_parameters))

    try:
        if outfile:
            subprocess.check_call(base_docker_call + [tool] + tool_parameters, stdout=outfile)
        else:
            if check_output:
                return subprocess.check_output(base_docker_call + [tool] + tool_parameters)
            else:
                subprocess.check_call(base_docker_call + [tool] + tool_parameters)

    except subprocess.CalledProcessError:
        raise RuntimeError('docker command returned a non-zero exit status. Check error logs.')
    except OSError:
        raise RuntimeError('docker not found on system. Install on all nodes.')


def _checkContainerStatus(sparkContainerID,
                          hdfsContainerID,
                          sparkNoun='leader',
                          hdfsNoun='namenode'):

    containers = subprocess.check_output(["docker", "ps", "-q"])

    # docker ps emits shortened versions of the hash
    # these shortened hashes are 12 characters long
    shortSpark = sparkContainerID[0:11]
    shortHdfs = hdfsContainerID[0:11]

    if ((sparkContainerID not in containers and
         shortSpark not in containers) or
        (hdfsContainerID not in containers and
         shortHdfs not in containers)):
        raise RuntimeError('Lost both Spark %s and HDFS %s.' % (sparkNoun, hdfsNoun))
    elif sparkContainerID not in containers and shortSpark not in containers:
        raise RuntimeError('Lost Spark %s. %r' % sparkNoun)
    elif hdfsContainerID not in containers and shortHdfs not in containers:
        raise RuntimeError('Lost HDFS %s. %r' % hdfsNoun)
    else:
        return True
    

class SparkService(Job.Service):
    """
    A Service job that spins up a Spark cluster that child jobs can then attach
    to. If the job that spawns this job is run with `checkpoint = True`, then
    this service will robustly restart the Spark cluster upon the loss of any
    nodes in the cluster.
    """

    def __init__(self,
                 sudo,
                 memory=None,
                 disk=None,
                 cores=None,
                 overrideLeaderIP=None):
        """
        :param sudo: Whether this code should run docker containers with sudo.
        :param memory: The amount of memory to be requested for the Spark leader. Optional.
        :param disk: The amount of disk to be requested for the Spark leader. Optional.
        :param cores: Optional parameter to set the number of cores per node. \
        If not provided, we use the number of cores on the node that launches \
        the service.
        :type sudo: boolean
        :type memory: int or string convertable by bd2k.util.humanize.human2bytes to an int
        :type disk: int or string convertable by bd2k.util.humanize.human2bytes to an int
        :type cores: int
        """
        self.sudo = sudo

        if cores is None:
            cores = multiprocessing.cpu_count()

        self.hostname = overrideLeaderIP

        Job.Service.__init__(self, memory=memory, cores=cores, disk=disk)


    def start(self, fileStore):
        """
        Start spark and hdfs master containers

        :param fileStore: Unused
        """

        if self.hostname is None:
            self.hostname = subprocess.check_output(["hostname", "-f",])[:-1]

        _log.info("Started Spark master container.")
        self.sparkContainerID = _docker_call(no_rm = True,
                                             work_dir = os.getcwd(),
                                             tool = "quay.io/ucsc_cgl/apache-spark-master:1.5.2",
                                             docker_parameters = ["--net=host",
                                                                  "-d",
                                                                  "-v", "/mnt/ephemeral/:/ephemeral/:rw",
                                                                  "-e", "SPARK_MASTER_IP="+self.hostname,
                                                                  "-e", "SPARK_LOCAL_DIRS=/ephemeral/spark/local",
                                                                  "-e", "SPARK_WORKER_DIR=/ephemeral/spark/work"],
                                             tool_parameters = [self.hostname],
                                             sudo = self.sudo,
                                             check_output = True)[:-1]
        _log.info("Started HDFS Datanode.")
        self.hdfsContainerID = _docker_call(no_rm = True,
                                            work_dir = os.getcwd(),
                                            tool = "quay.io/ucsc_cgl/apache-hadoop-master:2.6.2",
                                            docker_parameters = ["--net=host",
                                                                 "-d"],
                                            tool_parameters = [self.hostname],
                                            sudo = self.sudo,
                                            check_output = True)[:-1]

        return self.hostname


    def stop(self, fileStore):
        """
        Stop and remove spark and hdfs master containers

        fileStore: Unused
        """

        sudo = []
        if self.sudo:
            sudo = ["sudo"]
            
        subprocess.call(sudo + ["docker", "exec", self.sparkContainerID, "rm", "-r", "/ephemeral/spark"])
        subprocess.call(sudo + ["docker", "stop", self.sparkContainerID])
        subprocess.call(sudo + ["docker", "rm", self.sparkContainerID])
        _log.info("Stopped Spark master.")

        subprocess.call(sudo + ["docker", "stop", self.hdfsContainerID])
        subprocess.call(sudo + ["docker", "rm", self.hdfsContainerID])
        _log.info("Stopped HDFS namenode.")

        return


    def check(self):
        """
        Checks to see if Spark master and HDFS namenode are still running.
        """
        
        status = _checkContainerStatus(self.sparkContainerID, self.hdfsContainerID)

        return status

class WorkerService(Job.Service):
    """
    Service Job that implements the worker nodes in a Spark/HDFS cluster.
    Should not be called outside of `SparkService`.
    """
    
    def __init__(self, masterIP, sudo, memory=None, cores=None, disk=None):
        """
        :param sudo: Whether this code should run docker containers with sudo.
        :param memory: The memory requirement for each node in the cluster. Optional.
        :param disk: The disk requirement for each node in the cluster. Optional.
        :param cores: Optional parameter to set the number of cores per node. \
        If not provided, we use the number of cores on the node that launches \
        the service.
        :type sudo: boolean
        :type memory: int or string convertable by bd2k.util.humanize.human2bytes to an int
        :type disk: int or string convertable by bd2k.util.humanize.human2bytes to an int
        :type cores: int
        """

        self.masterIP = masterIP
        self.sudo = sudo

        if cores is None:
            cores = multiprocessing.cpu_count()
            
        Job.Service.__init__(self, memory=memory, cores=cores, disk=disk)


    def start(self, fileStore):
        """
        Start spark and hdfs worker containers

        :param fileStore: Unused
        """

        # start spark and our datanode
        self.sparkContainerID = _docker_call(no_rm = True,
                                             work_dir = os.getcwd(),
                                             tool = "quay.io/ucsc_cgl/apache-spark-worker:1.5.2",
                                             docker_parameters = ["--net=host", 
                                                                  "-d",
                                                                  "-v", "/mnt/ephemeral/:/ephemeral/:rw",
                                                                  "-e", "\"SPARK_MASTER_IP="+self.masterIP+":"+_SPARK_MASTER_PORT+"\"",
                                                                  "-e", "SPARK_LOCAL_DIRS=/ephemeral/spark/local",
                                                                  "-e", "SPARK_WORKER_DIR=/ephemeral/spark/work"],
                                             tool_parameters = [self.masterIP+":"+_SPARK_MASTER_PORT],
                                             sudo = self.sudo,
                                             check_output = True)[:-1]
        self.__start_datanode()
        
        # fake do/while to check if HDFS is up
        hdfs_down = True
        retries = 0
        while hdfs_down and (retries < 5):

            _log.info("Sleeping 30 seconds before checking HDFS startup.")
            time.sleep(30)
            clusterID = ""
            try:
                clusterID = check_output(["docker",
                                          "exec",
                                          self.hdfsContainerID,
                                          "grep",
                                          "clusterID",
                                          "-R",
                                          "/opt/apache-hadoop/logs"])
            except:
                # grep returns a non-zero exit code if the pattern is not found
                # we expect to not find the pattern, so a non-zero code is OK
                pass

            if "Incompatible" in clusterID:
                _log.warning("Hadoop Datanode failed to start with: %s", clusterID)
                _log.warning("Retrying container startup, retry #%d.", retries)
                retries += 1

                _log.warning("Removing ephemeral hdfs directory.")
                check_call(["docker",
                            "exec",
                            self.hdfsContainerID,
                            "rm",
                            "-rf",
                            "/ephemeral/hdfs"])

                _log.warning("Killing container %s.", self.hdfsContainerID)
                check_call(["docker",
                            "kill",
                            self.hdfsContainerID])

                # todo: this is copied code. clean up!
                _log.info("Restarting datanode.")
                self.__start_datanode()

            else:
                _log.info("HDFS datanode started up OK!")
                hdfs_down = False

        if retries >= 5:
            raise RuntimeError("Failed %d times trying to start HDFS datanode." % retries)

        return


    def __start_datanode(self):
        """
        Launches the Hadoop datanode.
        """
        self.hdfsContainerID = _docker_call(no_rm = True,
                                            work_dir = os.getcwd(),
                                            tool = "quay.io/ucsc_cgl/apache-hadoop-worker:2.6.2",
                                            docker_parameters = ["--net=host",
                                                                 "-d",
                                                                 "-v", "/mnt/ephemeral/:/ephemeral/:rw"],
                                            tool_parameters = [self.masterIP],
                                            sudo = self.sudo,
                                            check_output = True)[:-1]


    def stop(self, fileStore):
        """
        Stop spark and hdfs worker containers

        :param fileStore: Unused
        """

        sudo = []
        if self.sudo:
            sudo = ['sudo']

        subprocess.call(sudo + ["docker", "exec", self.sparkContainerID, "rm", "-r", "/ephemeral/spark"])
        subprocess.call(sudo + ["docker", "stop", self.sparkContainerID])
        subprocess.call(sudo + ["docker", "rm", self.sparkContainerID])
        _log.info("Stopped Spark worker.")

        subprocess.call(sudo + ["docker", "exec", self.hdfsContainerID, "rm", "-r", "/ephemeral/hdfs"])
        subprocess.call(sudo + ["docker", "stop", self.hdfsContainerID])
        subprocess.call(sudo + ["docker", "rm", self.hdfsContainerID])
        _log.info("Stopped HDFS datanode.")

        return


    def check(self):
        """
        Checks to see if Spark worker and HDFS datanode are still running.
        """

        status = _checkContainerStatus(self.sparkContainerID,
                                       self.hdfsContainerID,
                                       sparkNoun='worker',
                                       hdfsNoun='datanode')
        
        return status
