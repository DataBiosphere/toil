# Copyright (C) 2015 UC Berkeley AMPLab
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

from __future__ import absolute_import

# import from python system libraries
import copy
import logging
import os
import subprocess
from threading import Lock, Thread
import time
from Queue import Empty, Queue

# import from toil
from toil.batchSystems.abstractBatchSystem import AbstractBatchSystem
from toil.batchSystems.lsf import MemoryString

# create a log
_log = logging.getLogger(__name__)


class _YARNWorker(Thread):
    '''
    Helper thread to keep track of current running jobs, spin up new jobs,
    etc.
    '''

    def __init__(self,
                 newJobsQueue,
                 updatedJobsQueue,
                 yarnIDs,
                 yarnIDsLock,
                 hadoopHome,
                 subprocessLock):
        '''
        :param newJobsQueue: Multithreaded queue for passing new jobs to schedule.
        :param updatedJobsQueue: Multithreaded queue for passing status on jobs
        that have completed.
        :param yarnIDs: Dictionary mapping jobIDs to YARN application IDs.
        :param yarnIDsLock: Lock to protect yarn ID mapping dictionary.
        :param hadoopHome: Location of Hadoop
        :param subprocessLock: Lock, because subprocess is not thread safe..

        :type newJobsQueue: Queue
        :type updatedJobsQueue: Queue
        :type yarnIDs: dict
        :type yarnIDsLock: Lock
        :type hadoopHome: String
        :type subprocessLock: Lock
        '''

        Thread.__init__(self)

        self._newJobsQueue = newJobsQueue
        self._updatedJobsQueue = updatedJobsQueue
        self._yarnIDs = yarnIDs
        self._yarnIDsLock = yarnIDsLock
        self._hadoopHome = hadoopHome
        self._subprocessLock = subprocessLock

        # lock and value to coordiante shut down for worker thread
        self._run = True
        self._runLock = Lock()

        # set of yarn IDs for currently submitted jobs
        self._submittedJobs = set()


    def run(self):
        '''
        Run method for worker thread.
        '''

        import sys

        def continueRun():

            # acquire lock
            self._runLock.acquire()
            tmp = self._run
            self._runLock.release()
            
            # return temp value
            return tmp


        def submitJob(cmd):
            '''
            Submits a job to YARN and parses the output for the application ID.

            The `yarn jar` command outputs the application ID, however, once
            started, it will run and dump job status for the duration of the
            application. However, if you kill the `yarn jar` command, the
            application will continue to run. This function does the delicate
            dance around parsing the output and etc.
            
            :param cmd: Command to submit.
            :type cmd: list
            '''

            # submit job to YARN
            # note: yarn's chatter comes on stderr, not stdout
            self._subprocessLock.acquire()
            process = subprocess.Popen(cmd, stderr = subprocess.PIPE)

            # loop until we get a yarn application ID
            applicationID = None
            while True:
                
                # read a line from stderr of the yarn jar submission
                line = process.stderr.readline()

                # does this line contain the submitted YARN application ID?
                if 'Submitted application' in line:
                    
                    # if so, it is the last word in the line
                    applicationID = line.split()[-1]

                    print >> sys.stderr, 'Found ID %s in: %s' % (applicationID, line)

                    # and now, let's break out of the loop
                    break
                else:
                    print >> sys.stderr, 'Spurious: %s' % line

            for i in range(1):

                print >> sys.stderr, 'Extra: %s' % process.stderr.readline()

            # we can now kill off the yarn jar process
            #
            # if we don't, it will run and output application status for the
            # whole duration of the application
            process.kill()
            self._subprocessLock.release()

            return applicationID


        def getExitCode(yarnID):

            # get the yarn status report
            self._subprocessLock.acquire()
            status = subprocess.check_output(['%s/bin/yarn' % self._hadoopHome,
                                              'application', '-status', yarnID])
            self._subprocessLock.release()

            # split into lines and find the status
            status = status.split('\n')
            exitCode = -1
            for line in status:
                
                # is this the status line?
                if 'Final-State' in line:

                    # split the line on whitespace
                    line = line.split()

                    # status is the final word
                    if line[-1] == 'SUCCEEDED':
                        exitCode = 0
                    else:
                        exitCode = 1

            return exitCode


        # loop until we've been stopped
        while continueRun():

            # loop and submit new jobs
            while not self._newJobsQueue.empty():
                
                # pop job from queue and submit to yarn
                (jobID, cmd) = self._newJobsQueue.get()
                print >> sys.stderr, 'Submitting job %d app via %s' % (jobID, cmd)
                yarnID = submitJob(cmd)
                print >> sys.stderr, 'Received YARN ID %s' % yarnID

                # add yarn ID to map
                self._yarnIDsLock.acquire()
                self._yarnIDs[jobID] = yarnID
                self._yarnIDsLock.release()
                print >> sys.stderr, 'ID map: %s' % self._yarnIDs
                
                self._submittedJobs.add((jobID, yarnID))

            # loop and test known 
            for (jobID, yarnID) in list(self._submittedJobs):
                
                # check and see if YARN application has exited, and if so,
                # what the status code is
                status = getExitCode(yarnID)

                # if we have an exit code, add to the queue
                if status:
                    self._updatedJobsQueue.put((jobID, status))

        # upon termination, we need to clean up all running jobs
        remainingJobs = list(self._submittedJobs)
        _log.info('Worker is shutting down with %d jobs still running.',
                  len(remainingJobs))
        
        # loop and terminate applications
        for (jobID, yarnID) in remainingJobs:
            import sys
            
            try:
                cmd = ['%s/bin/yarn' % self._hadoopHome, 'application',
                       '-kill', yarnID]
                _log.info('Killing off YARN application %s (job ID: %d).',
                          yarnID, jobID)
                print >> sys.stderr, 'Killing with %r' % cmd
                
                self._subprocessLock.acquire()
                subprocess.call(cmd)
                self._subprocessLock.release()
            except Exception as e:
                print >> sys.stderr, 'caught %r' % e
                raise


    def stop(self):
        '''
        Sets flag to stop the worker thread on next work loop.
        '''

        # acquire lock and zero run flag
        self._runLock.acquire()
        self._run = False
        self._runLock.release()


class YARNBatchSystem(AbstractBatchSystem):
    """
    A Toil interface to the Apache Hadoop YARN scheduler.
    """

    def __init__(self, config, maxCores, maxMemory, maxDisk):
        AbstractBatchSystem.__init__(self, config, maxCores, maxMemory, maxDisk)

        # a lock, because subprocess is not threadsafe
        self._subprocessLock = Lock()

        if 'HADOOP_HOME' in os.environ:
            self._hadoopHome = os.environ['HADOOP_HOME']
        else:
            try:
                # get the true location of the yarn command
                yarnCmd = next(which('yarn'))
                yarnRealpath = os.realpath(yarnCmd)
                
                # yarn should be at ${HADOOP_HOME}/bin/yarn, so look two dirs up
                self._hadoopHome = os.path.dirname(os.path.dirname(yarnRealpath))
            except StopIteration:
                raise RuntimeError('$HADOOP_HOME is not set and yarn command could not be found!')
        _log.info('HADOOP_HOME is %s', self._hadoopHome)

        # run yarn to find the yarn version
        self._subprocessLock.acquire()
        yarnVersionOutput = subprocess.check_output(['%s/bin/yarn' %
                                                     self._hadoopHome, 'version'])
        self._subprocessLock.release()
        yarnVersion = yarnVersionOutput.split()[1]
        
        # check the version string
        yarnVersionSplit = yarnVersion.split('.')

        def badYarnVersion():
            '''
            Helper function for raising YARN version error.
            '''
            raise RuntimeError('Received invalid YARN version (%s).\n'
                               '"%s/bin/yarn version" returned:\n%s' % (yarnVersion,
                                                                        self._hadoopHome,
                                                                        yarnVersionOutput))
        
        # yarn version string should be formatted %d.%d.%d
        # also, yarn is hadoop 2.x+
        if len(yarnVersionSplit) != 3:
            badYarnVersion()
        else:
            try:
                yarnMajor = int(yarnVersionSplit[0])
                yarnMinor = int(yarnVersionSplit[1])
                yarnIncr = int(yarnVersionSplit[2])

                if yarnMajor == 1:
                    raise RuntimeError('YARN major version is 1 (%s --> '
                                       'major version %s).\n' % (yarnMajor,
                                                                 yarnVersion))
            except ValueError:
                badYarnVersion()

        # we will run using the yarn distributed shell, so make sure we've got
        # the jars we need for that
        self._yarnDistributedShellPath = ('%s/share/hadoop/yarn/'
                                          'hadoop-yarn-applications-distributedshell-%s.jar'
                                          % (self._hadoopHome, yarnVersion))
        if not os.path.exists(self._yarnDistributedShellPath):
            raise RuntimeError('Cannot find YARN distributed shell JAR.'
                               'Expected location: %s.' % (self._yarnDistributedShellPath))
        _log.info('Found YARN distributed shell JAR at %s.',
                  self._yarnDistributedShellPath)

        # set up worker
        self._yarnIDs = {}
        self._yarnIDsLock = Lock()
        self._newJobsQueue = Queue()
        self._updatedJobsQueue = Queue()
        self._worker = _YARNWorker(self._newJobsQueue,
                                   self._updatedJobsQueue,
                                   self._yarnIDs, self._yarnIDsLock,
                                   self._hadoopHome,
                                   self._subprocessLock)
        self._worker.setDaemon(True)
        self._worker.start()

        # set miscellaneous other state
        self._nextJobID = 0
        self._currentJobs = set()


    @classmethod
    def getRescueBatchJobFrequency(cls):

        return 10


    def _buildYARNCommand(self, cmd, memoryMB, cores, jobID):
        '''
        Builds a command for running a command on YARN using the YARN
        distributed shell JAR.
        
        :param cmd: Shell command to run.
        :param memoryMB: Amount of memory to allocate for the command, in MB.
        :param cores: Amount of cores to request for the command.
        :param jobID: Toil ID for this job.
        :type cmd: string
        :type memoryMB: int
        :type cores: int
        :type jobID: int
        '''

        # build environment
        envList = []
        if len(self.environment) > 0:
            env = ["%s=%s" % (k, v) for (k, v) in self.environment]
            env = ' '.join(env)
            envList = ['-shell_env', env]

        cmd = ['%s/bin/yarn' % self._hadoopHome, 'jar',
                self._yarnDistributedShellPath,
                # NOTE: this next line seems funny, but!
                # it is needed to set the _application master_ JAR
                # this is separate from the _application_ JAR
                #
                # TL;DR: if you remove this line, YARN will not run the job.
                '-jar', self._yarnDistributedShellPath,
                '-shell_command', cmd] + envList + ['-container_vcores', str(cores),
                '-container_memory', str(int(memoryMB)),
                '-appname', 'toil_job_%d' % jobID]
        
        return cmd

    def issueBatchJob(self, command, memory, cores, disk):
        '''
        Issues a batch job via the YARN batch scheduler.
        '''
        
        _log.warn('YARN batch system does not support disk requirements (%s given).',
                  disk)

        import sys

        # store/increment job ID
        jobID = self._nextJobID
        print >> sys.stderr, 'Issuing %s as job ID %d' % (command, jobID)
        self._nextJobID += 1

        # add to current job tracker
        self._currentJobs.add(jobID)

        # create the yarn distributed shell command
        memoryMB = MemoryString(str(memory)).megabyteVal()
        yarnCmd = self._buildYARNCommand(command, memoryMB, cores, jobID)

        # append to queue for worker to pick up
        self._newJobsQueue.put((jobID, yarnCmd))
        _log.info('Issued new job (ID: %d) as: %r',
                  jobID,
                  yarnCmd)
        print >> sys.stderr, 'Issued job ID %d. Next job ID is %d.' % (jobID, self._nextJobID)

        return jobID


    def getIssuedBatchJobIDs(self):
        '''
        Gets a list of job IDs for issued jobs.
        '''
        jobs = copy.deepcopy(self._currentJobs)

        return jobs


    def killBatchJobs(self, jobIDs):
        '''
        Kills a currently running job in YARN.
        '''

        import sys

        try:
            # loop and kill jobs
            for jobID in jobIDs:
                
                # is the job in the set of current jobs?
                assert jobID in self._currentJobs

                # get the yarn application ID
                self._yarnIDsLock.acquire()
                yarnID = self._yarnIDs[jobID]

                # issue yarn kill command
                _log.info('Killing job ID %d --> YARN application ID %s.',
                          jobID, yarnID)
                cmd = ['%s/bin/yarn' % self._hadoopHome, 'application',
                       '-kill', yarnID]
                print >> sys.stderr, 'Killing job via %r' % cmd
                self._subprocessLock.acquire()
                subprocess.call(cmd)
                self._subprocessLock.release()
                
                # clean up state
                del self._yarnIDs[jobID]
                self._yarnIDsLock.release()
                self._currentJobs.remove(jobID)
        except Exception as e:
            print >> sys.stderr, 'Caught %r' % e
            raise

            
    def getRunningBatchJobIDs(self):
        '''
        Returns a map that maps the job ID of a currently running job
        to how long that job has been running.
        '''

        idTimeMap = {}

        import sys

        print >> sys.stderr, 'Checking status of: %s' % self._currentJobs

        try:
            for jobID in self._currentJobs:

                # get the yarn application ID
                self._yarnIDsLock.acquire()
                
                # the key may not be in the id map, so check first
                print >> sys.stderr, 'Looking key for %d from %s' % (jobID, self._yarnIDs)
                if jobID in self._yarnIDs:
                    yarnID = self._yarnIDs[jobID]
                    print >> sys.stderr, 'Got ID %d -> %s' % (jobID, self._yarnIDs)
                    self._yarnIDsLock.release()
                else:
                    print >> sys.stderr, 'ID %d not found.' % jobID
                    self._yarnIDsLock.release()
                    continue
                
            # call to YARN and get the status of this job
                self._subprocessLock.acquire()
                cmd = ['%s/bin/yarn' % self._hadoopHome, 'application',
                       '-status', yarnID]
                print >> sys.stderr, 'Checking job %d/%s via %r' % (jobID, yarnID, cmd)
                yarnStatus = subprocess.check_output(cmd)
                print >> sys.stderr, yarnStatus
                self._subprocessLock.release()
                
                try:
                    # parse out status
                    splitStatus = yarnStatus.split()
                    status = None
                    startTime = None
                    
                    for idx in range(len(splitStatus)):
                        if splitStatus[idx] == 'Start-Time':
                            startTime = int(splitStatus[idx + 2])
                        elif splitStatus[idx] == 'State':
                            status = splitStatus[idx + 2]
                            
                    if status == 'RUNNING':
                        if startTime:
                            idTimeMap[jobID] = time.time() - startTime
                        else:
                            _log.warning('Job ID %d (YARN application %s) was running,'
                                         ' but we couldn\'t parse the start time:\n%s',
                                         jobID, yarnID, yarnStatus)

                except:
                    _log.warning('When doing status check on job ID %d (YARN '
                                 'application %s), failed to parse status:\n%s',
                                 jobID, yarnID, yarnStatus)
        except Exception as e:
            print >> sys.stderr, 'Failed with %r' % e
            raise
                        
        return idTimeMap


    def getUpdatedBatchJob(self, maxWait):
        '''
        Returns the status of a completed batch job.
        
        A limitation of the YARN implementation is that it only returns the
        correct job ID if the job completed successfully. For a failure, we
        return 1 because YARN doesn't report the status code for the
        application

        :param maxWait: The maximum amount of time to wait.
        :type maxWait: int.
        '''
        
        try:
            i = None
            
            # try to get a job from the queue
            try:
                (jobID, retCode) = self._updatedJobsQueue.get(timeout = maxWait)
                self._currentJobs -= set([jobID])
                i = (jobID, retCode)
            except Empty:
                pass

            return i
        except Exception as e:
            import sys
            print >> sys.stderr, 'Caught %r' % e
            raise


    def shutdown(self):
        '''
        Method to clean up the worker thread.
        Sets the stop flag, and waits for the thread to complete.
        '''

        self._worker.stop()
        self._worker.join()


class _YARNTestSupportThread(Thread):


    def __init__(self, cmd):
        Thread.__init__(self)            
        self._cmd = cmd
        self._process = None


    def run(self):
            
        self._process = subprocess.Popen(self._cmd)


    def stop(self):

        if self._process != None:
            self._process.kill()


class YARNTestSupport(object):
    '''
    A mixin for test cases that need a running YARN resource manager and
    node manager on the localhost.
    '''

    def _startYARN(self):
        '''
        Start the YARN Resource and Node managers.
        '''
        self._hadoopHome = os.environ['HADOOP_HOME']
        self.rm = _YARNTestSupportThread(['%s/bin/yarn' % self._hadoopHome,
                                          'resourcemanager'])
        self.nm = _YARNTestSupportThread(['%s/bin/yarn' % self._hadoopHome,
                                          'nodemanager'])
        self.rm.start()
        time.sleep(5)
        self.nm.start()
        time.sleep(5)

    def _stopYARN(self):
        '''
        Stop the YARN Resource and Node managers.
        '''
        
        self.rm.stop()
        self.nm.stop()
        self.rm.join()
        self.nm.join()

        time.sleep(5)
