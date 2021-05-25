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
import datetime
import logging
import pipes
import socket
import subprocess
import time
from itertools import count

from toil.lib.memoize import parse_iso_utc

a_short_time = 5

logger = logging.getLogger(__name__)


class Node:
    maxWaitTime = 7 * 60

    def __init__(self, publicIP, privateIP, name, launchTime, nodeType, preemptable, tags=None):
        self.publicIP = publicIP
        self.privateIP = privateIP
        self.effectiveIP = self.publicIP or self.privateIP
        self.name = name
        self.launchTime = launchTime
        self.nodeType = nodeType
        self.preemptable = preemptable
        self.tags = tags

    def __str__(self):
        return "%s at %s" % (self.name, self.effectiveIP)

    def __repr__(self):
        return str(self)

    def __hash__(self):
        return hash(self.effectiveIP)

    def remainingBillingInterval(self):
        """
        If the node has a launch time, this function returns a floating point value
        between 0 and 1.0 representing how far we are into the
        current billing cycle for the given instance. If the return value is .25, we are one
        quarter into the billing cycle, with three quarters remaining before we will be charged
        again for that instance.

        Assumes a billing cycle of one hour.

        :return: Float from 0 -> 1.0 representing percentage of pre-paid time left in cycle.
        """
        if self.launchTime:
            now = datetime.datetime.utcnow()
            delta = now - parse_iso_utc(self.launchTime)
            return 1 - delta.total_seconds() / 3600.0 % 1.0
        else:
            return 1

    def waitForNode(self, role, keyName='core'):
        self._waitForSSHPort()
        # wait here so docker commands can be used reliably afterwards
        self._waitForSSHKeys(keyName=keyName)
        self._waitForDockerDaemon(keyName=keyName)
        self._waitForAppliance(role=role, keyName=keyName)

    def copySshKeys(self, keyName):
        """ Copy authorized_keys file to the core user from the keyName user."""
        if keyName == 'core':
            return  # No point.

        # Make sure that keys are there.
        self._waitForSSHKeys(keyName=keyName)

        # copy keys to core user so that the ssh calls will work
        # - normal mechanism failed unless public key was in the google-ssh format
        # - even so, the key wasn't copied correctly to the core account
        keyFile = '/home/%s/.ssh/authorized_keys' % keyName
        self.sshInstance('/usr/bin/sudo', '/usr/bin/cp', keyFile, '/home/core/.ssh', user=keyName)
        self.sshInstance('/usr/bin/sudo', '/usr/bin/chown', 'core', '/home/core/.ssh/authorized_keys', user=keyName)

    def injectFile(self, fromFile, toFile, role):
        """
        rysnc a file to the container with the given role
        """
        maxRetries = 10
        for retry in range(maxRetries):
            try:
                self.coreRsync([fromFile, ":" + toFile], applianceName=role)
                return True
            except Exception as e:
                logger.debug("Rsync to new node failed, trying again. Error message: %s" % e)
                time.sleep(10 * retry)
        raise RuntimeError("Failed to inject file %s to %s with ip %s" % (fromFile, role, self.effectiveIP))

    def extractFile(self, fromFile, toFile, role):
        """
        rysnc a file from the container with the given role
        """
        maxRetries = 10
        for retry in range(maxRetries):
            try:
                self.coreRsync([":" + fromFile, toFile], applianceName=role)
                return True
            except Exception as e:
                logger.debug("Rsync from new node failed, trying again. Error message: %s" % e)
                time.sleep(10 * retry)
        raise RuntimeError("Failed to extract file %s from %s with ip %s" % (fromFile, role, self.effectiveIP))

    def _waitForSSHKeys(self, keyName='core'):
        # the propagation of public ssh keys vs. opening the SSH port is racey, so this method blocks until
        # the keys are propagated and the instance can be SSH into
        start_time = time.time()
        last_error = None
        while True:
            if time.time() - start_time > self.maxWaitTime:
                raise RuntimeError(f"Key propagation failed on machine with ip {self.effectiveIP}." +
                                   ("\n\nMake sure that your public key is attached to your account and you are using "
                                    "the correct private key. If you are using a key with a passphrase, be sure to "
                                    "set up ssh-agent. For details, refer to "
                                    "https://toil.readthedocs.io/en/latest/running/cloud/cloud.html."
                                    if last_error and 'Permission denied' in last_error else ""))
            try:
                logger.info('Attempting to establish SSH connection...')
                self.sshInstance('ps', sshOptions=['-oBatchMode=yes'], user=keyName)
            except RuntimeError as err:
                last_error = str(err)
                logger.info('Connection rejected, waiting for public SSH key to be propagated. Trying again in 10s.')
                time.sleep(10)
            else:
                logger.info('...SSH connection established.')
                return

    def _waitForDockerDaemon(self, keyName='core'):
        logger.info('Waiting for docker on %s to start...', self.effectiveIP)
        sleepTime = 10
        startTime = time.time()
        while True:
            if time.time() - startTime > self.maxWaitTime:
                raise RuntimeError("Docker daemon failed to start on machine with ip %s" % self.effectiveIP)
            try:
                output = self.sshInstance('/usr/bin/ps', 'auxww', sshOptions=['-oBatchMode=yes'], user=keyName)
                if b'dockerd' in output:
                    # docker daemon has started
                    logger.info('Docker daemon running')
                    break
                else:
                    logger.info('... Still waiting for docker daemon, trying in %s sec...' % sleepTime)
                    time.sleep(sleepTime)
            except RuntimeError:
                logger.info("Wait for docker daemon failed ssh, trying again.")
                sleepTime += 20

    def _waitForAppliance(self, role, keyName='core'):
        logger.info('Waiting for %s Toil appliance to start...', role)
        sleepTime = 20
        startTime = time.time()
        while True:
            if time.time() - startTime > self.maxWaitTime:
                raise RuntimeError("Appliance failed to start on machine with IP: " + self.effectiveIP +
                                   "\nCheck if TOIL_APPLIANCE_SELF is set correctly and the container exists.")
            try:
                output = self.sshInstance('/usr/bin/docker', 'ps', sshOptions=['-oBatchMode=yes'], user=keyName)

                role = bytes(role, encoding='utf-8') if type(role) != type(output) else role

                if role in output:
                    logger.info('...Toil appliance started')
                    break
                else:
                    logger.info('...Still waiting for appliance, trying again in %s sec...' % sleepTime)
                    logger.debug(f'Role: {role}\n'
                                 f'Output: {output}\n\n')
                    time.sleep(sleepTime)
            except RuntimeError:
                # ignore exceptions, keep trying
                logger.info("Wait for appliance failed ssh, trying again.")
                sleepTime += 20

    def _waitForSSHPort(self):
        """
        Wait until the instance represented by this box is accessible via SSH.

        :return: the number of unsuccessful attempts to connect to the port before a the first
        success
        """
        logger.debug('Waiting for ssh port on %s to open...', self.effectiveIP)
        for i in count():
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                s.settimeout(a_short_time)
                s.connect((self.effectiveIP, 22))
                logger.debug('...ssh port open')
                return i
            except socket.error:
                pass
            finally:
                s.close()

    def sshAppliance(self, *args, **kwargs):
        """
        :param args: arguments to execute in the appliance
        :param kwargs: tty=bool tells docker whether or not to create a TTY shell for
            interactive SSHing. The default value is False. Input=string is passed as
            input to the Popen call.
        """
        kwargs['appliance'] = True
        return self.coreSSH(*args, **kwargs)

    def sshInstance(self, *args, **kwargs):
        """
        Run a command on the instance.
        Returns the binary output of the command.
        """
        kwargs['collectStdout'] = True
        return self.coreSSH(*args, **kwargs)

    def coreSSH(self, *args, **kwargs):
        """
        If strict=False, strict host key checking will be temporarily disabled.
        This is provided as a convenience for internal/automated functions and
        ought to be set to True whenever feasible, or whenever the user is directly
        interacting with a resource (e.g. rsync-cluster or ssh-cluster). Assumed
        to be False by default.

        kwargs: input, tty, appliance, collectStdout, sshOptions, strict

        :param bytes input: UTF-8 encoded input bytes to send to the command

        """
        commandTokens = ['ssh', '-tt']
        if not kwargs.pop('strict', False):
            kwargs['sshOptions'] = ['-oUserKnownHostsFile=/dev/null', '-oStrictHostKeyChecking=no'] + kwargs.get(
                'sshOptions', [])
        sshOptions = kwargs.pop('sshOptions', None)
        # Forward ports:
        # 5050 for Mesos dashboard (although to talk to agents you will need a proxy)
        commandTokens.extend(['-L', '5050:localhost:5050'])
        if sshOptions:
            # add specified options to ssh command
            assert isinstance(sshOptions, list)
            commandTokens.extend(sshOptions)
        # specify host
        user = kwargs.pop('user', 'core')  # CHANGED: Is this needed?
        commandTokens.append('%s@%s' % (user, str(self.effectiveIP)))

        inputString = kwargs.pop('input', None)
        if inputString is not None:
            kwargs['stdin'] = subprocess.PIPE

        if kwargs.pop('collectStdout', None):
            kwargs['stdout'] = subprocess.PIPE
        kwargs['stderr'] = subprocess.PIPE

        tty = kwargs.pop('tty', None)
        if kwargs.pop('appliance', None):
            ttyFlag = '-t' if tty else ''
            commandTokens += ['docker', 'exec', '-i', ttyFlag, 'toil_leader']

        logger.debug('Node %s: %s', self.effectiveIP, ' '.join(args))
        args = list(map(pipes.quote, args))
        commandTokens += args
        logger.debug('Full command %s', ' '.join(commandTokens))
        process = subprocess.Popen(commandTokens, **kwargs)
        stdout, stderr = process.communicate(input=inputString)
        # at this point the process has already exited, no need for a timeout
        exit_code = process.returncode
        # ssh has been throwing random 255 errors - why?
        if exit_code != 0:
            logger.info('Executing the command "%s" on the appliance returned a non-zero '
                        'exit code %s with stdout %s and stderr %s'
                        % (' '.join(args), exit_code, stdout, stderr))
            raise RuntimeError('Executing the command "%s" on the appliance returned a non-zero '
                               'exit code %s with stdout %s and stderr %s'
                               % (' '.join(args), exit_code, stdout, stderr))
        return stdout

    def coreRsync(self, args, applianceName='toil_leader', **kwargs):
        remoteRsync = "docker exec -i %s rsync -v" % applianceName  # Access rsync inside appliance
        parsedArgs = []
        sshCommand = "ssh"
        if not kwargs.pop('strict', False):
            sshCommand = "ssh -oUserKnownHostsFile=/dev/null -oStrictHostKeyChecking=no"
        hostInserted = False
        # Insert remote host address
        for i in args:
            if i.startswith(":") and not hostInserted:
                user = kwargs.pop('user', 'core')  # CHANGED: Is this needed?
                i = ("%s@%s" % (user, self.effectiveIP)) + i
                hostInserted = True
            elif i.startswith(":") and hostInserted:
                raise ValueError("Cannot rsync between two remote hosts")
            parsedArgs.append(i)
        if not hostInserted:
            raise ValueError("No remote host found in argument list")
        command = ['rsync', '-e', sshCommand, '--rsync-path', remoteRsync]
        logger.debug("Running %r.", command + parsedArgs)

        return subprocess.check_call(command + parsedArgs)
