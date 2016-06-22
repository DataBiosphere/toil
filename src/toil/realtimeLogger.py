# Copyright (C) 2015-2016 Regents of the University of California
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

"""
Implements a real-time UDP-based logging system that user scripts can use for debugging.
"""

from __future__ import absolute_import
import os
import os.path
import json
import logging
import logging.handlers
import SocketServer
import socket
import threading

import toil.lib.bioio

log = logging.getLogger(__name__)


class LoggingDatagramHandler(SocketServer.BaseRequestHandler):
    """
    Receive logging messages from the jobs and display them on the leader.
    
    Uses bare JSON message encoding.
    """

    def handle(self):
        """
        Handle a single message. SocketServer takes care of splitting out the messages.
        
        Messages are JSON-encoded logging module records.
        """
        # Unpack the data from the request
        data, socket = self.request
        try:
            # Parse it as JSON
            message_attrs = json.loads(data)
            # Fluff it up into a proper logging record
            record = logging.makeLogRecord(message_attrs)
        except:
            # Complain someone is sending us bad logging data
            logging.error("Malformed log message from {}".format(self.client_address[0]))
        else:
            # Log level filtering should have been done on the remote end. The handle() method
            # skips it on this end.
            log.handle(record)


class JSONDatagramHandler(logging.handlers.DatagramHandler):
    """
    Send logging records over UDP serialized as JSON.
    
    They have to fit in a single UDP datagram, so don't try to log more than 64kb at once.
    """

    def makePickle(self, record):
        """
        Actually, encode the record as bare JSON instead.
        """
        return json.dumps(record.__dict__)


class RealtimeLoggerMetaclass(type):
    """
    Metaclass for RealtimeLogger that lets you do things like RealtimeLogger.warning(),
    RealtimeLogger.info(), etc.
    """

    def __getattr__(self, name):
        """
        If a real attribute can't be found, try one of the logging methods on the actual logger
        object.
        """
        return getattr(self.getLogger(), name)


class RealtimeLogger(object):
    """
    Provides a logger that logs over UDP to the leader. To use in a Toil job, do:

    >>> from toil.realtimeLogger import RealtimeLogger
    >>> RealtimeLogger.info("This logging message goes straight to the leader")

    That's all a user of Toil would need to do. On the leader, Job.Runner.startToil()
    automatically starts the UDP server by using an instance of this class as a context manager.
    """
    # Enable RealtimeLogger.info() syntactic sugar
    __metaclass__ = RealtimeLoggerMetaclass

    # The names of all environment variables used by this class are prefixed with this string
    envPrefix = "TOIL_RT_LOGGING_"

    # Avoid duplicating the default level everywhere
    defaultLevel = 'INFO'

    # State maintained on server and client

    lock = threading.RLock()

    # Server-side state

    # The leader keeps a server and thread
    loggingServer = None
    serverThread = None

    initialized = 0

    # Client-side state

    logger = None

    @classmethod
    def _startLeader(cls, batchSystem, level=defaultLevel):
        with cls.lock:
            if cls.initialized == 0:
                cls.initialized += 1
                if level:
                    log.info('Starting real-time logging.')
                    # Start up the logging server
                    cls.loggingServer = SocketServer.ThreadingUDPServer(
                            server_address=('0.0.0.0', 0),
                            RequestHandlerClass=LoggingDatagramHandler)

                    # Set up a thread to do all the serving in the background and exit when we do
                    cls.serverThread = threading.Thread(target=cls.loggingServer.serve_forever)
                    cls.serverThread.daemon = True
                    cls.serverThread.start()

                    # Set options for logging in the environment so they get sent out to jobs
                    fqdn = socket.getfqdn()
                    try:
                        ip = socket.gethostbyname(fqdn)
                    except socket.gaierror:
                        # FIXME: Does this only happen for me? Should we librarize the work-around?
                        import platform
                        if platform.system() == 'Darwin' and '.' not in fqdn:
                            ip = socket.gethostbyname(fqdn + '.local')
                        else:
                            raise
                    port = cls.loggingServer.server_address[1]

                    def _setEnv(name, value):
                        name = cls.envPrefix + name
                        os.environ[name] = value
                        batchSystem.setEnv(name)

                    _setEnv('ADDRESS', '%s:%i' % (ip, port))
                    _setEnv('LEVEL', level)
                else:
                    log.info('Real-time logging disabled')
            else:
                if level:
                    log.warn('Ignoring nested request to start real-time logging')

    @classmethod
    def _stopLeader(cls):
        """
        Stop the server on the leader.
        """
        with cls.lock:
            assert cls.initialized > 0
            cls.initialized -= 1
            if cls.initialized == 0:
                if cls.loggingServer:
                    log.info('Stopping real-time logging server.')
                    cls.loggingServer.shutdown()
                    cls.loggingServer = None
                if cls.serverThread:
                    log.info('Joining real-time logging server thread.')
                    cls.serverThread.join()
                    cls.serverThread = None
                for k in os.environ.keys():
                    if k.startswith(cls.envPrefix):
                        os.environ.pop(k)

    @classmethod
    def getLogger(cls):
        """
        Get the logger that logs real-time to the leader.
        
        Note that if the returned logger is used on the leader, you will see the message twice,
        since it still goes to the normal log handlers, too.
        """
        # Only do the setup once, so we don't add a handler every time we log. Use a lock to do
        # so safely even if we're being called in different threads. Use double-checked locking
        # to reduce the overhead introduced by the lock.
        if cls.logger is None:
            with cls.lock:
                if cls.logger is None:
                    cls.logger = logging.getLogger('toil-rt')
                    try:
                        level = os.environ[cls.envPrefix + 'LEVEL']
                    except KeyError:
                        # There is no server running on the leader, so suppress most log messages
                        # and skip the UDP stuff.
                        cls.logger.setLevel(logging.CRITICAL)
                    else:
                        # Adopt the logging level set on the leader.
                        toil.lib.bioio.setLogLevel(level, cls.logger)
                        try:
                            address = os.environ[cls.envPrefix + 'ADDRESS']
                        except KeyError:
                            pass
                        else:
                            # We know where to send messages to, so send them.
                            host, port = address.split(':')
                            cls.logger.addHandler(JSONDatagramHandler(host, int(port)))
        return cls.logger

    def __init__(self, batchSystem, level=defaultLevel):
        """
        A context manager that starts up the UDP server.

        Should only be invoked on the leader. Python logging should have already been configured.
        This method takes an optional log level, as a string level name, from the set supported
        by bioio. If the level is None, False or the empty string, real-time logging will be
        disabled, i.e. no UDP server will be started on the leader and log messages will be
        suppressed on the workers. Note that this is different from passing level='OFF',
        which is equivalent to level='CRITICAL' and does not disable the server.
        """
        super(RealtimeLogger, self).__init__()
        self.__level = level
        self.__batchSystem = batchSystem

    def __enter__(self):
        RealtimeLogger._startLeader(self.__batchSystem, level=self.__level)

    # noinspection PyUnusedLocal
    def __exit__(self, exc_type, exc_val, exc_tb):
        RealtimeLogger._stopLeader()
