# Copyright (C) 2015 UCSC Computational Genomics Lab
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
realtimeLogger.py: Implements a real-time UDP-based logging system that user
scripts can use for debugging.

"""

from __future__ import absolute_import

import toil.lib.bioio

import sys
import os
import os.path
import json
import collections
import logging
import logging.handlers
import SocketServer
import socket
import threading

class LoggingDatagramHandler(SocketServer.BaseRequestHandler):
    """
    Receive logging messages from the jobs and display them on the master.
    
    Uses bare JSON message encoding.
    """
    
    def handle(self):
        """
        Handle a single message. SocketServer takes care of splitting out the
        messages.
        
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
            logging.error("Malformed log message from {}".format(
                self.client_address[0]))
            
        # Log level filtering should have been done on the remote end. The
        # handle() method skips it on this end.
        logging.getLogger("remote").handle(record)
            
class JSONDatagramHandler(logging.handlers.DatagramHandler):
    """
    Send logging records over UDP serialized as JSON.
    
    They have to fit in a single UDP datagram, so don't try to log more than
    64kb at once.
    """
    
    def makePickle(self, record):
        """
        Actually, encode the record as bare JSON instead.
        """
        
        return json.dumps(record.__dict__)
        
class RealtimeLoggerMetaclass(type):
    """
    Metaclass for RealtimeLogger that lets you do things like
    RealtimeLogger.warning(), RealtimeLogger.info(), etc.
    
    """
    
    def __getattr__(self, name):
        """
        If a real attribute can't be found, try one of the logging methods on
        the actual logger object.
        
        """
        
        return getattr(self.get_logger(), name)
    
        
class RealtimeLogger(object):
    """
    All-static class for getting a logger that logs over UDP to the master.
    
    Usage:
    
    1. Make sure Job.Runner.startToil() is running on your master.
    
    2. From a running job on a worker, do:
    
    from toil.realtimeLogger import RealtimeLogger
    RealtimeLogger.info("This logging message goes straight to the master")
    
    """
    
    # Enable RealtimeLogger.info() syntactic sugar
    __metaclass__ = RealtimeLoggerMetaclass
    
    # Also the logger
    logger = None
    
    # The master keeps a server and thread
    logging_server = None
    server_thread = None
  
    @classmethod
    def start_master(cls, level="INFO"):
        """
        Start up the master server and put its details into the options
        namespace.
        
        Python logging should have already been configured.
        
        Takes an optional log level, as a string level name, from the set
        supported by bioio.
        
        """
        
        # Start up the logging server
        cls.logging_server = SocketServer.ThreadingUDPServer(("0.0.0.0", 0),
            LoggingDatagramHandler)
            
        # Set up a thread to do all the serving in the background and exit when
        # we do
        cls.server_thread = threading.Thread(
            target=cls.logging_server.serve_forever)
        cls.server_thread.daemon = True
        cls.server_thread.start()
        
        # Set options for logging in the environment so they get sent out to
        # jobs
        os.environ["TOIL_RT_LOGGING_HOST"] = socket.getfqdn()
        os.environ["TOIL_RT_LOGGING_PORT"] = str(
            cls.logging_server.server_address[1])
        os.environ["TOIL_RT_LOGGING_LEVEL"] = level
        
        
    @classmethod
    def stop_master(cls):
        """
        Stop the server on the master.
        
        """
        
        cls.logging_server.shutdown()
        cls.server_thread.join()
  
    @classmethod
    def get_logger(cls):
        """
        Get the logger that logs to the master.
        
        Note that if the master logs here, you will see the message twice,
        since it still goes to the normal log handlers too.
        """
        
        if cls.logger is None:
            # Only do the setup once, so we don't add a handler every time we
            # log
            cls.logger = logging.getLogger('realtime')
            
            if os.environ.has_key("TOIL_RT_LOGGING_LEVEL"):
                # Adopt the logging level set on the master.
                toil.lib.bioio.setLogLevel(os.environ["TOIL_RT_LOGGING_LEVEL"],
                    cls.logger)
            
            if (os.environ.has_key("TOIL_RT_LOGGING_HOST") and
                os.environ.has_key("TOIL_RT_LOGGING_PORT")):
                # We know where to send messages to, so send them.
            
                cls.logger.addHandler(JSONDatagramHandler(
                    os.environ["TOIL_RT_LOGGING_HOST"],
                    int(os.environ["TOIL_RT_LOGGING_PORT"])))
        
        return cls.logger

