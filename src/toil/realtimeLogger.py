#!/usr/bin/env python

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

import sys
import os
import os.path
import json
import collections
import logging
import logging.handlers
import SocketServer
import struct
import socket
import threading

class LoggingDatagramHandler(SocketServer.DatagramRequestHandler):
    """
    Receive logging messages from the jobs and display them on the master.
    
    Uses length-prefixed JSON message encoding.
    """
    
    def handle(self):
        """
        Handle messages coming in over self.connection.
        
        Messages are 4-byte-length-prefixed JSON-encoded logging module records.
        """
        
        while True:
            # Loop until we run out of messages
        
            # Parse the length
            length_data = self.rfile.read(4)
            if len(length_data) < 4:
                # The connection was closed, or we didn't get enough data
                # TODO: complain?
                break
                
            # Actually parse the length
            length = struct.unpack(">L", length_data)[0]
            
            # This is where we'll put the received message
            message_parts = []
            length_received = 0
            while length_received < length:
                # Keep trying to get enough data
                part = self.rfile.read(length - length_received)
                
                length_received += len(part)
                message_parts.append(part)
                
            # Stitch it all together
            message = "".join(message_parts)

            try:
            
                # Parse it as JSON
                message_attrs = json.loads(message)
                
                # Fluff it up into a proper logging record
                record = logging.makeLogRecord(message_attrs)
            except:
                logging.error("Malformed record")
                
            # TODO: do log level filtering
            logging.getLogger("remote").handle(record)
            
class JSONDatagramHandler(logging.handlers.DatagramHandler):
    """
    Send logging records over UDP serialized as JSON.
    """
    
    def makePickle(self, record):
        """
        Actually, encode the record as length-prefixed JSON instead.
        """
        
        json_string = json.dumps(record.__dict__)
        length = struct.pack(">L", len(json_string))
        
        return length + json_string
        
class RealtimeLogger(object):
    """
    All-static class for getting a logger that logs over UDP to the master.
    
    Usage:
    
    1. Make sure Job.Runner.startToil() is running on your master.
    
    2. From a running job on a worker, do:
    
    from toil.realtimeLogger import RealtimeLogger
    realtime = RealtimeLogger.get()
    realtime.info("This is a logging message that goes straight to the master")
    
    """
    
    # Also the logger
    logger = None
    
    # The master keeps a server and thread
    logging_server = None
    server_thread = None
  
    @classmethod
    def start_master(cls):
        """
        Start up the master server and put its details into the options
        namespace.
        
        Python logging should have already been configured.
        
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
        
        # Set options for logging in the class and the options namespace
        # Save them in the environment so they get sent out to jobs
        os.environ["RT_LOGGING_HOST"] = socket.getfqdn()
        os.environ["RT_LOGGING_PORT"] = str(
            cls.logging_server.server_address[1])
        
        
    @classmethod
    def stop_master(cls):
        """
        Stop the server on the master.
        
        """
        
        cls.logging_server.shutdown()
        cls.server_thread.join()
  
    @classmethod
    def get(cls):
        """
        Get the logger that logs to master.
        
        Note that if the master logs here, you will see the message twice,
        since it still goes to the normal log handlers too.
        """
        
        if cls.logger is None:
            # Only do the setup once, so we don't add a handler every time we
            # log
            cls.logger = logging.getLogger('realtime')
            cls.logger.setLevel(logging.INFO)
            
            if (os.environ.has_key("RT_LOGGING_HOST") and
                os.environ.has_key("RT_LOGGING_PORT")):
                # We know where to send messages to, so send them.
            
                cls.logger.addHandler(JSONDatagramHandler(
                    os.environ["RT_LOGGING_HOST"],
                    int(os.environ["RT_LOGGING_PORT"])))
        
        return cls.logger

