#!/usr/bin/env python

# Glenn Hickey 2011
#
#Released under the MIT license, see LICENSE.txt
"""
TODO: This approach needs to be refactored
launch a command as a daemon. 

mostly copied from (and see for comments & explanation):

########################################################################
Copyright (C) 2005 Chad J. Schroeder
http://code.activestate.com/recipes/278731/

Disk And Execution MONitor (Daemon)

Configurable daemon behaviors:

   1.) The current working directory set to the "/" directory.
   2.) The current file creation mode mask set to 0.
   3.) Close all open files (1024). 
   4.) Redirect standard I/O streams to "/dev/null".

A failed call to fork() now raises an exception.

References:
   1) Advanced Programming in the Unix Environment: W. Richard Stevens
   2) Unix Programming Frequently Asked Questions:
         http://www.erlenstar.demon.co.uk/unix/faq_toc.html
########################################################################

handy to break out of a toil dependence (which the regular
os.system and subprocess.Popen can't do on their own)

takes single argument: the command line to execute
be careful: the command line is not executed in a shell

example:  sonLib_daemonize.py 'ktserver -port 26'

"""

from __future__ import absolute_import
import os
import sys
import resource
import signal
import subprocess

# Default daemon parameters.
# File mode creation mask of the daemon.
# use sonLib.bioio.spawnDaemon() for a python interface
 
UMASK = 0

# Default working directory for the daemon.
WORKDIR = "/"

# Default maximum for the number of available file descriptors.
MAXFD = 1024

# The standard I/O file descriptors are redirected to /dev/null by default.
if (hasattr(os, "devnull")):
   REDIRECT_TO = os.devnull
else:
   REDIRECT_TO = "/dev/null"
   
if __name__ == '__main__':
    if len(sys.argv) != 2:
        raise Exception, "%s: Wrong number of arguments" % sys.argv[0]
    
    pid = os.fork()
    if pid > 0:
        os._exit(0)
    
    os.chdir("/")
    os.setsid()
    
    signal.signal(signal.SIGHUP, signal.SIG_IGN)
    os.umask(0)
    pid = os.fork()
    if pid > 0:
        os._exit(0)
    
    maxfd = resource.getrlimit(resource.RLIMIT_NOFILE)[1]
    if (maxfd == resource.RLIM_INFINITY):
        maxfd = MAXFD
  
    # Iterate through and close all file descriptors.
    for fd in range(0, maxfd):
        try:
            os.close(fd)
        except OSError:    # ERROR, fd wasn't open to begin with (ignored)
            pass

    # Redirect the standard I/O file descriptors to the specified file.  Since
    os.open(REDIRECT_TO, os.O_RDWR)    # standard input (0)

    # Duplicate standard input to standard output and standard error.
    os.dup2(0, 1)            # standard output (1)
    os.dup2(0, 2)            # standard error (2)
    
    retVal = subprocess.call(sys.argv[1].split(), shell=False, bufsize=-1)
    sys.exit(retVal) 
