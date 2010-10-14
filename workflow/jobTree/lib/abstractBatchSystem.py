#!/usr/bin/env python

class AbstractBatchSystem:
    """An abstract (as far as python currently allows) base class
    to represent the interface the batch system must provide to the jobTree.
    """
    def __init__(self, config): 
        """This method must be called.
        The config object is setup by the jobTreeSetup script and
        has configuration parameters for the job tree. You can add stuff
        to that script to get parameters for your batch system.
        """
        self.config = config
    
    def issueJobs(self, commands):
        """Issues the following commands, returning a map of commands
        and jobIDs. Each command is a tuple of the (command, memory, cpu, log-file), where command
        is the string to run, memory is an int giving
        the number of bytes the job needs to run in and cpu is the number of cpus needed for
        the job and error-file is the path of the file to place any std-err/std-out in.
        """
        raise RuntimeError("Abstract method")
    
    def killJobs(self, jobIDs):
        """Kills the given job IDs.
        """
        raise RuntimeError("Abstract method")
    
    def getIssuedJobIDs(self):
        """A list of jobs (as jobIDs) currently issued (may be running, or maybe 
        just waiting).
        """
        raise RuntimeError("Abstract method")
    
    def getRunningJobIDs(self):
        """Gets a map of jobs (as jobIDs) currently running (not just waiting) 
        and a how long they have been running for (in seconds).
        """
        raise RuntimeError("Abstract method")
    
    def getUpdatedJobs(self):
        """Gets a list of jobs that have updated there status,
        according to the job manager. The return value is a 
        map of jobIDs and exit values of the processes.
        """
        raise RuntimeError("Abstract method")
    
    def getWaitDuration(self):
        """Gets the period of time (floating point, seconds) to pause after updating the running jobs.
        This gives the batch system a bit of a breather, and can avoid to much
        IO thrashing. This duration may be overidden by a command line input.
        """
        raise RuntimeError("Abstract method")
    
    def getRescueJobFrequency(self):
        """Gets the period of time to wait (floating point, in seconds) between checking for 
        missing/overlong jobs.
        """
        raise RuntimeError("Abstract method")

def main():
    pass

def _test():
    import doctest      
    return doctest.testmod()

if __name__ == '__main__':
    _test()
    main()
