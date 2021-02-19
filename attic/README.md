#Toil
Python based pipeline management software for clusters that makes running recursive and dynamically scheduled computations straightforward. So far works with gridEngine, lsf, parasol and on multi-core machines.

##Authors
[Benedict Paten](https://github.com/benedictpaten/), [Dent Earl](https://github.com/dentearl/), [Daniel Zerbino](https://github.com/dzserbino/), [Glenn Hickey](https://github.com/glennhickey/), other UCSC people.

##Requirements

* Python 2.5 or later, but less than 3.0

##Installation

1. Install sonLib. See https://github.com/benedictpaten/sonLib

2. Place the directory containing the toil in the same directory as sonLib.
The directory containing both sonLib and toil should be on your python path. i.e.
PYTHONPATH=${PYTHONPATH}:FOO where FOO/toil is the path containing the base directory of toil.

3. Build the code:
Type 'make all' in the base directory, this just puts some stuff that is currently all python based in the bin dir. In the future there might be some actual compilation.

4. Test the code:
	python allTests.py or 'make test'.

##Running and examining a toil script

The following walks through running a toil script and using the command-line tools **toilStatus**, **toilRun** and **toilStats**, which are used to analyse the status, restart and print performance statistics, respectively, about a run.

Once toil is installed, running a toil script is performed by executing the script from the command-line, e.g. (using the file sorting toy example in **tests/sort/scriptTreeTest_Sort.py**):

<code>[]$ scriptTreeTest_Sort.py --fileToSort foo --toil bar/toil --batchSystem parasol --logLevel INFO --stats</code>

Which in this case uses the parasol batch system, and INFO level logging and where foo is the file to sort and bar/toil is the location of a directory (which should not already exist) from which the batch will be managed. Details of the toil options are described below; the stats option is used to gather statistics about the jobs in a run.

The script will return a zero exit value if the toil system is successfully able to run to completion, else it will create an exception. If the script fails because a job failed then the log file information of the job will be reported to std error.
The toil directory (here 'bar/toil') is not automatically deleted regardless of success or failure, and contains a record of the jobs run, which can be enquired about using the **toilStatus** command. e.g.

<code>[]$toilStatus bar/toil --verbose</code>

```
There are 0 active jobs, 0 parent jobs with children, 0 totally failed jobs and 0 empty jobs (i.e. finished but not cleaned up) curre
ntly in toil: toil
There are no failed jobs to report
```

If a job failed, this provides a convenient way to reprint the error. The following are the important options to **toilStatus**:

    --toil=TOIL     Directory containing the jobtree. The toil location can also be specified as the argument to the script. default=./toil
    --verbose             Print loads of information, particularly all the log
                        files of jobs that failed. default=False
    --failIfNotComplete   Return exit value of 1 if toil jobs not all
                        completed. default=False

If a job in the script failed or the system goes down, you may wish to retry the job after fixing the error. This can be achieved by restarting the script with the **toilRun** command which will restart an existing toil.

<code>[]$ toilRun --toil bar/toil --logLevel INFO</code>

It will always attempt to restart the jobs from the previous point of failure.

If the script was run with the **--stats** option then **toilStats** can be run on the pipeline do generate information about the performance of the run, in terms of how many jobs were run, how long they executed for and how much CPU time/wait time was involved, e.g.:

<code>[]$toilStats bar/toil</code>

```
Batch System: singleMachine
Default CPU: 1  Default Memory: 2097152K
Job Time: 0.50  Max CPUs: 9.22337e+18  Max Threads: 4
Total Clock: 0.09  Total Runtime: 7.60
Slave
    Count |                                    Time* |                                    Clock |                                     Wait |                                               Memory
        n |      min    med*     ave     max   total |      min     med     ave     max   total |      min     med     ave     max   total |        min       med       ave       max       total
      365 |     0.01    0.02    0.02    0.06    6.82 |     0.01    0.01    0.01    0.04    4.71 |     0.00    0.00    0.01    0.03    2.11 |   9781248K 13869056K 13799121K 14639104K 5036679168K
Job
 Slave Jobs   |     min    med    ave    max
              |       2      2      2      2
    Count |                                    Time* |                                    Clock |                                     Wait |                                               Memory
        n |      min    med*     ave     max   total |      min     med     ave     max   total |      min     med     ave     max   total |        min       med       ave       max       total
      367 |     0.00    0.00    0.00    0.03    0.68 |     0.00    0.00    0.00    0.01    0.42 |     0.00    0.00    0.00    0.03    0.26 |   9461760K 13869056K 13787694K 14639104K 5060083712K
 Cleanup
    Count |                                    Time* |                                    Clock |                                     Wait |                                               Memory
        n |      min    med*     ave     max   total |      min     med     ave     max   total |      min     med     ave     max   total |        min       med       ave       max       total
        1 |     0.00    0.00    0.00    0.00    0.00 |     0.00    0.00    0.00    0.00    0.00 |     0.00    0.00    0.00    0.00    0.00 |  14639104K 14639104K 14639104K 14639104K   14639104K
 Up
    Count |                                    Time* |                                    Clock |                                     Wait |                                               Memory
        n |      min    med*     ave     max   total |      min     med     ave     max   total |      min     med     ave     max   total |        min       med       ave       max       total
      124 |     0.00    0.00    0.00    0.01    0.15 |     0.00    0.00    0.00    0.01    0.12 |     0.00    0.00    0.00    0.01    0.03 |  13713408K 14090240K 14044985K 14581760K 1741578240K
 Setup
    Count |                                    Time* |                                    Clock |                                     Wait |                                               Memory
        n |      min    med*     ave     max   total |      min     med     ave     max   total |      min     med     ave     max   total |        min       med       ave       max       total
        1 |     0.00    0.00    0.00    0.00    0.00 |     0.00    0.00    0.00    0.00    0.00 |     0.00    0.00    0.00    0.00    0.00 |   9551872K  9551872K  9551872K  9551872K    9551872K
 Down
    Count |                                    Time* |                                    Clock |                                     Wait |                                               Memory
        n |      min    med*     ave     max   total |      min     med     ave     max   total |      min     med     ave     max   total |        min       med       ave       max       total
      241 |     0.00    0.00    0.00    0.03    0.53 |     0.00    0.00    0.00    0.00    0.30 |     0.00    0.00    0.00    0.03    0.23 |   9461760K 13828096K 13669354K 14155776K 3294314496K

```

The breakdown is given per "slave", which is unit of serial execution, and per "job", which corresponds to a scriptTree job (see below).
Despite its simplicity, we've found this can be **very** useful for tracking down performance issues, particularly when trying out a pipeline on a new system.

The important arguments to **toilStats** are:

    --outputFile=OUTPUTFILE
                        File in which to write results
    --raw                 output the raw xml data.
    --pretty, --human     if not raw, prettify the numbers to be human readable.
    --categories=CATEGORIES
                        comma separated list from [time, clock, wait, memory]
    --sortCategory=SORTCATEGORY
                        how to sort Job list. may be from [alpha, time,
                        clock, wait, memory, count]. default=%(default)s
    --sortField=SORTFIELD
                        how to sort Job list. may be from [min, med, ave,
                        max, total]. default=%(default)s
    --sortReverse, --reverseSort
                        reverse sort order.
    --cache               stores a cache to speed up data display.


##toil options

   A toil script will have the following command-line options.

    Options that control logging.

    --logOff            Turn off logging. (default is CRITICAL)
    --logInfo           Turn on logging at INFO level. (default is CRITICAL)
    --logDebug          Turn on logging at DEBUG level. (default is CRITICAL)
    --logLevel=LOGLEVEL
                        Log at level (may be either OFF/INFO/DEBUG/CRITICAL).
                        (default is CRITICAL)
    --logFile=LOGFILE   File to log in
    --rotatingLogging   Turn on rotating logging, which prevents log files
                        getting too big.

   Options to specify the location of the toil and turn on stats
    collation about the performance of jobs.

    --toil=TOIL   Directory in which to place job management files and
                        the global accessed temporary file directories(this
                        needs to be globally accessible by all machines
                        running jobs). If you pass an existing directory it
                        will check if it's a valid existing jobtree, then try
                        and restart the jobs in it. The default=./toil
    --stats             Records statistics about the job-tree to be used by
                        toilStats. default=False

   Options for specifying the batch system, and arguments to the
    batch system/big batch system (see below).

    --batchSystem=BATCHSYSTEM
                        The type of batch system to run the job(s) with,
                        currently can be
                        'singleMachine'/'parasol'/'acidTest'/'gridEngine'/'lsf'.
                        default=singleMachine
    --maxThreads=MAXTHREADS
                        The maximum number of threads (technically processes
                        at this point) to use when running in single machine
                        mode. Increasing this will allow more jobs to run
                        concurrently when running on a single machine.
                        default=4
    --parasolCommand=PARASOLCOMMAND
                        The command to run the parasol program default=parasol

    Options to specify default cpu/memory requirements (if not
    specified by the jobs themselves), and to limit the total amount of
    memory/cpu requested from the batch system.

    --defaultMemory=DEFAULTMEMORY
                        The default amount of memory to request for a job (in
                        bytes), by default is 2^31 = 2 gigabytes,
                        default=2147483648
    --defaultCores=DEFAULTCORES
                        The default the number of cpus to dedicate a job.
                        default=1
    --maxCpus=MAXCPUS   The maximum number of cpus to request from the batch
                        system at any one time. default=9223372036854775807
    --maxMemory=MAXMEMORY
                        The maximum amount of memory to request from the batch
                        system at any one time. default=9223372036854775807

   Options for rescuing/killing/restarting jobs, includes options for jobs that either run too long/fail or get lost (some
    batch systems have issues!).

    --retryCount=RETRYCOUNT
                        Number of times to retry a failing job before giving
                        up and labeling job failed. default=0
    --maxJobDuration=MAXJOBDURATION
                        Maximum runtime of a job (in seconds) before we kill
                        it (this is a lower bound, and the actual time before
                        killing the job may be longer).
                        default=9223372036854775807
    --rescueJobsFrequency=RESCUEJOBSFREQUENCY
                        Period of time to wait (in seconds) between checking
                        for missing/overlong jobs, that is jobs which get lost
                        by the batch system. Expert parameter. (default is set
                        by the batch system)

  toil big batch system options; toil can employ a secondary batch system for running large
    memory/cpu jobs using the following arguments.

    --bigBatchSystem=BIGBATCHSYSTEM
                        The batch system to run for jobs with larger
                        memory/cpus requests, currently can be
                        'singleMachine'/'parasol'/'acidTest'/'gridEngine'.
                        default=none
    --bigMemoryThreshold=BIGMEMORYTHRESHOLD
                        The memory threshold above which to submit to the big
                        queue. default=9223372036854775807
    --bigCpuThreshold=BIGCPUTHRESHOLD
                        The cpu threshold above which to submit to the big
                        queue. default=9223372036854775807
    --bigMaxCpus=BIGMAXCPUS
                        The maximum number of big batch system cpus to allow
                        at one time on the big queue.
                        default=9223372036854775807
    --bigMaxMemory=BIGMAXMEMORY
                        The maximum amount of memory to request from the big
                        batch system at any one time.
                        default=9223372036854775807

    Miscellaneous options.

    --jobTime=JOBTIME   The approximate time (in seconds) that you'd like a
                        list of child jobs to be run serially before being
                        parallelized. This parameter allows one to avoid over
                        parallelizing tiny jobs, and therefore paying
                        significant scheduling overhead, by running tiny jobs
                        in series on a single node/core of the cluster.
                        default=30
    --maxLogFileSize=MAXLOGFILESIZE
                        The maximum size of a job log file to keep (in bytes),
                        log files larger than this will be truncated to the
                        last X bytes. Default is 50 kilobytes, default=50120
    --command=COMMAND   The command to run (which will generate subsequent
                        jobs). This is deprecated


##Overview of toil

The following sections are for people creating toil scripts and as general information. The presentation **[docs/toilSlides.pdf](https://github.com/benedictpaten/toil/blob/master/doc/toilSlides.pdf)** is also a quite useful, albeit slightly out of date, guide to using toil. -

Most batch systems (such as LSF, Parasol, etc.) do not allow jobs to spawn
other jobs in a simple way.

The basic pattern provided by toil is as follows:

1. You have a job running on your cluster which requires further parallelisation.
2. You create a list of jobs to perform this parallelisation. These are the 'child' jobs of your process, we call them collectively the 'children'.
3. You create a 'follow-on' job, to be performed after all the children have successfully completed. This job is responsible for cleaning up the input files created for the children and doing any further processing. Children should not cleanup files created by parents, in case of a batch system failure which requires the child to be re-run (see 'Atomicity' below).
4. You end your current job successfully.
5. The batch system runs the children. These jobs may in turn have children and follow-on jobs.
6. Upon completion of all the children (and children's children and follow-ons, collectively descendants) the follow-on job is run. The follow-on job may create more children.

##scriptTree
ScriptTree provides a Python interface to toil, and is now the only way to interface with toil (previously you could manipulate XML files, but I've removed that functionality as I improved the underlying system).

Aside from being the interface to toil, scriptTree was designed to remediate some of the pain of writing wrapper scripts for cluster jobs, via the extension of a simple python wrapper class (called a 'Job' to avoid confusion with the more general use of the word 'job') which does much of the work for you.  Using scriptTree, you can describe your script as a series of these classes which link together, with all the arguments and options specified in one place. The script then, using the magic of python pickles, generates all the wrappers dynamically and clean them up when done.

This inherited template pattern has the following advantages:

1. You write (potentially) just one script, not a series of wrappers. It is much easier to understand, maintain, document and explain.
2. You write less boiler plate.
3. You can organise all the input arguments and options in one place.

The best way to learn how to use script tree is to look at an example. The following is taken from (an old version of) <code>toil.test.sort.scriptTreeTest_Sort.py</code> which provides a complete script for performing a parallel merge sort.

Below is the first 'Job' of this script inherited from the base class 'toil.scriptTree.Job'. Its job is to setup the merge sort.

```python
class Setup(Job):
    """Sets up the sort.
    """
    def __init__(self, inputFile, N):
        Job.__init__(self, time=1, memory=1000000, cpu=1)
        self.inputFile = inputFile
        self.N = N

    def run(self):
        tempOutputFile = getTempFile(rootDir=self.getGlobalTempDir())
        self.addChildJob(Down(self.inputFile, 0, os.path.getsize(self.inputFile), self.N, tempOutputFile))
        self.setFollowOnJob(Cleanup(tempOutputFile, self.inputFile))
```

The constructor (**__init__()**) assigns some variables to the class. When invoking the constructor of the base class (which should be the first thing the job does), you can optionally pass time (in seconds), memory (in bytes) and cpu parameters. The time parameter is your estimate of how long the job will run - UPDATE: IT IS CURRENTLY UNUSED BY THE SCHEDULAR. The memory and cpu parameters allow you to guarantee resources for a job.

The run method is where the variables assigned by the constructor are used and where in general actual work is done.
Aside from doing the specific work of the job (in this case creating a temporary file to hold some intermediate output), the run method is also where children and a follow-on job are created, using **addChildJob()** and **setFollowOnJob()**. A job may have arbitrary numbers of children, but one or zero follow-on jobs.

Jobs are also provided with two temporary file directories called **localTempDir** and **globalTempDir**, which can be accessed with the methods **getLocalTempDir()** and **getGlobalTempDir()**, respectively. The **localTempDir** is the path to a temporary directory that is local to the machine on which the job is being executed and that will exist only for the length of the run method. It is useful for storing interim results that are computed during runtime. All files in this directory are guaranteed to be removed once the run method has finished - even if your job crashes.

A job can either be created as a follow-on, or it can be the very first job, or it can be created as a child of another job. Let a job not created as a follow-on be called a 'founder'. Each founder job may have a follow-on job. If it has a follow-on job, this follow-on job may in turn have a follow-on, etc. Thus each founder job defines a chain of follow-ons.  Let a founder job and its maximal sequence of follow-ons be called a 'chain'. Let the last follow-on job in a chain be called the chain's 'closer'. For each chain of jobs a temporary directory, **globalTempDir**, is created immediately prior to calling the founder job's run method, this directory and its contents then persist until the completion of closer job's run method. Thus the **globalTempDir** is a scratch directory in which temporary results can be stored on disk between job jobs in a chain. Furthermore, files created in this directory can be passed to the children of job jobs in the chain, allowing results to be transmitted from a job job to its children.

##Making Functions into Jobs

To avoid the need to create a Job class for every job, I've added the ability to wrap functions, hence the code for the setup function described above becomes:

```
def setup(job, inputFile, N):
    """Sets up the sort.
    """
    tempOutputFile = getTempFile(rootDir=job.getGlobalTempDir())
    job.addChildJobFn(down, (inputFile, 0, os.path.getsize(inputFile), N, tempOutputFile))
    job.setFollowOnFn(cleanup, (tempOutputFile, inputFile))
```

The code to turn this into a job uses the static method **[Job.makeJobFnJob](https://github.com/benedictpaten/toil/blob/development/scriptTree/job.py#L142)**:

```
Job.makeJobFnJob(setup, (fileToSort, N))
```

Notice that the child and follow-on jobs have also been refactored as functions, hence the methods **[addChildJobFn](https://github.com/benedictpaten/toil/blob/development/scriptTree/job.py#L82)** and **[setFollowOnFn](https://github.com/benedictpaten/toil/blob/development/scriptTree/job.py#L67)**, which take functions as opposed to Job objects.

Note, there are two types of functions you can wrap - **job functions**, whose first argument must be the wrapping job object (the setup function above is an excample of a job function), and plain functions that do not have a reference to the wrapping job.

##Creating a scriptTree script:

ScriptTree jobs are serialized (written and retrieved from disk) so that they can be executed in parallel on a cluster of different machines. Thankfully, this is mostly transparent to the user, except for the fact that jobs must be 'pickled' (see python docs), which creates a few constraints upon what can and can not be passed to and stored by a job.
Currently the preferred way to run a pipeline is to create an executable python script.
For example, see **tests/sorts/scriptTreeTest_Sort.py**.

The first line to notice is:

```python
from toil.scriptTree.job import Job, Stack
```

This imports the Job and Stack objects (the stack object is used to run the jobs).

Most of the code defines a series of jobs (see above).
The **main()** method is where the script is setup and run.

The line:

```python
    parser = OptionParser()
```

Creates an options parser using the python module optparse.

The line:

```python
    Stack.addToilOptions(parser)
```

Adds the toil options to the parser. Most importantly it adds the command line options "--toil [path to toil]", which is the location in which the toil will be created, and which must be supplied to the script.

The subsequent lines parse the input arguments, notably the line:

```python
    options, args = parser.parse_args()
```

reads in the input parameters.

The line:

```python
    i = Stack(Setup(options.fileToSort, int(options.N))).startToil(options)
```

Is where the first job is created (the Setup job shown above), where a stack object is created, which is passed the first job as its sole construction argument, and finally where the toil is executed from, using the stack's **startToil(options)** function. The 'options' argument will contain a dictionary of command line arguments which are used by toil. The return value of this function is equal to the number of failed jobs. In this case we choose to throw an exception if there are any remaining.

One final important detail, the lines:

```python
    if __name__ == '__main__':
        from toil.test.sort.scriptTreeTest_Sort import *
```

reload the objects in the module, such that their module names will be absolute (this is necessary for the serialization that is used). Jobs in other classes that are imported do not need to be reloaded in this way.

##Atomicity
toil and scriptTree are designed to be robust, so that individuals jobs (jobs) can fail, and be subsequently restarted. It is assumed jobs can fail at any point. Thus until toil knows your children have been completed okay you can not assume that your Job has been completed. To ensure that your pipeline can be restarted after a failure ensure that every job (job):

1. **Never cleans up / alters its own input files.** Instead, parents and follow on jobs may clean up the files of children or prior jobs.
2. Can be re-run from just its input files any number of times. A job should only depend on its input, and it should be possible to run the job as many times as desired, essentially until news of its completion is successfully transmitted to the jobtree master process.

    These two properties are the key to job atomicity. Additionally, you'll find it much easier if a job:

3. Only creates temp files in the two provided temporary file directories. This ensures we don't soil the cluster's disks.
4. Logs sensibly, so that error messages can be transmitted back to the master and the pipeline can be successfully debugged.

##Environment
toil replicates the environment in which toil or scriptTree is invoked and provides this environment to all the jobs/jobs. This ensures uniformity of the environment variables for every job.

##FAQ's:

* _How robust is toil to failures of nodes and/or the master?_

    Toil checkpoints its state on disk, so that it or the job manager can be wiped out and restarted. There is some gnarly test code to show how this works, it will keep crashing everything, at random points, but eventually everything will complete okay. As a user you needn't worry about any of this, but your child jobs must be atomic (as with all batch systems), and must follow the convention regarding input files.

* _How scaleable?_

    We have tested having 1000 concurrent jobs running on our cluster. This will depend on the underlying batch system being used.

* _Can you support the XYZ batch system?_

    See the abstract base class '[AbstractBatchSystem](https://github.com/benedictpaten/toil/blob/master/batchSystems/abstractBatchSystem.py)' in the code to see what functions need to be implemented. It's reasonably straight forward.

* _Is there an API for the toil top level commands?_

    Not really - at this point please use scriptTree and the few command line utilities present in the bin directory.

* _Why am I getting the error "ImportError: No module named etree.ElementTree"?_

    The version of python in your path is less than 2.5. When toil spawns a new job it will use the python found in your PATH. Make sure that the first python in your PATH points to a python version greater than or equal to 2.5 but less than 3.0

