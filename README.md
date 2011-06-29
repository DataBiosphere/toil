#jobTree
10/07/2009, revised 09/17/2010.

##Author
Benedict Paten (benedict@soe.ucsc.edu)

##Introduction
Most batch systems (such as LSF, Parasol, etc.) do not allow jobs to spawn
other jobs in a simple way. 

The basic pattern provided by jobTree is as follows: 

1. You have a job running on your cluster which requires further parallelisation. 
2. You create a list of jobs to perform this parallelisation. These are the 'child' jobs of your process, we call them collectively the 'children'.
3. You create a 'follow-on' job, to be performed after all the children have successfully completed. This job is responsible for cleaning up the input files created for the children and doing any further processing. **Children should not cleanup files created by parents**, in case of a batch system failure which requires the child to be re-run (see 'Atomicity' below).
4. You end your current job successfully.
5. The batch system runs the children. These jobs may in turn have children and follow-on jobs.
6. Upon completion of all the children (and children's children and follow-ons, collectively descendants) the follow-on job is run. The follow-on job may create more children.

##scriptTree
ScriptTree provides a simple Python interface to jobTree, and is the preferred way to use jobTree. 

Aside from being the interface to jobTree, scriptTree was designed to remediate some of the pain of writing wrapper scripts for cluster jobs, via the extension of a simple python wrapper class (called a 'Target' to avoid confusion with the more general use of the word 'job') which does much of the work for you.  Using scriptTree, you can describe your script as a series of these classes which link together, with all the arguments and options specified in one place. The script then, using the magic of python pickles, generates all the wrappers dynamically and clean them up when done.

This inherited template pattern has the following advantages:

1. You write (potentially) just one script, not a series of wrappers. It is much easier to understand, maintain, document and explain.
2. You write less boiler plate.
3. You can organise all the input arguments and options in one place.

The best way to learn how to use script tree is to look at an example. The following is taken from <code>jobTree.test.sort.scriptTreeTest_Sort.py</code> which provides a complete script for performing a parallel merge sort. 

Below is the first 'Target' of this script inherited from the base class 'jobTree.scriptTree.Target'. Its job is to setup the merge sort.

```python
class Setup(Target):
    """Sets up the sort.
    """
    def __init__(self, inputFile, N):
        Target.__init__(self, time=1, memory=1000000, cpu=1)
        self.inputFile = inputFile
        self.N = N
    
    def run(self):
        tempOutputFile = getTempFile(rootDir=self.getGlobalTempDir())
        self.addChildTarget(Down(self.inputFile, 0, os.path.getsize(self.inputFile), self.N, tempOutputFile))
        self.setFollowOnTarget(Cleanup(tempOutputFile, self.inputFile))
```

The constructor (**__init__()**) assigns some variables to the class. When invoking the constructor of the base class (which should be the first thing the target does), you can optionally pass time (in seconds), memory (in bytes) and cpu parameters. The time parameter is your estimate of how long the target will run, and allows the scheduler to be more efficient. The memory and cpu parameters allow you to guarantee resources for a target.

The run method is where the variables assigned by the constructor are used and where in general actual work is done.
Aside from doing the specific work of the target (in this case creating a temporary file to hold some intermediate output), the run method is also where children and a follow-on job are created, using **addChildTarget()** and **setFollowOnTarget()**. A job may have arbitrary numbers of children, but one or zero follow-on jobs. 

Targets are also provided with two temporary file directories called **localTempDir** and **globalTempDir**, which can be accessed with the methods **getLocalTempDir()** and **getGlobalTempDir()**, respectively. The **localTempDir** is the path to a temporary directory that is local to the machine on which the target is being executed and that will exist only for the length of the run method. It is useful for storing interim results that are computed during runtime. All files in this directory are guaranteed to be removed once the run method has finished - even if your target crashes. 

A job can either be created as a follow-on, or it can be the very first job, or it can be created as a child of another job. Let a job not created as a follow-on be called a 'founder'. Each founder job may have a follow-on job. If it has a follow-on job, this follow-on job may in turn have a follow-on, etc. Thus each founder job defines a chain of follow-ons.  Let a founder job and its maximal sequence of follow-ons be called a 'chain'. Let the last follow-on job in a chain be called the chain's 'closer'. For each chain of targets a temporary directory, **globalTempDir**, is created immediately prior to calling the founder target's run method, this directory and its contents then persist until the completion of closer target's run method. Thus the **globalTempDir** is a scratch directory in which temporary results can be stored on disk between target jobs in a chain. Furthermore, files created in this directory can be passed to the children of target jobs in the chain, allowing results to be transmitted from a target job to its children.

##Running a scriptTree pipeline:

ScriptTree targets are serialized (written and retrieved from disk) so that they can be executed in parallel on cluster of different machines. Thankfully, this is mostly transparent to the user, except for the fact that targets must be 'pickled' (see python docs), which creates a few constraints upon what can and can not be passed to and stored by a target. 

Currently the preferred way to run a pipeline is to create an executable python script.
An example of this is shown in **tests/sorts/scriptTreeTest_Sort.py**. 

The first line to notice is:

```python
from jobTree.scriptTree.target import Target, Stack
```

This imports the Target and Stack objects (the stack object is used to run the targets).

Most of the code defines a series of targets (see above). 
The **main()** method is where the script is setup and run.

The line:

```python
    parser = OptionParser()
```

Creates an options parser using the python module optparse.

The line:

```python
    Stack.addJobTreeOptions(parser)
```

Adds the jobTree options to the parser. Most importantly it adds the command line options "--jobTree [path to jobTree]", which is the location in which the jobTree will be created, and which must be supplied to the script.

The subsequent lines parse the input arguments, notably the line:

```python
    options, args = parser.parse_args()
```

reads in the input parameters.

The line:

```python
    i = Stack(Setup(options.fileToSort, int(options.N))).startJobTree(options)
```

Is where the first target is created (the Setup target shown above), where a stack object is created, which is passed the first target as its sole construction argument, and finally where the jobTree is executed from, using the stack's **startJobTree(options)** function. The 'options' argument will contain a dictionary of command line arguments which are used by jobTree. The return value of this function is equal to the number of failed targets. In this case we choose to throw an exception if there are any remaining.

One final important detail, the lines:

```python
    if __name__ == '__main__':
        from jobTree.test.sort.scriptTreeTest_Sort import *
```

reload the objects in the module, such that their module names will be absolute (this is necessary for the serialization that is used). Targets in other classes that are imported do not need to be reloaded in this way.

The script can then be run, for example using the command: 

<code>[]$ scriptTreeTest_Sort.py --fileToSort foo --jobTree bar/jobTree --batchSystem parasol --logLevel INFO</code>

Which in this case uses parasol and INFO level logging and where foo is the file to sort and bar/jobTree is the location of a directory (which should not already exist) from which the batch will be managed.

The script will return a zero exit value if the jobTree system is successfully able to run to completion, else it will create an exception. The directory 'bar/jobTree', is not automatically deleted and contains a record of the jobs run, which can be enquired about using the **jobTreeStatus.py** command. 

If the script fails because a target failed then the script will return a non-zero exit value and log file information will be reported to std error (these errors can also be retrieved using the jobTreeStatus command). If you wish to retry the job after fixing the error then the batch can be restarted by calling

<code>[]$ jobTreeRun --jobTree bar/jobTree --logLevel INFO</code>

Which will attempt to restart the jobs from the previous point of failure.

##Atomicity
jobTree and scriptTree are designed to be robust, so that individuals jobs (targets) can fail, and be subsequently restarted. It is assumed jobs can fail at any point. Thus until jobTree knows your children have been completed okay you can not assume that your job (if using scriptTree, Target) has been completed. To ensure that your pipeline can be restarted after a failure ensure that every job (target):
         
1. **Never cleans up / alters its own input files.** Instead, parents and follow on jobs may clean up the files of children or prior jobs.
2. Can be re-run from just its input files any number of times. A job should only depend on its input, and it should be possible to run the job as many times as desired, essentially until news of its completion is successfully transmitted to the job tree master process. 

    These two properties are the key to job atomicity. Additionally, you'll find it much easier if a job:

3. Only creates temp files in the two provided temporary file directories. This ensures we don't soil the cluster's disks.
4. Logs sensibly, so that error messages can be transmitted back to the master and the pipeline can be successfully debugged.

##Environment
jobTree replicates the environment in which jobTree or scriptTree is invoked and provides this environment to all the jobs/targets. This ensures uniformity of the environment variables for every job.

##Probably FAQ's:
* _Why do we use this pattern?_

    Ideally when issuing children the parent job could just go to sleep on the cluster. But unless it frees the machine it's sleeping on, then the cluster soon jams up with sleeping jobs. This design is a pragmatic way of designing simple parallel code. It isn't heavy duty, it isn't map-reduce, but it has it's niche.

* _What do you mean 'crash only' software?_

    This is just a fancy way of saying that jobTree checkpoints its state on disk, so that it or the job manager can be wiped out and restarted. There is some gnarly test code to show how this works, it will keep crashing everything, at random points, but eventually everything will complete okay. As a consumer you needn't worry about any of this, but your child jobs must be atomic (as with all batch systems), and must follow the convention regarding input files.

* _How scaleable?_

    Probably not very, but it could be. You should be safe to have a 1000 concurrent jobs running, depending on your file-system and batch system.

* _Can you support my XYZ batch system?_

    See the abstract base class 'AbstractBatchSystem' in the code to see what's required. You'll probably need to speak to me as I haven't attempted to comprehensively document these functions, though it's pretty straight forward.

* _Is there an API for the jobTree top level commands?_

    Not really - at this point please use scriptTree and the few command line utilities present in the bin directory.
