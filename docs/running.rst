.. _running:

Running A Workflow
==================

.. _quickstart:

Running Quick Start
-------------------

Starting with Python, a Toil workflow can be run with just three steps.

1. ``pip install toil``
2. Copy and paste the following code block into **HelloWorld.py**::

        from toil.job import Job

        def helloWorld(message, memory="2G", cores=2, disk="3G"):
            return "Hello, world!, here's a message: %s" % message

        j = Job.wrapFn(helloWorld, "You did it!")

        if __name__=="__main__":
            parser = Job.Runner.getDefaultArgumentParser()
            options = parser.parse_args()
            print Job.Runner.startToil(j, options) #Prints Hello, world!, ...

3. ``python HelloWorld.py file:jobStore``

Now you have run Toil on **singleMachine** (default batch system) using the **FileStore** job store. The first
positional argument after the ``.py`` is the location of the job store, a place where intermediate files are
written to. In this example, a folder called **jobStore** will be created where **HelloWorld.py** is run from.
Information on the jobStore can be found at :ref:`jobStoreInterface`.

Run ``python HelloWorld.py --help`` to see a complete list of available options.

For something beyond a hello world example, refer to :ref:`runningDetail`

Running CWL Workflows
---------------------

The `Common Workflow Language`_ (CWL) is an emerging standard for writing
workflows that are portable across multiple workflow engines and platforms.  To
run workflows written using CWL, first ensure that Toil is installed with the
"cwl" extra as described in :ref:`installation-ref`.  This will install the
executables ``cwl-runner`` and ``cwltoil`` (these are identical, where
``cwl-runner`` is the portable name for the default system CWL runner).  To
learn more about CWL, see the `CWL User Guide`_.

To run in local batch mode, simply provide the CWL file and the input object
file::

    cwltoil example.cwl example-job.yml

To run in cloud and HPC configurations, you may need to provide additional
command line parameters to select and configure the batch system to use.
Consult the appropriate sections.

.. _Common Workflow Language: http://commonwl.org
.. _CWL User Guide: http://commonwl.org/draft-3/UserGuide.html

.. _runningDetail:

Running a Toil Pipeline in Detail
---------------------------------

For a detailed example and explanation, we'll walk through running a pipeline
that performs merge-sort on a temporary file.

1. Copy and paste the following code into **toil-sort-example.py**::

        from __future__ import absolute_import
        from argparse import ArgumentParser
        import os
        import logging
        import random

        from toil.job import Job


        def setup(job, input_file, n, down_checkpoints):
            """Sets up the sort.
            """
            # Write the input file to the file store
            input_filestore_id = job.fileStore.writeGlobalFile(input_file, True)
            job.fileStore.logToMaster(" Starting the merge sort ")
            job.addFollowOnJobFn(cleanup, job.addChildJobFn(down,
                                                            input_filestore_id, n,
                                                            down_checkpoints=down_checkpoints,
                                                            memory='1000M').rv(), input_file)


        def down(job, input_file_store_id, n, down_checkpoints):
            """Input is a file and a range into that file to sort and an output location in which
            to write the sorted file.
            If the range is larger than a threshold N the range is divided recursively and
            a follow on job is then created which merges back the results else
            the file is sorted and placed in the output.
            """
            # Read the file
            input_file = job.fileStore.readGlobalFile(input_file_store_id, cache=False)
            length = os.path.getsize(input_file)
            if length > n:
                # We will subdivide the file
                job.fileStore.logToMaster("Splitting file: %s of size: %s"
                                          % (input_file_store_id, length), level=logging.CRITICAL)
                # Split the file into two copies
                mid_point = get_midpoint(input_file, 0, length)
                t1 = job.fileStore.getLocalTempFile()
                with open(t1, 'w') as fH:
                    copy_subrange_of_file(input_file, 0, mid_point + 1, fH)
                t2 = job.fileStore.getLocalTempFile()
                with open(t2, 'w') as fH:
                    copy_subrange_of_file(input_file, mid_point + 1, length, fH)
                # Call down recursively
                return job.addFollowOnJobFn(up, job.addChildJobFn(down, job.fileStore.writeGlobalFile(t1), n,
                                            down_checkpoints=down_checkpoints, memory='1000M').rv(),
                                            job.addChildJobFn(down, job.fileStore.writeGlobalFile(t2), n,
                                                              down_checkpoints=down_checkpoints,
                                                              memory='1000M').rv()).rv()
            else:
                # We can sort this bit of the file
                job.fileStore.logToMaster("Sorting file: %s of size: %s"
                                          % (input_file_store_id, length), level=logging.CRITICAL)
                # Sort the copy and write back to the fileStore
                sort(input_file)
                return job.fileStore.writeGlobalFile(input_file)


        def up(job, input_file_id_1, input_file_id_2):
            """Merges the two files and places them in the output.
            """
            with job.fileStore.writeGlobalFileStream() as (fileHandle, output_id):
                with job.fileStore.readGlobalFileStream(input_file_id_1) as inputFileHandle1:
                    with job.fileStore.readGlobalFileStream(input_file_id_2) as inputFileHandle2:
                        merge(inputFileHandle1, inputFileHandle2, fileHandle)
                        job.fileStore.logToMaster("Merging %s and %s to %s"
                                                  % (input_file_id_1, input_file_id_2, output_id))
                # Cleanup up the input files - these deletes will occur after the completion is successful.
                job.fileStore.deleteGlobalFile(input_file_id_1)
                job.fileStore.deleteGlobalFile(input_file_id_2)
                return output_id


        def cleanup(job, temp_output_id, output_file):
            """Copies back the temporary file to input once we've successfully sorted the temporary file.
            """
            job.fileStore.readGlobalFile(temp_output_id, userPath=output_file)
            job.fileStore.logToMaster("Finished copying sorted file to output: %s" % output_file)


        # convenience functions
        def sort(file):
            """Sorts the given file.
            """
            filehandle = open(file, 'r')
            lines = filehandle.readlines()
            filehandle.close()
            lines.sort()
            filehandle = open(file, 'w')
            for line in lines:
                filehandle.write(line)
            filehandle.close()


        def merge(filehandle_1, filehandle_2, output_filehandle):
            """Merges together two files maintaining sorted order.
            """
            line2 = filehandle_2.readline()
            for line1 in filehandle_1.readlines():
                while line2 != '' and line2 <= line1:
                    output_filehandle.write(line2)
                    line2 = filehandle_2.readline()
                output_filehandle.write(line1)
            while line2 != '':
                output_filehandle.write(line2)
                line2 = filehandle_2.readline()


        def copy_subrange_of_file(input_file, file_start, file_end, output_filehandle):
            """Copies the range (in bytes) between fileStart and fileEnd to the given
            output file handle.
            """
            with open(input_file, 'r') as fileHandle:
                fileHandle.seek(file_start)
                data = fileHandle.read(file_end - file_start)
                assert len(data) == file_end - file_start
                output_filehandle.write(data)


        def get_midpoint(file, file_start, file_end):
            """Finds the point in the file to split.
            Returns an int i such that fileStart <= i < fileEnd
            """
            filehandle = open(file, 'r')
            mid_point = (file_start + file_end) / 2
            assert mid_point >= file_start
            filehandle.seek(mid_point)
            line = filehandle.readline()
            assert len(line) >= 1
            if len(line) + mid_point < file_end:
                return mid_point + len(line) - 1
            filehandle.seek(file_start)
            line = filehandle.readline()
            assert len(line) >= 1
            assert len(line) + file_start <= file_end
            return len(line) + file_start - 1


        def make_file_to_sort(file_name, lines, line_length):
            with open(file_name, 'w') as fileHandle:
                for _ in xrange(lines):
                    line = "".join(random.choice('actgACTGNXYZ') for _ in xrange(line_length - 1)) + '\n'
                    fileHandle.write(line)


        def main():
            parser = ArgumentParser()
            Job.Runner.addToilOptions(parser)

            parser.add_argument('--num-lines', default=1000, help='Number of lines in file to sort.', type=int)
            parser.add_argument('--line-length', default=50, help='Length of lines in file to sort.', type=int)
            parser.add_argument("--N",
                                help="The threshold below which a serial sort function is used to sort file. "
                                "All lines must of length less than or equal to N or program will fail",
                                default=10000)

            options = parser.parse_args()

            if int(options.N) <= 0:
                raise RuntimeError("Invalid value of N: %s" % options.N)

            make_file_to_srt(file_name='file_to_sort.txt', lines=options.num_lines, line_length=options.line_length)

            # Now we are ready to run
            Job.Runner.startToil(Job.wrapJobFn(setup, 'file_to_sort.txt', int(options.N), False,
                                               memory='1000M'), options)

        if __name__ == '__main__':
            main()

2. Run with default settings: ``python toil-sort-example.py file:jobStore``.
3. Run with options: ``python toil-sort-example.py file:jobStore --num-lines 5000 --line-length 10 --workDir /tmp/``

The ``if __name__ == '__main__'`` boilerplate is required to enable Toil to import the job
functions defined in the script into the context of a Toil *worker* process. By invoking the script
you created the *leader process*. A worker process is a separate process whose sole purpose is to
host the execution of one or more jobs defined in that script. When using the single-machine batch
system (the default), the worker processes will be running on the same machine as the leader
process. With full-fledged batch systems like Mesos the worker processes will typically be started
on separate machines. The boilerplate ensures that the pipeline is only started once–on the
leader–but not when its job functions are imported and executed on the individual workers.

Typing ``python toil-sort-example.py --help`` will show the complete list of arguments for the workflow which includes
both Toil's and ones defined inside **toil-sort-example.py**.  A complete explanation of Toil's arguments can be found
in :ref:`commandRef`.

Changing the log statements
~~~~~~~~~~~~~~~~~~~~~~~~~~~
When we run the pipeline, we see some logs printed to the screen.  At the top there's some information provided
to the user about the environment Toil is being setup in, and then as the pipeline runs we get INFO level messages
from the batch system that tell us when jobs are being executed.  We also see both INFO and CRITICAL level messages
that are in the user script.  By changing the logLevel, we can change what we see output to screen. For only
CRITICAL level messages: ``python toil-sort-examply.py file:jobStore --logLevel=critical``.  This hides most of the
information we get from the Toil run. For more detail, we can run the pipeline with ``--logLevel=debug``
to see a comprehensive output.  For more information see :ref:`loggingRef`.

Restarting after introducing a bug
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Let's now introduce a bug in the code, so we can understand what a failure looks like in Toil, and how we would go about
resuming the pipeline. On line 30, the first line of the **down()** function, let's add the line
``assert 1==2, 'Test Error!'``.  Now when we run the pipeline, ``python toil-sort-example.py file:jobStore``, we'll
see a failure log under the header **- - - TOIL WORKER OUTPUT LOG- - -**, that contains the stack trace. We see
a detailed message telling us that on line 30, in the **down** fuction, we encountered an error.

If we try and run the pipeline again, we get an error message telling us that a jobStore of the same name already
exists.  The default behavior for the job store is that it is not cleaned up in the event of failure
so that you can restart it from the last succesful job. We can restart the pipeline by running
``python toil-sort-example.py file:jobStore --restart``.  We can also change the number of times Toil will
attempt to retry a failed job, ``python toil-sort-example.py --retryCount 2 --restart``.
You'll now see Toil attempt to rerun the failed job, decrementing a counter until that job has exhausted the
retry count.  ``--retryCount`` is useful for non-systemic errors, like downloading a file that may experience
a sporadic interruption, or some other non-deterministic failure.

To succesfully restart our pipeline, we can edit our script to comment out line 30, or remove it, and then run
``python toil-sort-example.py --restart``. The pipeline will successfully complete, and the job store will be removed.


Getting stats from our pipeline run
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
We can execute the pipeline to let use retrieve statistics with ``python toil-sort-example.py --stats``.  Our
pipeline will finish successfully, but leave behind the job store.  Now we can type ``toil stats file:jobStore`` and
get back information about total runtime and stats pertaining to each job function.

We can then cleanup our jobStore by running ``toil clean file:jobStore``

Running in the Cloud
====================

There are several recommended ways to run Toil jobs in the cloud. Of these, running on Amazon Web Services (AWS) is currently the best-supported solution.

On all cloud providers, it is recommended that you run long-running jobs on remote systems under ``screen``. Simply type ``screen`` to open a new ``screen` session. Later, type ``ctrl-a`` and then ``d`` to disconnect from it, and run ``screen -r`` to reconnect to it. Commands running under ``screen`` will continue running even when you are disconnected, allowing you to unplug your laptop and take it home without ending your Toil jobs.


.. _runningAWS:

Running on AWS
--------------
See :ref:`installationAWS` to get setup for running on AWS.

Having followed the :ref:`quickstart` guide, the user can run their **HelloWorld.py** script on a distributed cluster
just by modifiying the run command.  Since our cluster is distributed, we'll use the **AWS Jobstore**
which creates a job store in S3 instead of on file system.

Place the HelloWorld.py script on the leader node, and run::

    python --batchSystem=mesos --mesosMaster=mesos-master:5050 \
                    HelloWorld.py aws:us-west-2:my-s3-jobstore

To run a CWL workflow::

    cwltoil --batchSystem=mesos --mesosMaster=mesos-master:5050 \
                    --jobStore=aws:us-west-2:my-s3-jobstore \
                    example.cwl example-job.yml

When running a CWL workflow on AWS, input files can be provided either on the
local file system or in S3 buckets using s3:// URL references.  Final output
files will be copied to the local file system of the leader node.

.. _runningAzure:

Running on Azure
----------------

See :ref:`installationAzure` to get setup for running on Azure. This section assumes that you are SSHed into your cluster's leader node.

The Azure templates do not create a shared filesystem; you need to use the **Azure Jobstore**, which needs an Azure Storage Account in which to store its job data. (Note that you can store multiple job stores in a single Azure Storage Account.)

To create a new Storage Account, if you do not already have one:

1. `Click here <https://portal.azure.com/#create/Microsoft.StorageAccount>`_, or navigate to ``https://portal.azure.com/#create/Microsoft.StorageAccount`` in your browser.
2. If necessary, log into the Microsoft Account that you use for Azure.
3. Fill out the presented form. The **Name** for the account, notably, must be a 3-to-24-character string of letters and lowercase numbers that is globally unique. For **Deployment model**, choose "Resource manager". For **Resource group**, choose or create a resource group **different than** the one in which you created your cluster. For **Location**, choose the **same** region that you used for your cluster.
4. Press the "Create" button. Wait for your Storage Account to be created; you should get a notification in the notifications area at the upper right.

Once you have a Storage Account, you need to authorize the cluster to access the Storage Account, by giving it the access key. To do find your Storage Account's access key:

1. When your Storage Account has been created, open it up and click the "Settings" icon.
2. In the "Settings" panel, select "Access keys".
3. Select the text in the "Key1" box and copy it to the clipboard, or use the copy-to-clipboard icon.

You then need to share the key with the cluster. To do this temporarily, for the duration of an SSH or screen session:

1. On the leader node, run ``export AZURE_ACCOUNT_KEY="<KEY>"``, replacing ``<KEY>`` with the access key you copied from the Azure portal.

To do this permanently:

1.  On the leader node, run ``nano ~/.toilAzureCredentials``.
2.  In the editor that opens, navigate with the arrow keys, and give the file the following contents::

        [AzureStorageCredentials]
        <accountname>=<accountkey>

    Be sure to replace ``<accountname>`` with the name that you used for your Azure Storage Account, and ``<accountkey>`` with the key you obtained above. (If you want, you can have multiple accounts with different keys in this file, by adding multipe lines. If you do this, be sure to leave the ``AZURE_ACCOUNT_KEY`` environment variable unset.)

3.  Press ``ctrl-o`` to save the file, and ``ctrl-x`` to exit the editor.

Once that's done, you are now ready to actually execute a job, storing your job store in that Azure Storage Account. Assuming you followed the :ref:`quickstart` guide above, you have an Azure Storage Account created, and you have placed the Storage Account's access key on the cluster, you can run the **HelloWorld.py** script by doing the following:

1.  Place your script on the leader node, either by downloading it from the command line or typing or copying it into a command-line editor.
2.  Run the command::

        python --batchSystem=mesos --mesosMaster=10.0.0.5:5050 \
                        HelloWorld.py azure:<accountname>:hello-world001

    To run a CWL workflow::

        cwltoil --batchSystem=mesos --mesosMaster=10.0.0.5:5050 \
                        --jobStore=azure:<accountname>:hello-world001 \
                        example.cwl example-job.yml

    Be sure to replace ``<accountname>`` with the name of your Azure Storage Account.

Note that once you run a job with a particular job store name (the part after the account name) in a particular Storage Account, you cannot re-use that name in that account unless one of the following happens:

1. You are restarting the same job with the ``--restart`` option.
2. You clean the job store with ``toil clean azure:<accountname>:<jobstore>``.
3. You delete all the items created by that job, and the main job store table used by Toil, from the account (destroying all other job stores using the account).
4. The job finishes successfully and cleans itself up.

.. _runningOpenStack:

Running on Open Stack
---------------------

After getting setup with :ref:`installationOpenStack`, Toil scripts can be run just by designating a job store
location as shown in :ref:`quickstart`.  The location of temporary directories Toil creates to run jobs
can be specified with ``--workDir``::

    python HelloWorld.py file:jobStore --workDir /tmp/

.. _runningGoogleComputeEngine:

Running on Google Compute Engine
--------------------------------

After getting setup with :ref:`installationGoogleComputeEngine`, Toil scripts can be run just by designating a job store
location as shown in :ref:`quickstart`.

If you wish to use the Google Storage job store, you must install Toil with the 'google' extra.
Having done this, you must create a file named '.boto' in your home directory with the following format::

    [Credentials]
    gs_access_key_id = KEY_ID
    gs_secret_access_key = SECRET_KEY

    [Boto]
    https_validate_certificates = True

    [GSUtil]
    content_language = en
    default_api_version = 2

The gs_access_key_id and gs_secret_access_key can be generated by navigating to your Google Cloud Storage console and clicking on 'Settings'.
Then, on the Settings page, navigate to the Interoperability tab and click 'Enable interoperability access'. On this page you can now click 'Create a new key' to generate an access key and a matching secret.
Insert these into their respective places in the .boto file and you will be able to use a Google job store when invoking a Toil script, as in the following example::

    python HelloWorld.py google:projectID:jobStore

The 'projectID' component of the job store argument above refers your Google Cloud project ID in the Google Cloud Console, and will be visible in the console's banner at the top of the screen.
The 'jobStore' component is a name of your choosing that you will use to refer to this job store. 
