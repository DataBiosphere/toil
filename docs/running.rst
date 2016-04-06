.. _running:

Running A Workflow
==================

.. _quickstart:

Running Quick Start
-------------------

Starting with Python, a Toil workflow can be run with just three simple steps.

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

Running CWL and WDL Workflows
-----------------------------

TODO

Running in the Cloud
====================

There are several recommended ways to run Toil jobs in the cloud. Of these, running on Amazon Web Services (AWS) is currently the best-supported solution.

On all cloud providers, it is recommended that you run long-running jobs on remote systems under ``screen``. Simply type ``screen`` to open a new ``screen` session. Later, type ``ctrl-a`` and then ``d`` to disconnect from it, and run ``screen -r`` to reconnect to it. Commands running under ``screen`` will continue running even when you are disconnected, allowing you to unplug your laptop and take it home without ending your Toil jobs.


.. _runningAWS:

Running on AWS
--------------
See :ref:`installationAWS` to get setup for running on AWS.

Having followed the :ref:`quickstart` guide, the user can run their **HelloWorld.py** script on a distributed cluster
just by modifiying the run command.  We'll use the **AWS Jobstore**, which uses S3 as a intermediate job store.

Place the HelloWorld.py script on the leader node, and run::

    python --batchSystem=mesos --mesosMaster=mesos-master:5050 \
                    HelloWorld.py aws:us-west-2:my-s3-jobstore

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
                      
    Be sure to replace ``<accountname>`` with the name of your Azure Storage Account.
    
Note that once you run a job with a particular job store name (the part after the account name) in a particular Storage Account, you cannot re-use that name in that account unless one of the following happens:

1. You are restarting the same job with the ``--restart`` option.
2. You clean the job store with ``toil clean azure:<accountname>:<jobstore>``.
3. You delete all the items created by that job, and the main job store table used by Toil, from the account (destroying all other job stores using the account).
4. The job finishes successfully and cleans itself up.

.. _runningOpenStack:

Running on Open Stack
---------------------

After getting setup with :ref:`installationOpenStack`, Toil scripts can be run just be designating a location of
the job store as shown in :ref:`quickstart`.  The temporary directories Toil creates to run jobs can be specified
with ``--workDir``::

    python HelloWorld.py file:jobStore --workDir /tmp/

