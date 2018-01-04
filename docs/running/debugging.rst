.. _debugging:

Toil Debugging
==============

Toil has a number of tools to assist in debugging.  Here we provide help in working through potential problems that a user might encounter in attempting to run a workflow.

Introspecting the Jobstore
--------------------------

To view what files currently reside in the jobstore, run the following command::

    $ toil-debug file:path-to-jobstore-directory --listFilesInJobStore

When run from the commandline, this should generate a file containing the contents of the job store (in addition to
displaying a series of log messages to the terminal).  This file is named "jobstore_files.txt" by default and will be
generated in the current workign directory.

If one wishes to copy these files to a local directory, one can run:

    $ toil-debug file:path-to-jobstore --fetchTheseJobStoreFiles="*.bam"+"*.fastq"

Example: Debugging a Broken Workflow
------------------------------------

We've generously provided a broken workflow to assist in debugging toil.  The jobGraph for this workflow looks like this:

IMAGE OF JOBGRAPH

This type of graph can be reproduced for any toil workflow using printDot, by running the command:

    $ toilStatus.py file:path-to-jobstore --printDot

The job labeled broken job is