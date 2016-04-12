Toil Architecture
*****************

The following diagram layouts out the software architecture of Toil. 

.. figure:: toil_architecture.jpg
    :width: 350px
    :align: center
    :height: 350px
    :alt: alternate text
    :figclass: align-center

    Figure 1: The basic components of the toil architecture. Note the node provisioning 
    is coming soon.

These components are described below: 
    * the leader:
        The leader is responsible for deciding which jobs should be run. To do this 
        it traverses the job graph. Currently this is a single threaded process, 
        but we make aggressive steps to prevent it becoming a bottleneck
        (see `Read-only Leader`_ described below).
    * the job-store:
        Handles all files shared between the components. Files in the job-store are the means
        by which the state of the workflow is maintained. Each job is backed by a file
        in the job store, and atomic updates to this state are used to ensure the workflow
        can always be resumed upon failure. The job-store can also store all user
        files, allowing them to be shared between jobs. The job-store is defined by the abstract
        class :class:`toil.jobStores.AbstractJobStore`. Multiple implementations of this
        class allow Toil to support different back-end file stores, e.g.: S3, network file systems,
        Azure file store, etc.
    * workers:
        The workers are temporary processes responsible for running jobs, 
        one at a time per worker. Each worker process is invoked with a job argument
        that it is responsible for running. The worker monitors this job and reports
        back success or failure to the leader by editing the job's state in the file-store. 
        If the job defines successor jobs the worker may choose to immediately run them
        (see `Job Chaining`_ below).
    * the batch-system: 
        Responsible for scheduling the jobs given to it by the leader, creating a 
        worker command for each job. The batch-system is defined by the abstract class
        class :class:`toil.batchSystems.AbstractBatchSystem`. Toil uses multiple existing
        batch systems to schedule jobs, including Apache Mesos, GridEngine and a multi-process
        single node implementation that allows workflows to be run without any of these frameworks.
        Toil can therefore fairly easily be made to run a workflow using an existing cluster.
    * the node provisioner:
        Creates worker nodes in which the batch system schedules workers. This is currently
        being developed. It is defined by the abstract class :class:`toil.provisioners.AbstractProvisioner`.
    * the statistics and logging monitor: 
        Monitors logging and statistics produced by the workers and reports them. Uses the 
        job-store to gather this information.

Optimizations
-------------

Toil implements lots of optimizations designed to allow it to scale out. 
Many are not worth mentioning, but we detail some of the key ones below that we
have found useful.

Read-only Leader
~~~~~~~~~~~~~~~~

The leader process is currently implemented as a single thread. Most of the leader's
tasks revolve around processing the state of jobs, each stored as a file within the job-store.
To minimise the load on this thread, each worker does as much work as possible 
to manage the state of the job it is running. As a result, with a couple of minor exceptions, 
the leader process never needs to write or update the state of a job within the job-store. 
For example, when a job is complete and has no further successors the responsible 
worker deletes the job from the job-store, marking it complete. The leader then 
only has to check for the existence of the file when it receives a signal from the batch-system
to know that the job is complete. This off-loading of state management is orthogonal to
future parallelization of the leader. 

Job Chaining
~~~~~~~~~~~~

The scheduling of successor jobs is partially managed by the worker, reducing the 
number of individual jobs the leader needs to process. Currently this is very 
simple: if the there is a single next successor job to run and it's resources fit within the
resources of the current job and closely match the resources of the current job then  
the job is run immediately on the worker without returning to the leader. Further extensions
of this strategy are possible, but for many workflows which define a series of serial successors
(e.g. map sequencing reads, post-process mapped reads, etc.) this pattern is very effective
at reducing leader workload. 

Preemptable Node Support
~~~~~~~~~~~~~~~~~~~~~~~~

Critical to running at large-scale is dealing with intermittent node failures. Toil is
therefore designed to always be resumable providing the job-store does not become corrupt. 
This robustness allows Toil to run on preemptible nodes, which are only available when others are not 
willing to pay more to use them. Designing workflows that divide into many short individual jobs 
that can use preemptable nodes allows for workflows to be efficiently scheduled and executed.  

Caching
~~~~~~~

Running bioinformatic pipelines often require the passing of large datasets between jobs. Toil
caches the results from jobs such that child jobs running on the same node can directly use the same
file objects, thereby eliminating the need for an intermediary transfer to the job store. Caching
also reduces the burden on the local disks, because multiple jobs can share a single file.
The resulting drop in I/O allows pipelines to run faster, and, by the sharing of files, 
allows users to run more jobs in parallel by reducing overall disk requirements.  

To demonstrate the efficiency of caching, we ran an experimental internal pipeline on 3 samples from
the TCGA Lung Squamous Carcinoma (LUSC) dataset. The pipeline takes the tumor and normal exome
fastqs, and the tumor rna fastq and input, and predicts MHC presented neoepitopes in the patient
that are potential targets for T-cell based immunotherapies. The pipeline was run individually on
the samples on c3.8xlarge machines on AWS (60GB RAM,600GB SSD storage, 32 cores). The pipeline
aligns the data to hg19-based references, predicts MHC haplotypes using PHLAT, calls mutations using
2 callers (MuTect and RADIA) and annotates them using SnpEff, then predicts MHC:peptide binding
using the IEDB suite of tools before running an in-house rank boosting algorithm on the final calls.

To optimize time taken, The pipeline is written such that mutations are called on a per-chromosome
basis from the whole-exome bams and are merged into a complete vcf. Running mutect in parallel on
whole exome bams requires each mutect job to download the complete Tumor and Normal Bams to their
working directories -- An operation that quickly fills the disk and limits the parallelizability of
jobs. The script was run in Toil, with and without caching, and Figure 2 shows that the workflow
finishes faster in the cached case while using less disk on average than the uncached run. We
believe that benefits of caching arising from file transfers will be much higher on magnetic
disk-based storage systems as compared to the SSD systems we tested this on.

.. figure:: caching_benefits.png
    :width: 700px
    :align: center
    :height: 1000px
    :alt: alternate text
    :figclass: align-center

    Figure 2: Efficiency gain from caching. The lower half of each plot describes the disk used by
    the pipeline recorded every 10 minutes over the duration of the pipeline, and the upper half
    shows the corresponding stage of the pipeline that is being processed. Since jobs requesting the
    same file shared the same inode, the effective load on the disk is considerably lower than in
    the uncached case where every job downloads a personal copy of every file it needs. We see that
    in all cases, the uncached run uses almost 300-400GB more that the uncached run in the resource
    heavy mutation calling step. We also see a benefit in terms of wall time for each stage since we
    eliminate the time taken for file transfers.
