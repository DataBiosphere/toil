Running Spark on Toil
=====================

General Architecture
--------------------

Toil's Service Job infrastructure (:class:`toil.job.ServiceJob`) allows for
the creation of long lived services that persist for the whole duration of
a subtree of a Toil workflow DAG. This infrastructure can be used to implement
a cluster running Apache Spark_ [ZAHARIA12]_ and the Apache Hadoop_
Distributed File System (HDFS). We run Apache Spark in standalone mode on
top of Toil.

.. _Apache Spark: http://spark.apache.org
.. [ZAHARIA12] Zaharia, Matei, et al. "Resilient distributed datasets: \
   A fault-tolerant abstraction for in-memory cluster computing." \
   Proceedings of the 9th USENIX conference on Networked Systems Design and \
   Implementation. USENIX Association, 2012.
.. _Apache Hadoop: http://hadoop.apache.org

Similar to Toil, both Spark and HDFS use a master/slave architecture. In Spark,
the application driver (typically the user application or shell) submits work
to the Spark master, which manages the scheduling and distribution of work
across a collection of Spark workers. Spark is typically used with a distributed
mounted file system, that ensures that a given file can be accessed by all of
the workers in the cluster. We run Spark on top of HDFS, which is a typical
configuration. However, Spark can also load data from block stores like S3, or
from HPC style distributed file systems like NFS, Gluster, or Ceph. In HDFS,
the master is called the namenode, and workers are datanodes. The HDFS namenode
manages file metadata across the cluster, while the datanodes store the raw data.
HDFS has very good performance for large reads; write performance varies with
the cluster configuration.

We decompose this cluster into two separate classes inside of Toil:
:class:`toil.spark.spark_cluster.SparkCluster` and
:class:`toil.spark.spark_cluster.WorkerService`. The
:class:`toil.spark.spark_cluster.SparkCluster` class launches the Spark master
and HDFS namenode, and then adds one or more
:class:`toil.spark.spark_cluster.WorkerService` child services. The
:class:`toil.spark.spark_cluster.WorkerService` class then launches the
Spark worker and HDFS datanode. The individual Spark and HDFS applications are
run using docker containers from cgl-docker-lib_.

.. _cgl-docker-lib: https://github.com/BD2KGenomics/cgl-docker-lib

Launching a Spark Cluster
-------------------------

To launch a Spark cluster, import:

::
        from toil.spark.spark_cluster import spawn_spark_cluster

Then, add a service job:

::
        masterIPPromise = spawn_spark_cluster(job, ...)

The service job will return a promise for the IP address of the master node.
The service job takes several parameters:

- job: The job that the Spark cluster should descend from.
- sudo: Whether or not to run the Docker containers with sudo permission.
- numWorkers: The number of workers to start in the cluster. Must be greater than
  or equal to one.

We also take several optional parameters:

- cores: An optional parameter to set the number of cores that each machine should
  use. If not provided, we will auto-detect the cores on a worker.
- overrideLeaderIP: an optional parameter that can override the leader IP.
- memory: The amount of memory to allocate for each worker used by Spark.
- disk: The amount of disk to request for each worker.

When launching a Spark cluster, there is one important caveat: the job that
launches the services must be executed with `checkpoint = True`. If the job is
not checkpointable, then the cluster may not restart robustly. This is important
if you are running on a preemptable cluster: in Spark and HDFS, failed workers can
be restarted, but a failed master will cause all workers in the cluster to
dissociate. If the cluster does not restart, this causes a deadlock.

Using Locally
-------------

To use Spark locally, you must install Toil and Docker. Docker installation
instructions can be found [here](https://docs.docker.com/engine/installation/).
For examples of how to connect to a Toil-based Spark cluster, see the Spark
unit tests:

::
        make test tests=src/toil/test/spark/sparkTest.py

These unit tests create a Spark cluster, and then run a simple job using PySpark.

You must install Spark and make PySpark available on your
Python PATH. Pre-built binaries for Spark can be downloaded from the
[Apache Spark downloads page](http://spark.apache.org/downloads.html). Once
these are downloaded, untar the archive, and add the following location
to your PYTHONPATH environment variable:

::
        export PYTHONPATH=${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.1-src.zip:${PYTHONPATH}

SPARK_HOME should be the location where you installed Spark. We run
Spark 1.5.2 by default, but are compatible with all Spark versions prior
to 1.5.2, and may be compatible with some Spark 1.6.x programs.