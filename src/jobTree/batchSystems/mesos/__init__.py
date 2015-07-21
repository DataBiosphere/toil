from collections import namedtuple

TaskData = namedtuple('TaskData', (
    # Time when the task was started
    'startTime',
    # Mesos' ID of the slave where task is being run
    'slaveID',
    # Mesos' ID of the executor running the task
    'executorID'))

ResourceRequirement = namedtuple('ResourceRequirement', (
    # Number of bytes (!) needed for a task
    'memory',
    # Number of CPU cores needed for a task
    'cpu'
    # Number of bytes (!) needed for task on disk
    'storage'))

JobTreeJob = namedtuple('JobTreeJob', (
    # A job ID specific to this batch system implementation
    'jobID',
    # A ResourceRequirement tuple describing the resources needed by this job
    'resources',
    # The command to be run on the worker node
    'command',
    # The resource object representing the user script
    'userScript',
    # The resource object representing the jobTree source tarball
    'jobTreeDistribution'))

