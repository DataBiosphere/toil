#!/usr/bin/env python3
"""
Runs mypy and ignores files that do not yet have passing type hints.

Does not type check test files (any path including "src/toil/test").
"""
import os
import subprocess
import sys

os.environ['MYPYPATH'] = 'contrib/typeshed'
pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from src.toil.lib.resources import glob  # type: ignore


def main():
    all_files_to_check = []
    for d in ['dashboard', 'docker', 'docs', 'src']:
        all_files_to_check += glob(glob_pattern='*.py', directoryname=os.path.join(pkg_root, d))

    # TODO: Remove these paths as typing is added and mypy conflicts are addressed
    ignore_paths = [os.path.abspath(f) for f in [
        'docker/Dockerfile.py',
        'docs/conf.py',
        'docs/vendor/sphinxcontrib/fulltoc.py',
        'docs/vendor/sphinxcontrib/__init__.py',
        'src/toil/job.py',
        'src/toil/leader.py',
        'src/toil/statsAndLogging.py',
        'src/toil/common.py',
        'src/toil/realtimeLogger.py',
        'src/toil/worker.py',
        'src/toil/serviceManager.py',
        'src/toil/toilState.py',
        'src/toil/__init__.py',
        'src/toil/resource.py',
        'src/toil/deferred.py',
        'src/toil/version.py',
        'src/toil/wdl/utils.py',
        'src/toil/wdl/wdl_types.py',
        'src/toil/wdl/wdl_synthesis.py',
        'src/toil/wdl/wdl_analysis.py',
        'src/toil/wdl/wdl_functions.py',
        'src/toil/wdl/toilwdl.py',
        'src/toil/wdl/versions/draft2.py',
        'src/toil/wdl/versions/v1.py',
        'src/toil/wdl/versions/dev.py',
        'src/toil/provisioners/clusterScaler.py',
        'src/toil/provisioners/abstractProvisioner.py',
        'src/toil/provisioners/gceProvisioner.py',
        'src/toil/provisioners/__init__.py',
        'src/toil/provisioners/node.py',
        'src/toil/provisioners/aws/boto2Context.py',
        'src/toil/provisioners/aws/awsProvisioner.py',
        'src/toil/provisioners/aws/__init__.py',
        'src/toil/batchSystems/slurm.py',
        'src/toil/batchSystems/gridengine.py',
        'src/toil/batchSystems/singleMachine.py',
        'src/toil/batchSystems/abstractBatchSystem.py',
        'src/toil/batchSystems/parasol.py',
        'src/toil/batchSystems/kubernetes.py',
        'src/toil/batchSystems/torque.py',
        'src/toil/batchSystems/options.py',
        'src/toil/batchSystems/registry.py',
        'src/toil/batchSystems/lsf.py',
        'src/toil/batchSystems/__init__.py',
        'src/toil/batchSystems/abstractGridEngineBatchSystem.py',
        'src/toil/batchSystems/lsfHelper.py',
        'src/toil/batchSystems/htcondor.py',
        'src/toil/batchSystems/mesos/batchSystem.py',
        'src/toil/batchSystems/mesos/executor.py',
        'src/toil/batchSystems/mesos/conftest.py',
        'src/toil/batchSystems/mesos/__init__.py',
        'src/toil/batchSystems/mesos/test/__init__.py',
        'src/toil/cwl/conftest.py',
        'src/toil/cwl/__init__.py',
        'src/toil/cwl/cwltoil.py',
        'src/toil/fileStores/cachingFileStore.py',
        'src/toil/fileStores/abstractFileStore.py',
        'src/toil/fileStores/nonCachingFileStore.py',
        'src/toil/fileStores/__init__.py',
        'src/toil/jobStores/utils.py',
        'src/toil/jobStores/abstractJobStore.py',
        'src/toil/jobStores/conftest.py',
        'src/toil/jobStores/fileJobStore.py',
        'src/toil/jobStores/__init__.py',
        'src/toil/jobStores/googleJobStore.py',
        'src/toil/jobStores/aws/utils.py',
        'src/toil/jobStores/aws/jobStore.py',
        'src/toil/jobStores/aws/__init__.py',
        'src/toil/utils/toilDebugFile.py',
        'src/toil/utils/toilStatus.py',
        'src/toil/utils/toilStats.py',
        'src/toil/utils/__init__.py',
        'src/toil/utils/toilLaunchCluster.py',
        'src/toil/lib/memoize.py',
        'src/toil/lib/throttle.py',
        'src/toil/lib/humanize.py',
        'src/toil/lib/compatibility.py',
        'src/toil/lib/iterables.py',
        'src/toil/lib/bioio.py',
        'src/toil/lib/ec2.py',
        'src/toil/lib/ec2nodes.py',
        'src/toil/lib/expando.py',
        'src/toil/lib/threading.py',
        'src/toil/lib/exceptions.py',
        'src/toil/lib/__init__.py',
        'src/toil/lib/generatedEC2Lists.py',
        'src/toil/lib/retry.py',
        'src/toil/lib/objects.py',
        'src/toil/lib/io.py',
        'src/toil/lib/docker.py',
        # 'src/toil/lib/encryption/_nacl.py',
        'src/toil/lib/encryption/_dummy.py',
        'src/toil/lib/encryption/conftest.py',
        'src/toil/lib/encryption/__init__.py',
        'src/toil/lib/aws/utils.py',
        'src/toil/lib/aws/__init__.py'
    ]]

    filtered_files_to_check = []
    for file_path in all_files_to_check:
        if file_path not in ignore_paths and 'src/toil/test' not in file_path:
            filtered_files_to_check.append(file_path)
    # follow-imports type checks pypi projects we don't control, so we skip it; why is this their default?
    args = ['mypy', '--follow-imports=skip'] + filtered_files_to_check
    p = subprocess.run(args=args, stdout=subprocess.PIPE)
    result = p.stdout.decode()
    print(result)
    if 'Success: no issues found' not in result:
        exit(1)


if __name__ == '__main__':
    main()
