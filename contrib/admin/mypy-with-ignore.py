#!/usr/bin/env python3
"""
Runs mypy and ignores files that do not yet have passing type hints.

Does not type check test files (any path including "src/toil/test").
"""
import os
import subprocess
import sys

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from src.toil.lib.resources import glob  # type: ignore


def main():
    all_files_to_check = []
    for d in ['contrib', 'dashboard', 'docker', 'docs', 'src']:
        all_files_to_check += glob(glob_pattern='*.py', directoryname=os.path.join(pkg_root, d))

    # TODO: Remove these paths as typing is added and mypy conflicts are addressed
    ignore_paths = [os.path.abspath(f) for f in [
        'src/toil/lib/encryption/conftest.py',
        'src/toil/lib/encryption/_nacl.py',
        'src/toil/batchSystems/mesos/conftest.py',
        'src/toil/lib/threading.py',
        'src/toil/lib/docker.py',
        'src/toil/lib/retry.py',
        'src/toil/lib/ec2.py',
        'src/toil/jobStores/aws/utils.py',
        'src/toil/__init__.py',
        'src/toil/provisioners/aws/boto2Context.py',
        'docs/conf.py',
        'src/toil/provisioners/abstractProvisioner.py',
        'src/toil/provisioners/aws/awsProvisioner.py',
        'src/toil/deferred.py',
        'src/toil/common.py',
        'src/toil/fileStores/abstractFileStore.py',
        'src/toil/job.py',
        'src/toil/jobStores/abstractJobStore.py',
        'src/toil/fileStores/nonCachingFileStore.py',
        'src/toil/jobStores/aws/jobStore.py',
        'src/toil/jobStores/googleJobStore.py',
        'src/toil/cwl/cwltoil.py',
        'src/toil/batchSystems/abstractBatchSystem.py',
        'src/toil/provisioners/gceProvisioner.py',
        'src/toil/worker.py',
        'src/toil/wdl/wdl_analysis.py',
        'src/toil/wdl/versions/v1.py',
        'src/toil/wdl/versions/draft2.py',
        'src/toil/batchSystems/htcondor.py',
        'src/toil/wdl/versions/dev.py',
        'src/toil/leader.py',
        'src/toil/batchSystems/mesos/batchSystem.py',
        'src/toil/batchSystems/mesos/executor.py',
        'src/toil/batchSystems/kubernetes.py',
        'src/toil/wdl/wdl_functions.py'
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
