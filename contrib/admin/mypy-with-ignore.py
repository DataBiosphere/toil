#!/usr/bin/env python3
"""
Runs mypy and ignores files that do not yet have passing type hints.

Does not type check test files (any path including "src/toil/test").
"""
import os
import subprocess
import sys

os.environ['MYPYPATH'] = 'contrib/mypy-stubs'
pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from src.toil.lib.resources import glob  # type: ignore


def main():
    all_files_to_check = []
    for d in ['dashboard', 'docker', 'docs', 'src']:
        all_files_to_check += glob(glob_pattern='*.py', directoryname=os.path.join(pkg_root, d))

    # TODO: Remove these paths as typing is added and mypy conflicts are addressed.
    # These are handled as path prefixes.

    ignore_paths = [os.path.join(pkg_root, f) for f in [
        'docs/_build',
        'docker/Dockerfile.py',
        'docs/conf.py',
        'docs/vendor/sphinxcontrib/fulltoc.py',
        'docs/vendor/sphinxcontrib/__init__.py',
        'src/toil/job.py',
        'src/toil/leader.py',
        'src/toil/__init__.py',
        'src/toil/deferred.py',
        'src/toil/version.py',
        'src/toil/provisioners/abstractProvisioner.py',
        'src/toil/provisioners/gceProvisioner.py',
        'src/toil/provisioners/__init__.py',
        'src/toil/provisioners/node.py',
        'src/toil/provisioners/aws/boto2Context.py',
        'src/toil/provisioners/aws/__init__.py',
        'src/toil/batchSystems/gridengine.py',
        'src/toil/batchSystems/singleMachine.py',
        'src/toil/batchSystems/torque.py',
        'src/toil/batchSystems/options.py',
        'src/toil/batchSystems/registry.py',
        'src/toil/batchSystems/lsf.py',
        'src/toil/batchSystems/__init__.py',
        'src/toil/batchSystems/abstractGridEngineBatchSystem.py',
        'src/toil/batchSystems/awsBatch.py',
        'src/toil/batchSystems/lsfHelper.py',
        'src/toil/batchSystems/htcondor.py',
        'src/toil/batchSystems/mesos/batchSystem.py',
        'src/toil/batchSystems/mesos/executor.py',
        'src/toil/batchSystems/mesos/conftest.py',
        'src/toil/batchSystems/mesos/__init__.py',
        'src/toil/batchSystems/mesos/test/__init__.py',
        'src/toil/fileStores/cachingFileStore.py',
        'src/toil/jobStores/utils.py',
        'src/toil/jobStores/conftest.py',
        'src/toil/jobStores/fileJobStore.py',
        'src/toil/jobStores/__init__.py',
        'src/toil/jobStores/googleJobStore.py',
        'src/toil/jobStores/aws/utils.py',
        'src/toil/jobStores/aws/jobStore.py',
        'src/toil/jobStores/aws/__init__.py',
        'src/toil/utils/__init__.py',
        'src/toil/lib/throttle.py',
        'src/toil/lib/bioio.py',
        'src/toil/lib/ec2.py',
        'src/toil/lib/expando.py',
        'src/toil/lib/exceptions.py',
        'src/toil/lib/__init__.py',
        'src/toil/lib/generatedEC2Lists.py',
        'src/toil/lib/retry.py',
        'src/toil/lib/threading.py',
        'src/toil/lib/objects.py',
        'src/toil/lib/io.py',
        'src/toil/lib/docker.py',
        'src/toil/lib/encryption/_dummy.py',
        'src/toil/lib/encryption/conftest.py',
        'src/toil/lib/encryption/__init__.py',
        'src/toil/lib/aws/__init__.py',
        'src/toil/server/utils.py',
        'src/toil/test',
        'src/toil/utils/toilStats.py',
        'src/toil/server/utils.py'
    ]]

    def ignore(file_path):
        """
        Return True if a file should be ignored.
        """
        for prefix in ignore_paths:
            if file_path.startswith(prefix):
                return True
        return False

    filtered_files_to_check = []
    for file_path in all_files_to_check:
        if not ignore(file_path):
            filtered_files_to_check.append(file_path)
    args = ['mypy', '--color-output', '--show-traceback'] + filtered_files_to_check
    p = subprocess.run(args=args)
    exit(p.returncode)


if __name__ == '__main__':
    main()
