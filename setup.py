# Copyright (C) 2015-2016 Regents of the University of California
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from setuptools import find_packages, setup
import sys

def runSetup():
    """
    Calls setup(). This function exists so the setup() invocation preceded more internal
    functionality. The `version` module is imported dynamically by importVersion() below.
    """
    boto = 'boto==2.38.0'
    boto3 = 'boto3==1.4.7'
    futures = 'futures==3.2.0'
    pycryptodome = 'pycryptodome==3.5.1'
    psutil = 'psutil==3.0.1'
    protobuf = 'protobuf==3.5.1'
    azureCosmosdbTable = 'azure-cosmosdb-table==0.37.1'
    azureAnsible = 'ansible[azure]==2.5.0a1'
    azureStorage = 'azure-storage==0.35.1'
    secretstorage = 'secretstorage<3'
    pynacl = 'pynacl==1.1.2'
    gcs = 'google-cloud-storage==1.6.0'
    gcs_oauth2_boto_plugin = 'gcs_oauth2_boto_plugin==1.14'
    apacheLibcloud = 'apache-libcloud==2.2.1'
    cwltool = 'cwltool==1.0.20180518123035'
    schemaSalad = 'schema-salad >= 2.6, < 3'
    galaxyLib = 'galaxy-lib==17.9.3'
    htcondor = 'htcondor>=8.6.0'

    mesos_reqs = [
        psutil,
        protobuf]
    aws_reqs = [
        boto,
        boto3,
        futures,
        pycryptodome]
    azure_reqs = [
        azureCosmosdbTable,
        secretstorage,
        azureAnsible,
        azureStorage]
    encryption_reqs = [
        pynacl]
    google_reqs = [
        gcs_oauth2_boto_plugin,  # is this being used??
        apacheLibcloud,
        gcs]
    cwl_reqs = [
        cwltool,
        schemaSalad,
        galaxyLib]
    wdl_reqs = []
    htcondor_reqs = [
        htcondor]

    all_reqs = \
        mesos_reqs + \
        aws_reqs + \
        azure_reqs + \
        encryption_reqs + \
        google_reqs + \
        cwl_reqs + \
        htcondor_reqs

    # htcondor is not supported by apple
    if sys.platform != 'linux' or 'linux2':
        all_reqs.remove(htcondor)

    if not sys.version_info[0] == 2:
        raise RuntimeError("Toil currently requires Python 2, but we're working on adding Python 3 support (#1780)")

    setup(
        name='toil',
        version=version.distVersion,
        python_requires='~=2.7',
        description='Pipeline management software for clusters.',
        author='Benedict Paten',
        author_email='benedict@soe.usc.edu',
        url="https://github.com/BD2KGenomics/toil",
        classifiers=["License :: OSI Approved :: Apache Software License"],
        license="Apache License v2.0",
        install_requires=[
            'dill==0.2.7.1',
            'six>=1.10.0',
            'future',
            'requests==2.18.4',
            'docker==2.5.1',
            'subprocess32==3.5.1',
            'python-dateutil'],
        extras_require={
            'mesos': mesos_reqs,
            'aws': aws_reqs,
            'azure': azure_reqs,
            'encryption': encryption_reqs,
            'google': google_reqs,
            'cwl': cwl_reqs,
            'wdl': wdl_reqs,
            'htcondor': htcondor_reqs,
            'all': all_reqs},
        package_dir={'': 'src'},
        packages=find_packages(where='src',
                               # Note that we intentionally include the top-level `test` package for
                               # functionality like the @experimental and @integrative decoratorss:
                               exclude=['*.test.*']),
        package_data = {
            '': ['*.yml', 'contrib/azure_rm.py', 'cloud-config'],
        },
        # Unfortunately, the names of the entry points are hard-coded elsewhere in the code base so
        # you can't just change them here. Luckily, most of them are pretty unique strings, and thus
        # easy to search for.
        entry_points={
            'console_scripts': [
                'toil = toil.utils.toilMain:main',
                '_toil_worker = toil.worker:main',
                'cwltoil = toil.cwl.cwltoil:main [cwl]',
                'toil-cwl-runner = toil.cwl.cwltoil:main [cwl]',
                'toil-wdl-runner = toil.wdl.toilwdl:main',
                '_toil_mesos_executor = toil.batchSystems.mesos.executor:main [mesos]']})


def importVersion():
    """
    Load and return the module object for src/toil/version.py, generating it from the template if
    required.
    """
    import imp
    try:
        # Attempt to load the template first. It only exists in a working copy cloned via git.
        import version_template
    except ImportError:
        # If loading the template fails we must be in a unpacked source distribution and
        # src/toil/version.py will already exist.
        pass
    else:
        # Use the template to generate src/toil/version.py
        import os
        import errno
        from tempfile import NamedTemporaryFile

        new = version_template.expand_()
        try:
            with open('src/toil/version.py') as f:
                old = f.read()
        except IOError as e:
            if e.errno == errno.ENOENT:
                old = None
            else:
                raise

        if old != new:
            with NamedTemporaryFile(dir='src/toil', prefix='version.py.', delete=False) as f:
                f.write(new)
            os.rename(f.name, 'src/toil/version.py')
    # Unfortunately, we can't use a straight import here because that would also load the stuff
    # defined in src/toil/__init__.py which imports modules from external dependencies that may
    # yet to be installed when setup.py is invoked.
    return imp.load_source('toil.version', 'src/toil/version.py')


version = importVersion()
runSetup()
