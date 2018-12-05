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

from setuptools import find_packages, setup, command

try:
    # On Python 3 urlopen is here
    from urllib.request import urlopen
    from urllib.error import URLError
except ImportError:
    # On Python 2 it is here
    from urllib2 import urlopen, URLError

import sys
import uuid
import inspect
import importlib

# We have an analytics system to keep track of installs. The next few functions
# support that. TODO: We can't hook a wheel being installed, only made. See
# <https://github.com/pypa/packaging-problems/issues/64>

def phone_home(event):
    """
    Report setup.py activity events and Toil installation/packaging to the UCSC
    Genomics Institute.
    
    Reports high bits of the IP address, Toil version, and the setuptools
    command being used.
    
    Does not report any personally identifiable information. Uses a random
    ephemeral user ID for every action, even if an install takes multiple
    actions, or if the same machine installs multiple times.
    """
    
    print("reporting {} to UCSC Genomics Institute".format(event))
        
    # Compose the GA report
    report_data = '&'.join([
        'v=1',
        'aip=1',
        'tid=UA-103168923-3',
        'cid=' + str(uuid.uuid4()),
        't=event',
        'ec=lifecycle',
        'ea=' + event,
        'an=toil',
        'av=' + version.distVersion,
        'aid=edu.ucsc.gi.toil'
    ]).encode('utf-8')
    
    # We assume that Toil version numbers don't need to be URL encoded.
    # That means they can't contain +
    
    print(report_data)
    
    try:
        # Send the data and open the reply with a 1 second timeout
        handle = urlopen('https://www.google-analytics.com/collect', report_data, 1.0)
        print("report sent")
    except URLError:
        print("could not report event; continuing")
        
def wrap_command(imported_command, command_name):
    """
    Wrap a single setuptools command class.
    Uses its local scope so we can find the base class.
    """
    
    class WrappedCommand(imported_command):
        """
        Setuptools install command wrapper that phones home.
        """
        
        def run(self):
            # First do the base work
            # Problem: we need to get ahold of the original imported command but our enclosing scope vars may have changed.
            imported_command.run(self)
            
            # Then report in
            phone_home(command_name)
            
    return WrappedCommand
    
    
def wrap_commands():
    """
    Wrap all commands from setuptools.command and make them phone home.
    
    Returns a dict from Setuptools command name to wrapped implementing class.
    """
    
    to_return = {}
    
    for command_name in command.__all__:
        # For each (basic) command defined by setuptools
        # We expect it to be a class with the command name in a module with the command name
        
        # Grab it by import. These all need importing.
        try:
            # We expect a class named after the module in each module.
            imported_command = importlib.import_module('setuptools.command.{}'.format(command_name)).__dict__[command_name]
        except:
            # If it's not there, skip this module
            continue
        
        if not inspect.isclass(imported_command):
            # This is some other child module
            continue
        
        # Save the wrapping class
        to_return[command_name] = wrap_command(imported_command, command_name)
        
    return to_return
                
def runSetup():
    """
    Calls setup(). This function exists so the setup() invocation preceded more internal
    functionality. The `version` module is imported dynamically by importVersion() below.
    """
    boto = 'boto==2.48.0'
    boto3 = 'boto3>=1.7.50, <2.0'
    futures = 'futures==3.1.1'
    pycryptodome = 'pycryptodome==3.5.1'
    pymesos = 'pymesos==0.3.7'
    psutil = 'psutil==3.0.1'
    azureCosmosdbTable = 'azure-cosmosdb-table==0.37.1'
    azureAnsible = 'ansible[azure]==2.5.0a1'
    azureStorage = 'azure-storage==0.35.1'
    secretstorage = 'secretstorage<3'
    pynacl = 'pynacl==1.1.2'
    gcs = 'google-cloud-storage==1.6.0'
    gcs_oauth2_boto_plugin = 'gcs_oauth2_boto_plugin==1.14'
    apacheLibcloud = 'apache-libcloud==2.2.1'
    cwltool = 'cwltool==1.0.20181118133959'
    schemaSalad = 'schema-salad>=2.6, <3'
    galaxyLib = 'galaxy-lib==17.9.9'
    htcondor = 'htcondor>=8.6.0'
    dill = 'dill==0.2.7.1'
    six = 'six>=1.10.0'
    future = 'future'
    requests = 'requests==2.18.4'
    docker = 'docker==2.5.1'
    subprocess32 = 'subprocess32<=3.5.2'
    dateutil = 'python-dateutil'
    pytest = 'pytest==3.7.4'
    pytest_cov = 'pytest-cov==2.5.1'
    addict = 'addict<=2.2.0'
    sphinx = 'sphinx==1.7.5'
    pathlib2 = 'pathlib2==2.3.2'

    core_reqs = [
        dill,
        six,
        future,
        requests,
        docker,
        dateutil,
        psutil,
        subprocess32,
        pytest,
        pytest_cov,
        addict,
        sphinx,
        pathlib2]

    mesos_reqs = [
        pymesos,
        psutil]
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

    # htcondor is not supported by apple
    # this is tricky to conditionally support in 'all' due
    # to how wheels work, so it is not included in all and
    # must be explicitly installed as an extra
    all_reqs = \
        mesos_reqs + \
        aws_reqs + \
        azure_reqs + \
        encryption_reqs + \
        google_reqs + \
        cwl_reqs

    # remove the subprocess32 backport if not python2
    if not sys.version_info[0] == 2:
        core_reqs.remove(subprocess32)

    setup(
        name='toil',
        version=version.distVersion,
        description='Pipeline management software for clusters.',
        author='Benedict Paten',
        author_email='benedict@soe.usc.edu',
        url="https://github.com/BD2KGenomics/toil",
        classifiers=[
          'Development Status :: 5 - Production/Stable',
          'Environment :: Console',
          'Intended Audience :: Developers',
          'Intended Audience :: Science/Research',
          'Intended Audience :: Healthcare Industry',
          'License :: OSI Approved :: Apache Software License',
          'Natural Language :: English',
          'Operating System :: MacOS :: MacOS X',
          'Operating System :: POSIX',
          'Operating System :: POSIX :: Linux',
          'Programming Language :: Python :: 2.7',
          'Topic :: Scientific/Engineering',
          'Topic :: Scientific/Engineering :: Bio-Informatics',
          'Topic :: Scientific/Engineering :: Astronomy',
          'Topic :: Scientific/Engineering :: Atmospheric Science',
          'Topic :: Scientific/Engineering :: Information Analysis',
          'Topic :: Scientific/Engineering :: Medical Science Apps.',
          'Topic :: System :: Distributed Computing',
          'Topic :: Utilities'],
        license="Apache License v2.0",
        cmdclass=wrap_commands(), # Report to Toil Central Command when Toil is installed
        install_requires=core_reqs,
        extras_require={
            'mesos': mesos_reqs,
            'aws': aws_reqs,
            'azure': azure_reqs,
            'encryption': encryption_reqs,
            'google': google_reqs,
            'cwl': cwl_reqs,
            'wdl': wdl_reqs,
            'htcondor:sys_platform!="darwin"': htcondor_reqs,
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
            with NamedTemporaryFile(mode='w',dir='src/toil', prefix='version.py.', delete=False) as f:
                f.write(new)
            os.rename(f.name, 'src/toil/version.py')
    # Unfortunately, we can't use a straight import here because that would also load the stuff
    # defined in src/toil/__init__.py which imports modules from external dependencies that may
    # yet to be installed when setup.py is invoked.
    return imp.load_source('toil.version', 'src/toil/version.py')


version = importVersion()
runSetup()
