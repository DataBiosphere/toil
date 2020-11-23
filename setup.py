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
import os


def runSetup():
    """
    Calls setup(). This function exists so the setup() invocation preceded more internal
    functionality. The `version` module is imported dynamically by importVersion() below.
    """
    boto = 'boto==2.48.0'
    boto3 = 'boto3>=1.7.50, <2.0'
    futures = 'futures==3.1.1'
    pycryptodome = 'pycryptodome==3.5.1'
    pymesos = 'pymesos==0.3.15'
    psutil = 'psutil >= 3.0.1, <6'
    pynacl = 'pynacl==1.3.0'
    gcs = 'google-cloud-storage==1.6.0'
    gcs_oauth2_boto_plugin = 'gcs_oauth2_boto_plugin==1.14'
    apacheLibcloud = 'apache-libcloud==2.2.1'
    cwltool = 'cwltool==3.0.20201121085451'
    galaxyToolUtil = 'galaxy-tool-util'
    htcondor = 'htcondor>=8.6.0'
    kubernetes = 'kubernetes>=10, <11'
    idna = 'idna>=2'
    pytz = 'pytz>=2012'
    dill = 'dill>=0.3.2, <0.4'
    six = 'six>=1.10.0'
    future = 'future'
    requests = 'requests>=2, <3'
    docker = 'docker==4.3.1'
    dateutil = 'python-dateutil'
    addict = 'addict>=2.2.1, <2.3'
    enlighten = 'enlighten>=1.5.2, <2'

    core_reqs = [
        dill,
        six,
        future,
        requests,
        docker,
        dateutil,
        psutil,
        addict,
        pytz,
        enlighten]

    aws_reqs = [
        boto,
        boto3,
        futures,
        pycryptodome]
    cwl_reqs = [
        cwltool,
        galaxyToolUtil]
    encryption_reqs = [
        pynacl]
    google_reqs = [
        gcs_oauth2_boto_plugin,  # is this being used??
        apacheLibcloud,
        gcs]
    htcondor_reqs = [
        htcondor]
    kubernetes_reqs = [
        kubernetes,
        idna] # Kubernetes's urllib3 can mange to use idna without really depending on it.
    mesos_reqs = [
        pymesos,
        psutil]
    wdl_reqs = []
    

    # htcondor is not supported by apple
    # this is tricky to conditionally support in 'all' due
    # to how wheels work, so it is not included in all and
    # must be explicitly installed as an extra
    all_reqs = \
        aws_reqs + \
        cwl_reqs + \
        encryption_reqs + \
        google_reqs + \
        kubernetes_reqs + \
        mesos_reqs


    setup(
        name='toil',
        version=version.distVersion,
        description='Pipeline management software for clusters.',
        author='Benedict Paten',
        author_email='benedict@soe.usc.edu',
        url="https://github.com/DataBiosphere/toil",
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
          'Programming Language :: Python :: 3.6',
          'Topic :: Scientific/Engineering',
          'Topic :: Scientific/Engineering :: Bio-Informatics',
          'Topic :: Scientific/Engineering :: Astronomy',
          'Topic :: Scientific/Engineering :: Atmospheric Science',
          'Topic :: Scientific/Engineering :: Information Analysis',
          'Topic :: Scientific/Engineering :: Medical Science Apps.',
          'Topic :: System :: Distributed Computing',
          'Topic :: Utilities'],
        license="Apache License v2.0",
        python_requires=">=3.6",
        install_requires=core_reqs,
        extras_require={
            'aws': aws_reqs,
            'cwl': cwl_reqs,
            'encryption': encryption_reqs,
            'google': google_reqs,
            'htcondor:sys_platform!="darwin"': htcondor_reqs,
            'kubernetes': kubernetes_reqs,
            'mesos': mesos_reqs,
            'wdl': wdl_reqs,
            'all': all_reqs},
        package_dir={'': 'src'},
        packages=find_packages(where='src',
                               # Note that we intentionally include the top-level `test` package for
                               # functionality like the @experimental and @integrative decorators:
                               exclude=['*.test.*']),
        package_data = {
            '': ['*.yml', 'cloud-config'],
        },
        # Unfortunately, the names of the entry points are hard-coded elsewhere in the code base so
        # you can't just change them here. Luckily, most of them are pretty unique strings, and thus
        # easy to search for.
        entry_points={
            'console_scripts': [
                'toil = toil.utils.toilMain:main',
                '_toil_worker = toil.worker:main',
                'cwltoil = toil.cwl.cwltoil:cwltoil_was_removed [cwl]',
                'toil-cwl-runner = toil.cwl.cwltoil:main [cwl]',
                'toil-wdl-runner = toil.wdl.toilwdl:main',
                '_toil_mesos_executor = toil.batchSystems.mesos.executor:main [mesos]',
                '_toil_kubernetes_executor = toil.batchSystems.kubernetes:executor [kubernetes]']})


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
            with NamedTemporaryFile(mode='w', dir='src/toil', prefix='version.py.', delete=False) as f:
                f.write(new)
            os.rename(f.name, 'src/toil/version.py')
    # Unfortunately, we can't use a straight import here because that would also load the stuff
    # defined in src/toil/__init__.py which imports modules from external dependencies that may
    # yet to be installed when setup.py is invoked.
    return imp.load_source('toil.version', 'src/toil/version.py')


version = importVersion()
runSetup()
