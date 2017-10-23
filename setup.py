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

botoRequirement = 'boto==2.38.0'

versionFileName = 'version.py'
versionFilePath = 'src/toil/'

def readVersionKeyValues():
    """
    Read and return key values from the version file.
    """
    versionVars = {}
    with open(versionFilePath+versionFileName) as versionFile:
        for line in versionFile:
            name, var = line.partition("=")[::2]
            versionVars[name.strip()] = var.strip()
    return versionVars

def runSetup():
    """
    Calls setup(). This function exists so the setup() invocation preceded more internal
    functionality. The distribution version is generated dynamically with checkVersionFile.
    """
    versionDict = readVersionKeyValues()
    setup(
        name='toil',
        version=versionDict['distVersion'],
        description='Pipeline management software for clusters.',
        author='Benedict Paten',
        author_email='benedict@soe.usc.edu',
        url="https://github.com/BD2KGenomics/toil",
        classifiers=["License :: OSI Approved :: Apache Software License"],
        license="Apache License v2.0",
        install_requires=[
            'dill==0.2.5',
            'six>=1.10.0',
            'future',
            'docker==2.5.1'],
        extras_require={
            'mesos': [
                'psutil==3.0.1'],
            'aws': [
                botoRequirement,
                'futures==3.0.5',
                'pycrypto==2.6.1'],
            'azure': [
                'azure==1.0.3'],
            'encryption': [
                'pynacl==1.1.2'],
            'google': [
                'gcs_oauth2_boto_plugin==1.9',
                botoRequirement],
            'cwl': [
                'cwltool==1.0.20170822192924',
                'schema-salad >= 2.6, < 3',
                'cwltest>=1.0.20170214185319']},
        package_dir={'': 'src'},
        packages=find_packages(where='src',
                               # Note that we intentionally include the top-level `test` package for
                               # functionality like the @experimental and @integrative decoratorss:
                               exclude=['*.test.*']),
        # Unfortunately, the names of the entry points are hard-coded elsewhere in the code base so
        # you can't just change them here. Luckily, most of them are pretty unique strings, and thus
        # easy to search for.
        entry_points={
            'console_scripts': [
                'toil = toil.utils.toilMain:main',
                '_toil_worker = toil.worker:main',
                'cwltoil = toil.cwl.cwltoil:main [cwl]',
                'toil-cwl-runner = toil.cwl.cwltoil:main [cwl]',
                'cwl-runner = toil.cwl.cwltoil:main [cwl]',
                '_toil_mesos_executor = toil.batchSystems.mesos.executor:main [mesos]']})


def checkVersionFile():
    """
    Check version.py, generating it from the template if required.
    """
    try:
        # Attempt to load the template first. It only exists in a working copy cloned via git.
        import version_template
    except ImportError:
        # If loading the template fails we must be in a unpacked source distribution and
        # version.py will already exist.
        pass
    else:
        # Use the template to generate version.py
        import os
        import errno
        from tempfile import NamedTemporaryFile

        new = version_template.expand_()
        versionFullPath = versionFilePath + versionFileName
        try:
            with open(versionFullPath) as f:
                old = f.read()
        except IOError as e:
            if e.errno == errno.ENOENT:
                old = None
            else:
                raise

        if old != new:
            with NamedTemporaryFile(dir=versionFilePath, prefix=versionFileName, delete=False) as f:
                f.write(new)
            os.rename(f.name, versionFullPath)



checkVersionFile()
runSetup()
