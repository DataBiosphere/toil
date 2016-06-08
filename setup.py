# Copyright (C) 2015 UCSC Computational Genomics Lab
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

from version import version
from setuptools import find_packages, setup

botoRequirement = 'boto==2.38.0'

setup(
    name='toil',
    version=version,
    description='Pipeline management software for clusters.',
    author='Benedict Paten',
    author_email='benedict@soe.usc.edu',
    url="https://github.com/BD2KGenomics/toil",
    install_requires=[
        'bd2k-python-lib==1.13.dev14',
        'dill==0.2.5'],
    tests_require=[
        'mock==1.0.1',
        'pytest==2.8.3'],
    test_suite='toil',
    extras_require={
        'mesos': [
            'psutil==3.0.1'],
        'aws': [
            botoRequirement,
            'cgcloud-lib==1.4a1.dev195' ],
        'azure': [
            'azure==1.0.3'],
        'encryption': [
            'pynacl==0.3.0'],
        'google': [
            'gcs_oauth2_boto_plugin==1.9',
            botoRequirement],
        'cwl': [
            'cwltool==1.0.20160425140546']},
    package_dir={'': 'src'},
    packages=find_packages('src', exclude=['*.test']),
    entry_points={
        'console_scripts': [
            'toil = toil.utils.toilMain:main',
            '_toil_worker = toil.worker:main',
            'cwltoil = toil.cwl.cwltoil:main [cwl]',
            'cwl-runner = toil.cwl.cwltoil:main [cwl]',
            '_toil_mesos_executor = toil.batchSystems.mesos.executor:main [mesos]']})
