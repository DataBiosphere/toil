# Copyright (C) 2015-2018 Regents of the University of California
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

from __future__ import absolute_import

import importlib
import os
import sys
from inspect import getsource
from io import BytesIO
from textwrap import dedent
from zipfile import ZipFile

from toil.test import mkdir_p
from mock import MagicMock, patch

import subprocess
from toil import inVirtualEnv
from toil.resource import ModuleDescriptor, Resource, ResourceException
from toil.test import ToilTest, tempFileContaining, travis_test
from toil.version import exactPython


class ResourceTest(ToilTest):
    """
    Test module descriptors and resources derived from them.
    """

    @travis_test
    def testStandAlone(self):
        self._testExternal(moduleName='userScript', pyFiles=('userScript.py',
                                                             'helper.py'))

    @travis_test
    def testPackage(self):
        self._testExternal(moduleName='foo.userScript', pyFiles=('foo/__init__.py',
                                                                 'foo/userScript.py',
                                                                 'foo/bar/__init__.py',
                                                                 'foo/bar/helper.py'))

    @travis_test
    def testVirtualEnv(self):
        self._testExternal(moduleName='foo.userScript',
                           virtualenv=True,
                           pyFiles=('foo/__init__.py',
                                    'foo/userScript.py',
                                    'foo/bar/__init__.py',
                                    'foo/bar/helper.py',
                                    'de/pen/dency.py',
                                    'de/__init__.py',
                                    'de/pen/__init__.py'))

    @travis_test
    def testStandAloneInPackage(self):
        self.assertRaises(ResourceException,
                          self._testExternal,
                          moduleName='userScript',
                          pyFiles=('__init__.py', 'userScript.py', 'helper.py'))

    def _testExternal(self, moduleName, pyFiles, virtualenv=False):
        dirPath = self._createTempDir()
        if virtualenv:
            self.assertTrue(inVirtualEnv())
            # --never-download prevents silent upgrades to pip, wheel and setuptools
            subprocess.check_call(['virtualenv', '--never-download', '--python', exactPython, dirPath])
            sitePackages = os.path.join(dirPath, 'lib',
                                        exactPython,
                                        'site-packages')
            # tuple assignment is necessary to make this line immediately precede the try:
            oldPrefix, sys.prefix, dirPath = sys.prefix, dirPath, sitePackages
        else:
            oldPrefix = None
        try:
            for relPath in pyFiles:
                path = os.path.join(dirPath, relPath)
                mkdir_p(os.path.dirname(path))
                with open(path, 'w') as f:
                    f.write('pass\n')
            sys.path.append(dirPath)
            try:
                userScript = importlib.import_module(moduleName)
                try:
                    self._test(userScript.__name__,
                               expectedContents=pyFiles,
                               allowExtraContents=True)
                finally:
                    del userScript
                    while moduleName:
                        del sys.modules[moduleName]
                        self.assertFalse(moduleName in sys.modules)
                        moduleName = '.'.join(moduleName.split('.')[:-1])

            finally:
                sys.path.remove(dirPath)
        finally:
            if oldPrefix:
                sys.prefix = oldPrefix

    @travis_test
    def testBuiltIn(self):
        # Create a ModuleDescriptor for the module containing ModuleDescriptor, i.e. toil.resource
        module_name = ModuleDescriptor.__module__
        self.assertEqual(module_name, 'toil.resource')
        self._test(module_name, shouldBelongToToil=True)

    def _test(self, module_name,
              shouldBelongToToil=False, expectedContents=None, allowExtraContents=True):
        module = ModuleDescriptor.forModule(module_name)
        # Assert basic attributes and properties
        self.assertEqual(module.belongsToToil, shouldBelongToToil)
        self.assertEqual(module.name, module_name)
        if shouldBelongToToil:
            self.assertTrue(module.dirPath.endswith('/src'))

        # Before the module is saved as a resource, localize() and globalize() are identity
        # methods. This should log.warnings.
        self.assertIs(module.localize(), module)
        self.assertIs(module.globalize(), module)
        # Create a mock job store ...
        jobStore = MagicMock()
        # ... to generate a fake URL for the resource ...
        url = 'file://foo.zip'
        jobStore.getSharedPublicUrl.return_value = url
        # ... and save the resource to it.
        resource = module.saveAsResourceTo(jobStore)
        # Ensure that the URL generation method is actually called, ...
        jobStore.getSharedPublicUrl.assert_called_once_with(sharedFileName=resource.pathHash)
        # ... and that ensure that writeSharedFileStream is called.
        jobStore.writeSharedFileStream.assert_called_once_with(sharedFileName=resource.pathHash,
                                                               isProtected=False)
        # Now it gets a bit complicated: Ensure that the context manager returned by the
        # jobStore's writeSharedFileStream() method is entered and that the file handle yielded
        # by the context manager is written to once with the zipped source tree from which
        # 'toil.resource' was orginally imported. Keep the zipped tree around such that we can
        # mock the download later.
        file_handle = jobStore.writeSharedFileStream.return_value.__enter__.return_value
        # The first 0 index selects the first call of write(), the second 0 selects positional
        # instead of keyword arguments, and the third 0 selects the first positional, i.e. the
        # contents. This is a bit brittle since it assumes that all the data is written in a
        # single call to write(). If more calls are made we can easily concatenate them.
        zipFile = file_handle.write.call_args_list[0][0][0]
        self.assertTrue(zipFile.startswith(b'PK'))  # the magic header for ZIP files

        # Check contents if requested
        if expectedContents is not None:
            with ZipFile(BytesIO(zipFile)) as _zipFile:
                actualContents = set(_zipFile.namelist())
                if allowExtraContents:
                    self.assertTrue(actualContents.issuperset(expectedContents))
                else:
                    self.assertEqual(actualContents, expectedContents)

        self.assertEqual(resource.url, url)
        # Now we're on the worker. Prepare the storage for localized resources
        Resource.prepareSystem()
        try:
            # Register the resource for subsequent lookup.
            resource.register()
            # Lookup the resource and ensure that the result is equal to but not the same as the
            # original resource. Lookup will also be used when we localize the module that was
            # originally used to create the resource.
            localResource = Resource.lookup(module._resourcePath)
            self.assertEqual(resource, localResource)
            self.assertIsNot(resource, localResource)
            # Now show that we can localize the module using the registered resource. Set up a mock
            # urlopen() that yields the zipped tree ...
            mock_urlopen = MagicMock()
            mock_urlopen.return_value.read.return_value = zipFile
            with patch('toil.resource.urlopen', mock_urlopen):
                # ... and use it to download and unpack the resource
                localModule = module.localize()
            # The name should be equal between original and localized resource ...
            self.assertEqual(module.name, localModule.name)
            # ... but the directory should be different.
            self.assertNotEqual(module.dirPath, localModule.dirPath)
            # Show that we can 'undo' localization. This is necessary when the user script's jobs
            #  are invoked on the worker where they generate more child jobs.
            self.assertEqual(localModule.globalize(), module)
        finally:
            Resource.cleanSystem()

    @travis_test
    def testNonPyStandAlone(self):
        """
        Asserts that Toil enforces the user script to have a .py or .pyc extension because that's
        the only way auto-deployment can re-import the module on a worker. See

        https://github.com/BD2KGenomics/toil/issues/631 and
        https://github.com/BD2KGenomics/toil/issues/858
        """

        def script():
            import argparse
            from toil.job import Job
            from toil.common import Toil

            def fn():
                pass

            if __name__ == '__main__':
                parser = argparse.ArgumentParser()
                Job.Runner.addToilOptions(parser)
                options = parser.parse_args()
                job = Job.wrapFn(fn, memory='10M', cores=0.1, disk='10M')
                with Toil(options) as toil:
                    toil.start(job)

        scriptBody = dedent('\n'.join(getsource(script).split('\n')[1:]))
        shebang = '#! %s\n' % sys.executable
        with tempFileContaining(shebang + scriptBody) as scriptPath:
            self.assertFalse(scriptPath.endswith(('.py', '.pyc')))
            os.chmod(scriptPath, 0o755)
            jobStorePath = scriptPath + '.jobStore'
            process = subprocess.Popen([scriptPath, jobStorePath], stderr=subprocess.PIPE)
            stdout, stderr = process.communicate()
            self.assertTrue('The name of a user script/module must end in .py or .pyc.' in stderr.decode('utf-8'))
            self.assertNotEqual(0, process.returncode)
            self.assertFalse(os.path.exists(jobStorePath))
