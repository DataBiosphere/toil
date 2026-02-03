# Copyright (C) 2015-2021 Regents of the University of California
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
import importlib
import os
import subprocess
import sys
from collections.abc import Iterable
from inspect import getsource
from io import BytesIO
from pathlib import Path
from textwrap import dedent
from unittest.mock import MagicMock, patch
from zipfile import ZipFile

import pytest

from toil import inVirtualEnv
from toil.resource import ModuleDescriptor, Resource, ResourceException
from toil.version import exactPython


def tempFileContaining(directory: Path, content: str, suffix: str = "") -> str:
    """
    Write a file with the given contents, and keep it on disk as long as the context is active.
    :param str content: The contents of the file.
    :param str suffix: The extension to use for the temporary file.
    """
    with (directory / "temp").open("wb") as fd:
        encoded = content.encode("utf-8")
        assert fd.write(encoded) == len(encoded)
    return str(directory / "temp")


class TestResource:
    """Test module descriptors and resources derived from them."""

    def testStandAlone(self, tmp_path: Path) -> None:
        self._testExternal(
            tmp_path, moduleName="userScript", pyFiles=("userScript.py", "helper.py")
        )

    def testPackage(self, tmp_path: Path) -> None:
        self._testExternal(
            tmp_path,
            moduleName="foo.userScript",
            pyFiles=(
                "foo/__init__.py",
                "foo/userScript.py",
                "foo/bar/__init__.py",
                "foo/bar/helper.py",
            ),
        )

    def testVirtualEnv(self, tmp_path: Path) -> None:
        self._testExternal(
            tmp_path,
            moduleName="foo.userScript",
            virtualenv=True,
            pyFiles=(
                "foo/__init__.py",
                "foo/userScript.py",
                "foo/bar/__init__.py",
                "foo/bar/helper.py",
                "de/pen/dency.py",
                "de/__init__.py",
                "de/pen/__init__.py",
            ),
        )

    def testStandAloneInPackage(self, tmp_path: Path) -> None:
        with pytest.raises(ResourceException):
            self._testExternal(
                tmp_path,
                moduleName="userScript",
                pyFiles=("__init__.py", "userScript.py", "helper.py"),
            )

    def _testExternal(
        self,
        dirPath: Path,
        moduleName: str,
        pyFiles: Iterable[str],
        virtualenv: bool = False,
    ) -> None:
        if virtualenv:
            assert inVirtualEnv()
            # --never-download prevents silent upgrades to pip, wheel and setuptools
            subprocess.check_call(
                [
                    "virtualenv",
                    "--never-download",
                    "--python",
                    exactPython,
                    str(dirPath),
                ]
            )
            sitePackages = dirPath / "lib" / exactPython / "site-packages"
            # tuple assignment is necessary to make this line immediately precede the try:
            oldPrefix, sys.prefix, dirPath = sys.prefix, str(dirPath), sitePackages
        else:
            oldPrefix = None
        try:
            for relPath in pyFiles:
                path = dirPath / relPath
                path.parent.mkdir(parents=True, exist_ok=True)
                with path.open("w") as f:
                    f.write("pass\n")
            sys.path.append(str(dirPath))
            try:
                userScript = importlib.import_module(moduleName)
                try:
                    self._test(
                        userScript.__name__,
                        expectedContents=pyFiles,
                        allowExtraContents=True,
                    )
                finally:
                    del userScript
                    while moduleName:
                        del sys.modules[moduleName]
                        assert moduleName not in sys.modules
                        moduleName = ".".join(moduleName.split(".")[:-1])

            finally:
                sys.path.remove(str(dirPath))
        finally:
            if oldPrefix:
                sys.prefix = oldPrefix

    def testBuiltIn(self) -> None:
        # Create a ModuleDescriptor for the module containing ModuleDescriptor, i.e. toil.resource
        module_name = ModuleDescriptor.__module__
        assert module_name == "toil.resource"
        self._test(module_name, shouldBelongToToil=True)

    def _test(
        self,
        module_name: str,
        shouldBelongToToil: bool = False,
        expectedContents: Iterable[str] | None = None,
        allowExtraContents: bool = True,
    ) -> None:
        module = ModuleDescriptor.forModule(module_name)
        # Assert basic attributes and properties
        assert module.belongsToToil == shouldBelongToToil
        assert module.name == module_name
        if shouldBelongToToil:
            assert module.dirPath.endswith("/src")

        # Before the module is saved as a resource, localize() and globalize() are identity
        # methods. This should log.warnings.
        assert module.localize() is module
        assert module.globalize() is module
        # Create a mock job store ...
        jobStore = MagicMock()
        # ... to generate a fake URL for the resource ...
        url = "file://foo.zip"
        jobStore.getSharedPublicUrl.return_value = url
        # ... and save the resource to it.
        resource = module.saveAsResourceTo(jobStore)
        # Ensure that the URL generation method is actually called, ...
        jobStore.getSharedPublicUrl.assert_called_once_with(
            sharedFileName=resource.pathHash
        )
        # ... and that ensure that write_shared_file_stream is called.
        jobStore.write_shared_file_stream.assert_called_once_with(
            shared_file_name=resource.pathHash, encrypted=False
        )
        # Now it gets a bit complicated: Ensure that the context manager returned by the
        # jobStore's write_shared_file_stream() method is entered and that the file handle yielded
        # by the context manager is written to once with the zipped source tree from which
        # 'toil.resource' was originally imported. Keep the zipped tree around such that we can
        # mock the download later.
        file_handle = (
            jobStore.write_shared_file_stream.return_value.__enter__.return_value
        )
        # The first 0 index selects the first call of write(), the second 0 selects positional
        # instead of keyword arguments, and the third 0 selects the first positional, i.e. the
        # contents. This is a bit brittle since it assumes that all the data is written in a
        # single call to write(). If more calls are made we can easily concatenate them.
        zipFile = file_handle.write.call_args_list[0][0][0]
        assert zipFile.startswith(b"PK")  # the magic header for ZIP files

        # Check contents if requested
        if expectedContents is not None:
            with ZipFile(BytesIO(zipFile)) as _zipFile:
                actualContents = set(_zipFile.namelist())
                if allowExtraContents:
                    assert actualContents.issuperset(expectedContents)
                else:
                    assert actualContents == expectedContents

        assert resource.url == url
        # Now we're on the worker. Prepare the storage for localized resources
        Resource.prepareSystem()
        try:
            # Register the resource for subsequent lookup.
            resource.register()
            # Lookup the resource and ensure that the result is equal to but not the same as the
            # original resource. Lookup will also be used when we localize the module that was
            # originally used to create the resource.
            localResource = Resource.lookup(module._resourcePath)
            assert resource == localResource
            assert resource is not localResource
            # Now show that we can localize the module using the registered resource. Set up a mock
            # urlopen() that yields the zipped tree ...
            mock_urlopen = MagicMock()
            mock_urlopen.return_value.read.return_value = zipFile
            with patch("toil.resource.urlopen", mock_urlopen):
                # ... and use it to download and unpack the resource
                localModule = module.localize()
            # The name should be equal between original and localized resource ...
            assert module.name == localModule.name
            # ... but the directory should be different.
            assert module.dirPath != localModule.dirPath
            # Show that we can 'undo' localization. This is necessary when the user script's jobs
            #  are invoked on the worker where they generate more child jobs.
            assert localModule.globalize() == module
        finally:
            Resource.cleanSystem()

    def testNonPyStandAlone(self, tmp_path: Path) -> None:
        """
        Asserts that Toil enforces the user script to have a .py or .pyc extension because that's
        the only way auto-deployment can re-import the module on a worker. See

        https://github.com/BD2KGenomics/toil/issues/631 and
        https://github.com/BD2KGenomics/toil/issues/858
        """

        def script() -> None:
            from configargparse import ArgumentParser

            from toil.common import Toil
            from toil.job import Job

            def fn() -> None:
                pass

            if __name__ == "__main__":
                parser = ArgumentParser()
                Job.Runner.addToilOptions(parser)
                options = parser.parse_args()
                job = Job.wrapFn(fn, memory="10M", cores=0.1, disk="10M")
                with Toil(options) as toil:
                    toil.start(job)

        scriptBody = dedent("\n".join(getsource(script).split("\n")[1:]))
        shebang = "#! %s\n" % sys.executable
        scriptPath = tempFileContaining(tmp_path, shebang + scriptBody)
        assert not scriptPath.endswith((".py", ".pyc"))
        os.chmod(scriptPath, 0o755)
        jobStorePath = scriptPath + ".jobStore"
        process = subprocess.Popen([scriptPath, jobStorePath], stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()
        assert (
            "The name of a user script/module must end in .py or .pyc."
            in stderr.decode("utf-8")
        )
        assert 0 != process.returncode
        assert not os.path.exists(jobStorePath)
