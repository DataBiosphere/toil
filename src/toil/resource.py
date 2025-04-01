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
import errno
import hashlib
import importlib
import json
import logging
import os
import shutil
import sys
from collections import namedtuple
from collections.abc import Sequence
from contextlib import closing
from io import BytesIO
from pydoc import locate
from types import ModuleType
from typing import IO, TYPE_CHECKING, BinaryIO, Callable, Optional
from urllib.error import HTTPError
from urllib.request import urlopen
from zipfile import ZipFile

from toil import inVirtualEnv
from toil.lib.io import mkdtemp
from toil.lib.memoize import strict_bool
from toil.lib.retry import ErrorCondition, retry
from toil.version import exactPython

if TYPE_CHECKING:
    from toil.jobStores.abstractJobStore import AbstractJobStore

logger = logging.getLogger(__name__)


class Resource(namedtuple("Resource", ("name", "pathHash", "url", "contentHash"))):
    """
    Represents a file or directory that will be deployed to each node before any jobs in the user script are invoked.

    Each instance is a namedtuple with the following elements:

    The pathHash element contains the MD5 (in hexdigest form) of the path to the resource on the
    leader node. The path, and therefore its hash is unique within a job store.

    The url element is a "file:" or "http:" URL at which the resource can be obtained.

    The contentHash element is an MD5 checksum of the resource, allowing for validation and
    caching of resources.

    If the resource is a regular file, the type attribute will be 'file'.

    If the resource is a directory, the type attribute will be 'dir' and the URL will point at a
    ZIP archive of that directory.
    """

    resourceEnvNamePrefix = "JTRES_"

    rootDirPathEnvName = resourceEnvNamePrefix + "ROOT"

    @classmethod
    def create(cls, jobStore: "AbstractJobStore", leaderPath: str) -> "Resource":
        """
        Saves the content of the file or directory at the given path to the given job store
        and returns a resource object representing that content for the purpose of obtaining it
        again at a generic, public URL. This method should be invoked on the leader node.

        :param toil.jobStores.abstractJobStore.AbstractJobStore jobStore:

        :param str leaderPath:
        """
        pathHash = cls._pathHash(leaderPath)
        contentHash = hashlib.md5()
        # noinspection PyProtectedMember
        with cls._load(leaderPath) as src:
            with jobStore.write_shared_file_stream(
                shared_file_name=pathHash, encrypted=False
            ) as dst:
                userScript = src.read()
                contentHash.update(userScript)
                dst.write(userScript)
        return cls(
            name=os.path.basename(leaderPath),
            pathHash=pathHash,
            url=jobStore.getSharedPublicUrl(sharedFileName=pathHash),
            contentHash=contentHash.hexdigest(),
        )

    def refresh(self, jobStore: "AbstractJobStore") -> "Resource":
        return type(self)(
            name=self.name,
            pathHash=self.pathHash,
            url=jobStore.get_shared_public_url(shared_file_name=self.pathHash),
            contentHash=self.contentHash,
        )

    @classmethod
    def prepareSystem(cls) -> None:
        """
        Prepares this system for the downloading and lookup of resources. This method should only
        be invoked on a worker node. It is idempotent but not thread-safe.
        """
        try:
            resourceRootDirPath = os.environ[cls.rootDirPathEnvName]
        except KeyError:
            # Create directory holding local copies of requested resources ...
            resourceRootDirPath = mkdtemp()
            # .. and register its location in an environment variable such that child processes
            # can find it.
            os.environ[cls.rootDirPathEnvName] = resourceRootDirPath
        if not os.path.isdir(resourceRootDirPath):
            raise RuntimeError("Resource root path is not a directory.")

    @classmethod
    def cleanSystem(cls) -> None:
        """Remove all downloaded, localized resources."""
        resourceRootDirPath = os.environ[cls.rootDirPathEnvName]
        os.environ.pop(cls.rootDirPathEnvName)
        shutil.rmtree(resourceRootDirPath)
        for k, v in list(os.environ.items()):
            if k.startswith(cls.resourceEnvNamePrefix):
                os.environ.pop(k)

    def register(self) -> None:
        """Register this resource for later retrieval via lookup(), possibly in a child process."""
        os.environ[self.resourceEnvNamePrefix + self.pathHash] = self.pickle()

    @classmethod
    def lookup(cls, leaderPath: str) -> Optional["Resource"]:
        """
        Return a resource object representing a resource created from a file or directory at the given path on the leader.

        This method should be invoked on the worker. The given path does not need
        to refer to an existing file or directory on the worker, it only identifies
        the resource within an instance of toil. This method returns None if no resource
        for the given path exists.
        """
        pathHash = cls._pathHash(leaderPath)
        try:
            path_key = cls.resourceEnvNamePrefix + pathHash
            s = os.environ[path_key]
        except KeyError:
            # Resources that don't actually exist but get looked up are normal; don't complain.
            return None
        else:
            self = cls.unpickle(s)
            if self.pathHash != pathHash:
                raise RuntimeError("The Resource's path is incorrect.")

            return self

    def download(self, callback: Optional[Callable[[str], None]] = None) -> None:
        """
        Download this resource from its URL to a file on the local system.

        This method should only be invoked on a worker node after the node was setup
        for accessing resources via prepareSystem().
        """
        dirPath = self.localDirPath
        if not os.path.exists(dirPath):
            tempDirPath = mkdtemp(
                dir=os.path.dirname(dirPath), prefix=self.contentHash + "-"
            )
            self._save(tempDirPath)
            if callback is not None:
                callback(tempDirPath)
            try:
                os.rename(tempDirPath, dirPath)
            except OSError as e:
                # If dirPath already exists & is non-empty either ENOTEMPTY or EEXIST will be raised
                if e.errno == errno.ENOTEMPTY or e.errno == errno.EEXIST:
                    # Another process beat us to it.
                    # TODO: This is correct but inefficient since multiple processes download the resource redundantly
                    pass
                else:
                    raise

    @property
    def localPath(self) -> str:
        """
        Get the path to resource on the worker.

        The file or directory at the returned path may or may not yet exist.
        Invoking download() will ensure that it does.
        """
        raise NotImplementedError

    @property
    def localDirPath(self) -> str:
        """
        The path to the directory containing the resource on the worker.
        """
        rootDirPath = os.environ[self.rootDirPathEnvName]
        return os.path.join(rootDirPath, self.contentHash)

    def pickle(self) -> str:
        return (
            self.__class__.__module__
            + "."
            + self.__class__.__name__
            + ":"
            + json.dumps(self)
        )

    @classmethod
    def unpickle(cls, s: str) -> "Resource":
        className, _json = s.split(":", 1)
        return locate(className)(*json.loads(_json))  # type: ignore

    @classmethod
    def _pathHash(cls, path: str) -> str:
        return hashlib.md5(path.encode("utf-8")).hexdigest()

    @classmethod
    def _load(cls, path: str) -> IO[bytes]:
        """
        Returns a readable file-like object for the given path. If the path refers to a regular
        file, this method returns the result of invoking open() on the given path. If the path
        refers to a directory, this method returns a ZIP file with all files and subdirectories
        in the directory at the given path.

        :type path: str
        """
        raise NotImplementedError()

    def _save(self, dirPath: str) -> None:
        """
        Save this resource to the directory at the given parent path.

        :type dirPath: str
        """
        raise NotImplementedError()

    @retry(errors=[ErrorCondition(error=HTTPError, error_codes=[400])])
    def _download(self, dstFile: IO[bytes]) -> None:
        """
        Download this resource from its URL to the given file object.

        :type dstFile: io.BytesIO|io.FileIO
        """
        with closing(urlopen(self.url)) as content:
            buf = content.read()
        contentHash = hashlib.md5(buf)
        if contentHash.hexdigest() != self.contentHash:
            raise RuntimeError("The Resource's contents is incorrect.")
        dstFile.write(buf)


class FileResource(Resource):
    """A resource read from a file on the leader."""

    @classmethod
    def _load(cls, path: str) -> BinaryIO:
        return open(path, "rb")

    def _save(self, dirPath: str) -> None:
        with open(os.path.join(dirPath, self.name), mode="wb") as localFile:
            self._download(localFile)

    @property
    def localPath(self) -> str:
        return os.path.join(self.localDirPath, self.name)


class DirectoryResource(Resource):
    """
    A resource read from a directory on the leader.

    The URL will point to a ZIP archive of the directory. All files in that directory
    (and any subdirectories) will be included. The directory
    may be a package but it does not need to be.
    """

    @classmethod
    def _load(cls, path: str) -> BytesIO:
        bytesIO = BytesIO()
        initfile = os.path.join(path, "__init__.py")
        if os.path.isfile(initfile):
            # This is a package directory. To emulate
            # PyZipFile.writepy's behavior, we need to keep everything
            # relative to this path's parent directory.
            rootDir = os.path.dirname(path)
        else:
            # This is a simple user script (with possibly a few helper files)
            rootDir = path
        skipdirList = [
            "/tmp",
            "/var",
            "/etc",
            "/bin",
            "/sbin",
            "/home",
            "/dev",
            "/sys",
            "/usr",
            "/run",
        ]
        if path not in skipdirList:
            with ZipFile(file=bytesIO, mode="w") as zipFile:
                for dirName, _, fileList in os.walk(path):
                    for fileName in fileList:
                        try:
                            fullPath = os.path.join(dirName, fileName)
                            zipFile.write(fullPath, os.path.relpath(fullPath, rootDir))
                        except OSError:
                            logger.critical(
                                "Cannot access and read the file at path: %s" % fullPath
                            )
                            sys.exit(1)
        else:
            logger.critical(
                "Couldn't package the directory at {} for hot deployment. Would recommend to create a \
                subdirectory (ie {}/MYDIR_HERE/)".format(
                    path, path
                )
            )
            sys.exit(1)
        bytesIO.seek(0)
        return bytesIO

    def _save(self, dirPath: str) -> None:
        bytesIO = BytesIO()
        self._download(bytesIO)
        bytesIO.seek(0)
        with ZipFile(file=bytesIO, mode="r") as zipFile:
            zipFile.extractall(path=dirPath)

    @property
    def localPath(self) -> str:
        return self.localDirPath


class VirtualEnvResource(DirectoryResource):
    """
    A resource read from a virtualenv on the leader.

    All modules and packages found in the virtualenv's site-packages directory will be included.
    """

    @classmethod
    def _load(cls, path: str) -> BytesIO:
        if os.path.basename(path) != "site-packages":
            raise RuntimeError("An incorrect path was passed through.")
        bytesIO = BytesIO()
        with ZipFile(file=bytesIO, mode="w") as zipFile:
            for dirName, _, fileList in os.walk(path):
                zipFile.write(dirName)
                for fileName in fileList:
                    fullPath = os.path.join(dirName, fileName)
                    zipFile.write(fullPath, os.path.relpath(fullPath, path))
        bytesIO.seek(0)
        return bytesIO


class ModuleDescriptor(
    namedtuple("ModuleDescriptor", ("dirPath", "name", "fromVirtualEnv"))
):
    """
    A path to a Python module decomposed into a namedtuple of three elements

    - dirPath, the path to the directory that should be added to sys.path before importing the
      module,

    - moduleName, the fully qualified name of the module with leading package names separated by
      dot and

    >>> import toil.resource
    >>> ModuleDescriptor.forModule('toil.resource') # doctest: +ELLIPSIS
    ModuleDescriptor(dirPath='/.../src', name='toil.resource', fromVirtualEnv=False)

    >>> import subprocess, tempfile, os
    >>> dirPath = tempfile.mkdtemp()
    >>> path = os.path.join( dirPath, 'foo.py' )
    >>> with open(path,'w') as f:
    ...     _ = f.write('from toil.resource import ModuleDescriptor\\n'
    ...                 'print(ModuleDescriptor.forModule(__name__))')
    >>> subprocess.check_output([ sys.executable, path ]) # doctest: +ELLIPSIS +ALLOW_BYTES
    b"ModuleDescriptor(dirPath='...', name='foo', fromVirtualEnv=False)\\n"

    >>> from shutil import rmtree
    >>> rmtree( dirPath )

    Now test a collision. 'collections' is part of the standard library in Python 2 and 3.
    >>> dirPath = tempfile.mkdtemp()
    >>> path = os.path.join( dirPath, 'collections.py' )
    >>> with open(path,'w') as f:
    ...     _ = f.write('from toil.resource import ModuleDescriptor\\n'
    ...                 'ModuleDescriptor.forModule(__name__)')

    This should fail and return exit status 1 due to the collision with the built-in module:
    >>> subprocess.call([ sys.executable, path ])
    1

    Clean up
    >>> rmtree( dirPath )
    """

    dirPath: str
    name: str

    @classmethod
    def forModule(cls, name: str) -> "ModuleDescriptor":
        """
        Return an instance of this class representing the module of the given name.

        If the given module name is "__main__", it will be translated to the actual
        file name of the top-level script without the .py or .pyc extension. This
        method assumes that the module with the specified name has already been loaded.
        """
        module = sys.modules[name]
        if module.__file__ is None:
            raise Exception(f"Module {name} does not exist.")
        fileAbsPath = os.path.abspath(module.__file__)
        filePath = fileAbsPath.split(os.path.sep)
        filePath[-1], extension = os.path.splitext(filePath[-1])
        if extension not in (".py", ".pyc"):
            raise Exception("The name of a user script/module must end in .py or .pyc.")
        if name == "__main__":
            logger.debug("Discovering real name of module")
            # User script/module was invoked as the main program
            if module.__package__:
                # Invoked as a module via python -m foo.bar
                logger.debug("Script was invoked as a module")
                nameList = [filePath.pop()]
                for package in reversed(module.__package__.split(".")):
                    dirPathTail = filePath.pop()
                    if dirPathTail != package:
                        raise RuntimeError("Incorrect path to package.")
                    nameList.append(dirPathTail)
                name = ".".join(reversed(nameList))
                dirPath = os.path.sep.join(filePath)
            else:
                # Invoked as a script via python foo/bar.py
                name = filePath.pop()
                dirPath = os.path.sep.join(filePath)
                cls._check_conflict(dirPath, name)
        else:
            # User module was imported. Determine the directory containing the top-level package
            if filePath[-1] == "__init__":
                # module is a subpackage
                filePath.pop()

            for package in reversed(name.split(".")):
                dirPathTail = filePath.pop()
                if dirPathTail != package:
                    raise RuntimeError("Incorrect path to package.")
            dirPath = os.path.abspath(os.path.sep.join(filePath))
        absPrefix = os.path.abspath(sys.prefix)
        inVenv = inVirtualEnv()
        logger.debug(
            "Module dir is %s, our prefix is %s, virtualenv: %s",
            dirPath,
            absPrefix,
            inVenv,
        )
        if not os.path.isdir(dirPath):
            raise Exception(
                f"Bad directory path {dirPath} for module {name}. Note that hot-deployment does not support .egg-link files yet, or scripts located in the root directory."
            )
        fromVirtualEnv = inVenv and dirPath.startswith(absPrefix)
        return cls(dirPath=dirPath, name=name, fromVirtualEnv=fromVirtualEnv)

    @classmethod
    def _check_conflict(cls, dirPath: str, name: str) -> None:
        """
        Check whether the module of the given name conflicts with another module on the sys.path.

        :param dirPath: the directory from which the module was originally loaded
        :param name: the mpdule name
        """
        old_sys_path = sys.path
        try:
            sys.path = [
                d
                for d in old_sys_path
                if os.path.realpath(d) != os.path.realpath(dirPath)
            ]
            try:
                colliding_module = importlib.import_module(name)
            except ImportError:
                pass
            else:
                raise ResourceException(
                    "The user module '{}' collides with module '{} from '{}'.".format(
                        name, colliding_module.__name__, colliding_module.__file__
                    )
                )
        finally:
            sys.path = old_sys_path

    @property
    def belongsToToil(self) -> bool:
        """
        True if this module is part of the Toil distribution
        """
        return self.name.startswith("toil.")

    def saveAsResourceTo(self, jobStore: "AbstractJobStore") -> Resource:
        """
        Store the file containing this module--or even the Python package directory hierarchy
        containing that file--as a resource to the given job store and return the
        corresponding resource object. Should only be called on a leader node.

        :type jobStore: toil.jobStores.abstractJobStore.AbstractJobStore
        """
        return self._getResourceClass().create(jobStore, self._resourcePath)

    def _getResourceClass(self) -> type[Resource]:
        """
        Return the concrete subclass of Resource that's appropriate for auto-deploying this module.
        """
        subcls: type[Resource]
        if self.fromVirtualEnv:
            subcls = VirtualEnvResource
        elif os.path.isdir(self._resourcePath):
            subcls = DirectoryResource
        elif os.path.isfile(self._resourcePath):
            subcls = FileResource
        elif os.path.exists(self._resourcePath):
            raise AssertionError(
                "Neither a file or a directory: '%s'" % self._resourcePath
            )
        else:
            raise AssertionError("No such file or directory: '%s'" % self._resourcePath)
        return subcls

    def localize(self) -> "ModuleDescriptor":
        """
        Check if this module was saved as a resource.

        If it was, return a new module descriptor that points to a local copy of
        that resource. Should only be called on a worker node. On
        the leader, this method returns this resource, i.e. self.
        """
        if not self._runningOnWorker():
            logger.warning("The localize() method should only be invoked on a worker.")
        resource = Resource.lookup(self._resourcePath)
        if resource is None:
            return self
        else:

            def stash(tmpDirPath: str) -> None:
                # Save the original dirPath such that we can restore it in globalize()
                with open(os.path.join(tmpDirPath, ".stash"), "w") as f:
                    f.write("1" if self.fromVirtualEnv else "0")
                    f.write(self.dirPath)

            resource.download(callback=stash)
            return self.__class__(
                dirPath=resource.localDirPath,
                name=self.name,
                fromVirtualEnv=self.fromVirtualEnv,
            )

    def _runningOnWorker(self) -> bool:
        try:
            mainModule = sys.modules["__main__"]
        except KeyError:
            logger.warning("Cannot determine main program module.")
            return False
        else:
            # If __file__ is not a valid attribute, it's because
            # toil is being run interactively, in which case
            # we can reasonably assume that we are not running
            # on a worker node.
            try:
                if mainModule.__file__ is None:
                    return False
                mainModuleFile = os.path.basename(mainModule.__file__)
            except AttributeError:
                return False

            workerModuleFiles = [
                "worker.py",
                "worker.pyc",
                "worker.pyo",
                "_toil_worker",
            ]  # setuptools entry point
            return mainModuleFile in workerModuleFiles

    def globalize(self) -> "ModuleDescriptor":
        """
        Reverse the effect of localize().
        """
        try:
            with open(os.path.join(self.dirPath, ".stash")) as f:
                fromVirtualEnv = [False, True][int(f.read(1))]
                dirPath = f.read()
        except OSError as e:
            if e.errno == errno.ENOENT:
                return self
            else:
                raise
        else:
            return self.__class__(
                dirPath=dirPath, name=self.name, fromVirtualEnv=fromVirtualEnv
            )

    @property
    def _resourcePath(self) -> str:
        """
        The path to the directory that should be used when shipping this module and its siblings
        around as a resource.
        """
        if self.fromVirtualEnv:
            return self.dirPath
        elif "." in self.name:
            return os.path.join(self.dirPath, self._rootPackage())
        else:
            initName = self._initModuleName(self.dirPath)
            if initName:
                raise ResourceException(
                    f"Toil does not support loading a user script from a package directory. You "
                    f"may want to remove {initName} from {self.dirPath} or invoke the user script as a module via: "
                    f"PYTHONPATH='{self.dirPath}' {exactPython} -m {self.dirPath}.{self.name}"
                )
            return self.dirPath

    @classmethod
    def _initModuleName(cls, dirPath: str) -> Optional[str]:
        for name in ("__init__.py", "__init__.pyc", "__init__.pyo"):
            if os.path.exists(os.path.join(dirPath, name)):
                return name
        return None

    def _rootPackage(self) -> str:
        try:
            head, tail = self.name.split(".", 1)
        except ValueError:
            raise ValueError("%r is stand-alone module." % self.__repr__())
        else:
            return head

    def toCommand(self) -> Sequence[str]:
        return tuple(map(str, self))

    @classmethod
    def fromCommand(cls, command: Sequence[str]) -> "ModuleDescriptor":
        if len(command) != 3:
            raise RuntimeError("Incorrect number of arguments (Expected 3).")
        return cls(
            dirPath=command[0], name=command[1], fromVirtualEnv=strict_bool(command[2])
        )

    def makeLoadable(self) -> "ModuleDescriptor":
        module = self if self.belongsToToil else self.localize()
        if module.dirPath not in sys.path:
            sys.path.append(module.dirPath)
        return module

    def load(self) -> Optional[ModuleType]:
        module = self.makeLoadable()
        try:
            return importlib.import_module(module.name)
        except ImportError:
            logger.error(
                "Failed to import user module %r from sys.path (%r).", module, sys.path
            )
            raise


class ResourceException(Exception):
    pass
