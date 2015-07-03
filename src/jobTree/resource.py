from collections import namedtuple
from contextlib import closing
import hashlib
from io import BytesIO
import json
import logging
import os
from pydoc import locate
from tempfile import mkdtemp
from urllib2 import urlopen
import errno
from zipfile import ZipFile
import sys

log = logging.getLogger(__name__)


class Resource(namedtuple('Resource', ('name', 'pathHash', 'url', 'contentHash'))):
    """
    Represents a file or directory that will be deployed to each node before any targets in the user script are
    invoked. Each instance is a namedtuple with the following elements:

    The pathHash element contains the MD5 (in hexdigest form) of the path to the resource on the leader node. The
    path, and therefore its hash is unique within a job store.

    The url element is a "file:" or "http:" URL at which the resource can be obtained.

    The contentHash element is an MD5 checksum of the resource, allowing for validation and caching of resources.

    If the resource is a regular file, the type attribute will be 'file'.

    If the resource is a directory, the type attribute will be 'dir' and the URL will point at a ZIP archive of
    that directory.
    """

    rootDirPathEnvName = 'JTRES_ROOT'

    resourceEnvNamePrefix = 'JTRES_'

    @classmethod
    def create(cls, jobStore, leaderPath):
        """
        Saves the content of the file or directory at the given path to the given job store and returns a resource
        object representing that content for the purpose of obtaining it again at a generic, public URL. This method
        should be invoked on the leader node.

        :rtype: Resource
        """
        if os.path.isdir(leaderPath):
            subcls = DirectoryResource
        elif os.path.isfile(leaderPath):
            subcls = FileResource
        elif os.path.exists(leaderPath):
            raise AssertionError("Neither a file or a directory: '%s'" % leaderPath)
        else:
            raise AssertionError("No such file or directory: '%s'" % leaderPath)

        pathHash = subcls._pathHash(leaderPath)
        contentHash = hashlib.md5()
        # noinspection PyProtectedMember
        with subcls._load(leaderPath) as src:
            with jobStore.writeSharedFileStream(pathHash) as dst:
                userScript = src.read()
                contentHash.update(userScript)
                dst.write(userScript)
        return subcls(name=os.path.basename(leaderPath),
                      pathHash=pathHash,
                      url=(jobStore.getSharedPublicUrl(pathHash)),
                      contentHash=contentHash.hexdigest())

    @classmethod
    def prepareSystem(cls):
        """
        Prepares this system for the downloading and lookup of resources. This method should only be invoked on a
        worker node. It is idempotent but not thread-safe.
        """
        try:
            resourceRootDirPath = os.environ[cls.rootDirPathEnvName]
        except KeyError:
            # Create directory holding local copies of requested resources ...
            resourceRootDirPath = mkdtemp()
            # .. and register its location in an environment variable such that child processes can find it
            os.environ[cls.rootDirPathEnvName] = resourceRootDirPath
        assert os.path.isdir(resourceRootDirPath)

    def register(self):
        """
        Register this resource for later retrieval via lookup(), possibly in a child process.
        """
        os.environ[self.resourceEnvNamePrefix + self.pathHash] = self._pickle()

    @classmethod
    def lookup(cls, leaderPath):
        """
        Returns a resource object representing a resource created from a file or directory at the given path on the
        leader. This method should be invoked on the worker. The given path does not need to refer to an existing
        file or directory on the worker, it only identifies the resource within an instance of jobTree. This method
        returns None if no resource for the given path exists.

        :rtype: Resource
        """
        pathHash = cls._pathHash(leaderPath)
        try:
            s = os.environ[cls.resourceEnvNamePrefix + pathHash]
        except KeyError:
            return None
        else:
            self = cls._unpickle(s)
            assert self.pathHash == pathHash
            return self

    def download(self):
        """
        Downloads this resource from its URL to a file on the local system. This method should only be invoked on a
        worker node after the node was setup for accessing resources via prepareSystem().
        """
        dirPath = self.localDirPath
        if not os.path.exists(dirPath):
            tempDirPath = mkdtemp(dir=os.path.dirname(dirPath), prefix=self.contentHash + "-")
            self._save(tempDirPath)
            try:
                os.rename(tempDirPath, dirPath)
            except OSError as e:
                if e.errno == errno.ENOTEMPTY:
                    # Another process beat us to it.
                    # TODO: This is correct but inefficient since multiple processes download the resource redundantly
                    pass
                else:
                    raise

    @property
    def localPath(self):
        """
        The path to resource on the worker. The file or directory at the given resource may not exist. Invoking
        download() will ensure that it does.
        """
        raise NotImplementedError

    @property
    def localDirPath(self):
        """
        The path to the directory containing the resource on the worker. For directory resources this is the same as
        the localPath property.
        """
        rootDirPath = os.environ[self.rootDirPathEnvName]
        return os.path.join(rootDirPath, self.contentHash)

    def _pickle(self):
        return self.__class__.__module__ + "." + self.__class__.__name__ + ':' + json.dumps(self)

    @classmethod
    def _unpickle(cls, s):
        """
        :rtype: Resource
        """
        className, _json = s.split(':', 1)
        return locate(className)(*json.loads(_json))

    @classmethod
    def _pathHash(cls, path):
        return hashlib.md5(path).hexdigest()

    @classmethod
    def _load(cls, path):
        """
        Returns a readable file-like object for the given path. If the path refers to a regular file, this method
        returns the result of invoking open() on the given path. If the path refers to a directory, this method
        returns a ZIP file with all files and subdirectories in the directory at the given path.

        :type path: str
        :rtype: io.IOBase
        """
        raise NotImplementedError()

    def _save(self, dirPath):
        """
        Save this resource to disk at the given parent path.

        :type dirPath: str
        """
        raise NotImplementedError()

    def _download(self, dstFile):
        """
        Download this resource from its URL to the given file object.

        :type dstFile: io.BytesIO|io.FileIO
        """
        with closing(urlopen(self.url)) as content:
            buf = content.read()
        contentHash = hashlib.md5(buf)
        assert contentHash.hexdigest() == self.contentHash
        dstFile.write(buf)


class FileResource(Resource):
    """
    A resource read from a file on the leader.
    """

    @classmethod
    def _load(cls, path):
        return open(path)

    def _save(self, dirPath):
        with open(os.path.join(dirPath, self.name), mode='w') as localFile:
            self._download(localFile)

    @property
    def localPath(self):
        return os.path.join(self.localDirPath, self.name)


class DirectoryResource(Resource):
    """
    A resource read from a directory on the leader. The URL will point to a ZIP archive of the directory.
    """

    @classmethod
    def _load(cls, leaderPath):
        bytesIO = BytesIO()
        with ZipFile(file=bytesIO, mode='w') as zipFile:
            for dirPath, fileNames, dirNames in os.walk(leaderPath):
                assert dirPath.startswith(leaderPath)
                for fileName in fileNames:
                    filePath = os.path.join(dirPath, fileName)
                    assert filePath.encode('ascii') == filePath
                    relativeFilePath = os.path.relpath(filePath, leaderPath)
                    assert not relativeFilePath.startswith(os.path.sep)
                    zipFile.write(filePath, relativeFilePath)
        bytesIO.seek(0)
        return bytesIO

    def _save(self, dirPath):
        bytesIO = BytesIO()
        self._download(bytesIO)
        bytesIO.seek(0)
        with ZipFile(file=bytesIO, mode='r') as zipFile:
            zipFile.extractall(path=dirPath)

    @property
    def localPath(self):
        return self.localDirPath


class ModuleDescriptor(namedtuple('ModuleDescriptor', ('dirPath', 'name', 'extension'))):
    """
    A path to a Python module decomposed into a namedtuple of three elements, namely

    - dirPath, the path to the directory that should be added to sys.path before importing the module,

    - moduleName, the fully qualified name of the module with leading package names separated by dot and

    - extension, the file extension of the file containing the module, including the leading period.

    >>> import jobTree.resource
    >>> ModuleDescriptor.forModule(jobTree.resource.__name__) # doctest: +ELLIPSIS
    ModuleDescriptor(dirPath='/.../src', name='jobTree.resource', extension='.pyc')
    >>> import subprocess, tempfile, os
    >>> fh,path = tempfile.mkstemp(prefix='foo', suffix='.py')
    >>> with os.fdopen(fh,'w') as f:
    ...     f.write('from jobTree.resource import ModuleDescriptor; print ModuleDescriptor.forModule(__name__)')
    >>> subprocess.check_output([ 'python', path ]) # doctest: +ELLIPSIS
    "ModuleDescriptor(dirPath='/...', name='foo...', extension='.py')\\n"
    """

    @classmethod
    def forModule(cls, moduleName):
        """
        Return an instance of this class representing the module of the given name. If the given module name is
        "__main__", it will be translated to the actual file name of the top-level script without the .py or .pyc
        extension. This method expects that the module with the specified name has already been loaded.
        """
        module = sys.modules[moduleName]
        moduleFilePath = os.path.abspath(module.__file__)
        dirPath = moduleFilePath.split(os.path.sep)
        dirPath[-1], extension = os.path.splitext(dirPath[-1])
        assert extension in ('.py', '.pyc')
        if moduleName == '__main__':
            moduleName = dirPath.pop()
        else:
            for package in reversed(moduleName.split('.')):
                dirPathTail = dirPath.pop()
                assert dirPathTail == package
        return cls(dirPath=os.path.sep.join(dirPath), name=moduleName, extension=extension)

    @classmethod
    def forDirPath(cls, userModuleDirPath, userModuleName):
        pass

    @property
    def belongsToJobTree(self):
        """
        True if this module is part of the jobTree distribution
        """
        # FIXME: Forcing this to False for now to give this code more exposure but once we hot-deploy jobTree itself
        # FIXME: ... it should be used to shortcut hot-deployment for user scripts inside the jobTree distribution.
        return False and self.name.startswith('jobTree.')

    @property
    def filePath(self):
        """
        The full path to the file containing this module
        """
        return os.path.join(self.dirPath, *self.name.split('.')) + self.extension

    def saveAsResourceTo(self, jobStore):
        """
        Store the file containing this module--or even the Python package directory hierarchy containing that
        file--as a resource to the given job store and return the corresponding resource object. Should only be
        called on a leader node.

        :type jobStore: jobTree.jobStores.abstractJobStore.AbstractJobStore
        :rtype: jobTree.resource.Resource
        """
        return Resource.create(jobStore, self._resourcePath)

    def localize(self):
        """
        Check if this module was saved as a resource. If it was, return a new module descriptor that points to a
        local copy of that resource. Should only be called on a worker node.

        :rtype: jobTree.resource.Resource
        """
        resource = Resource.lookup(self._resourcePath)
        if resource is None:
            return self
        else:
            resource.download()
            return self.__class__(dirPath=resource.localDirPath, name=self.name, extension=self.extension)

    @property
    def _resourcePath(self):
        """
        The path to the file or package directory that should be used when shipping this module around as a resource.
        """
        return self.dirPath if '.' in self.name else self.filePath

