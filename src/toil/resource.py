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
from __future__ import absolute_import
from collections import namedtuple
from contextlib import closing
import hashlib
import importlib
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
import shutil

log = logging.getLogger(__name__)


class Resource(namedtuple('Resource', ('name', 'pathHash', 'url', 'contentHash'))):
    """
    Represents a file or directory that will be deployed to each node before any jobs in the user
    script are invoked. Each instance is a namedtuple with the following elements:

    The pathHash element contains the MD5 (in hexdigest form) of the path to the resource on the
    leader node. The path, and therefore its hash is unique within a job store.

    The url element is a "file:" or "http:" URL at which the resource can be obtained.

    The contentHash element is an MD5 checksum of the resource, allowing for validation and
    caching of resources.

    If the resource is a regular file, the type attribute will be 'file'.

    If the resource is a directory, the type attribute will be 'dir' and the URL will point at a
    ZIP archive of that directory.
    """

    rootDirPathEnvName = 'JTRES_ROOT'

    resourceEnvNamePrefix = 'JTRES_'

    @classmethod
    def create(cls, jobStore, leaderPath):
        """
        Saves the content of the file or directory at the given path to the given job store
        and returns a resource object representing that content for the purpose of obtaining it
        again at a generic, public URL. This method should be invoked on the leader node.

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
            with jobStore.writeSharedFileStream(pathHash, isProtected=False) as dst:
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
        Prepares this system for the downloading and lookup of resources. This method should only
        be invoked on a worker node. It is idempotent but not thread-safe.
        """
        try:
            resourceRootDirPath = os.environ[cls.rootDirPathEnvName]
        except KeyError:
            # Create directory holding local copies of requested resources ...
            resourceRootDirPath = mkdtemp()
            # .. and register its location in an environment variable such that child processes can find it
            os.environ[cls.rootDirPathEnvName] = resourceRootDirPath
        assert os.path.isdir(resourceRootDirPath)

    @classmethod
    def cleanSystem(cls):
        """
        Removes all downloaded, localized resources
        """
        resourceRootDirPath = os.environ[cls.rootDirPathEnvName]
        shutil.rmtree(resourceRootDirPath)
        for k, v in os.environ.items():
            if k.startswith(cls.resourceEnvNamePrefix):
                os.unsetenv(k)

    def register(self):
        """
        Register this resource for later retrieval via lookup(), possibly in a child process.
        """
        os.environ[self.resourceEnvNamePrefix + self.pathHash] = self._pickle()

    @classmethod
    def lookup(cls, leaderPath):
        """
        Returns a resource object representing a resource created from a file or directory at the
        given path on the leader. This method should be invoked on the worker. The given path
        does not need to refer to an existing file or directory on the worker, it only identifies
        the resource within an instance of toil. This method returns None if no resource for the
        given path exists.

        :rtype: Resource
        """
        pathHash = cls._pathHash(leaderPath)
        try:
            s = os.environ[cls.resourceEnvNamePrefix + pathHash]
        except KeyError:
            log.warn("Can't find resource for leader path '%s'", leaderPath)
            return None
        else:
            self = cls._unpickle(s)
            assert self.pathHash == pathHash
            return self

    def download(self, callback=None):
        """
        Downloads this resource from its URL to a file on the local system. This method should
        only be invoked on a worker node after the node was setup for accessing resources via
        prepareSystem().
        """
        dirPath = self.localDirPath
        if not os.path.exists(dirPath):
            tempDirPath = mkdtemp(dir=os.path.dirname(dirPath), prefix=self.contentHash + "-")
            self._save(tempDirPath)
            if callback is not None:
                callback(tempDirPath)
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
        The path to resource on the worker. The file or directory at the given resource may not
        exist. Invoking download() will ensure that it does.
        """
        raise NotImplementedError

    @property
    def localDirPath(self):
        """
        The path to the directory containing the resource on the worker.
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
        Returns a readable file-like object for the given path. If the path refers to a regular
        file, this method returns the result of invoking open() on the given path. If the path
        refers to a directory, this method returns a ZIP file with all files and subdirectories
        in the directory at the given path.

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
    A resource read from a directory on the leader. The URL will point to a ZIP archive of the
    directory.
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

    - dirPath, the path to the directory that should be added to sys.path before importing the
      module,

    - moduleName, the fully qualified name of the module with leading package names separated by
      dot and

    - extension, the file extension of the file containing the module, including the leading period.

    >>> import toil.resource
    >>> ModuleDescriptor.forModule('toil.resource') # doctest: +ELLIPSIS
    ModuleDescriptor(dirPath='/.../src', name='toil.resource', extension='.py')

    Note that the above test only succeeds on py.test. To run with doctest or an IDE you may have
    to change the assertion to .pyc.

    >>> import subprocess, tempfile, os
    >>> dirPath = tempfile.mkdtemp()
    >>> path = os.path.join( dirPath, 'foo.py' )
    >>> with open(path,'w') as f:
    ...     f.write('from toil.resource import ModuleDescriptor\\n'
    ...             'print ModuleDescriptor.forModule(__name__)')
    >>> expected = [ str( ModuleDescriptor(dirPath=dirPath, name='foo', extension=extension) )
    ...     for extension in ('.py', '.pyc') ]
    >>> subprocess.check_output([ sys.executable, path ]).strip() in expected
    True

    Now test a collision. As funny as it sounds, the robotparser module is included in the Python
    standard library.
    >>> dirPath = tempfile.mkdtemp()
    >>> path = os.path.join( dirPath, 'robotparser.py' )
    >>> with open(path,'w') as f:
    ...     f.write('from toil.resource import ModuleDescriptor\\n'
    ...             'ModuleDescriptor.forModule(__name__)')

    This should fail and return exit status 1 due to the collision with the built-in 'test' module:
    >>> subprocess.call([ sys.executable, path ])
    1

    Clean up
    >>> from shutil import rmtree
    >>> rmtree( dirPath )
    """

    @classmethod
    def forModule(cls, name):
        """
        Return an instance of this class representing the module of the given name. If the given
        module name is "__main__", it will be translated to the actual file name of the top-level
        script without the .py or .pyc extension. This method assumes that the module with the
        specified name has already been loaded.
        """
        module = sys.modules[name]
        filePath = os.path.abspath(module.__file__)
        filePath = filePath.split(os.path.sep)
        filePath[-1], extension = os.path.splitext(filePath[-1])
        assert extension in ('.py', '.pyc')
        if name == '__main__':
            name = filePath.pop()
            dirPath = os.path.sep.join(filePath)
            cls._check_conflict(dirPath, name)
        else:
            for package in reversed(name.split('.')):
                dirPathTail = filePath.pop()
                assert dirPathTail == package
            dirPath = os.path.sep.join(filePath)

        return cls(dirPath=dirPath, name=name, extension=extension)

    @classmethod
    def _check_conflict(cls, dirPath, name):
        """
        Check whether the module of the given name conflicts with another module on the sys.path.

        :param dirPath: the directory from which the module was originally loaded
        :param name: the mpdule name
        """
        old_sys_path = sys.path
        try:
            sys.path = [dir for dir in old_sys_path
                        if os.path.realpath(dir) != os.path.realpath(dirPath)]
            try:
                colliding_module = importlib.import_module(name)
            except ImportError:
                pass
            else:
                raise RuntimeError(
                    "The user module '%s' collides with module '%s from '%s'." % (
                        name, colliding_module.__name__, colliding_module.__file__))
        finally:
            sys.path = old_sys_path

    @property
    def belongsToToil(self):
        """
        True if this module is part of the Toil distribution
        """
        return self.name.startswith('toil.')

    @property
    def filePath(self):
        """
        The full path to the file containing this module
        """
        return os.path.join(self.dirPath, *self.name.split('.')) + self.extension

    def saveAsResourceTo(self, jobStore):
        """
        Store the file containing this module--or even the Python package directory hierarchy
        containing that file--as a resource to the given job store and return the
        corresponding resource object. Should only be called on a leader node.

        :type jobStore: toil.jobStores.abstractJobStore.AbstractJobStore
        :rtype: toil.resource.Resource
        """
        return Resource.create(jobStore, self._resourcePath)

    def localize(self):
        """
        Check if this module was saved as a resource. If it was, return a new module descriptor
        that points to a local copy of that resource. Should only be called on a worker node.

        :rtype: toil.resource.Resource
        """
        resource = Resource.lookup(self._resourcePath)
        if resource is None:
            log.warn("Can't localize module %r", self)
            return self
        else:
            def stash(tmpDirPath):
                # Save the original dirPath such that we can restore it in globalize()
                with open(os.path.join(tmpDirPath, '.original'), 'w') as f:
                    f.write(json.dumps(self))

            resource.download(callback=stash)
            return self.__class__(dirPath=resource.localDirPath, name=self.name,
                                  extension=self.extension)

    def globalize(self):
        try:
            with open(os.path.join(self.dirPath, '.original')) as f:
                return self.__class__(*json.loads(f.read()))
        except IOError as e:
            if e.errno == errno.ENOENT:
                log.warn("Can't globalize module %r.", self)
                return self
            else:
                raise

    @property
    def _resourcePath(self):
        """
        The path to the file or package directory that should be used when shipping this module
        around as a resource.
        """
        return self.dirPath if '.' in self.name else self.filePath
