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

from __future__ import absolute_import

import errno
import hashlib
import importlib
import json
import logging
import os
import shutil
import sys
from collections import namedtuple
from contextlib import closing
from io import BytesIO
from pydoc import locate
from tempfile import mkdtemp
from urllib2 import HTTPError
from zipfile import ZipFile, PyZipFile

# Python 3 compatibility imports
from bd2k.util.retry import retry
from six.moves.urllib.request import urlopen

from bd2k.util import strict_bool
from bd2k.util.iterables import concat
from bd2k.util.exceptions import require

from toil import inVirtualEnv

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

    resourceEnvNamePrefix = 'JTRES_'

    rootDirPathEnvName = resourceEnvNamePrefix + 'ROOT'

    @classmethod
    def create(cls, jobStore, leaderPath):
        """
        Saves the content of the file or directory at the given path to the given job store
        and returns a resource object representing that content for the purpose of obtaining it
        again at a generic, public URL. This method should be invoked on the leader node.

        :param toil.jobStores.abstractJobStore.AbstractJobStore jobStore:

        :param str leaderPath:

        :rtype: Resource
        """
        pathHash = cls._pathHash(leaderPath)
        contentHash = hashlib.md5()
        # noinspection PyProtectedMember
        with cls._load(leaderPath) as src:
            with jobStore.writeSharedFileStream(sharedFileName=pathHash, isProtected=False) as dst:
                userScript = src.read()
                contentHash.update(userScript)
                dst.write(userScript)
        return cls(name=os.path.basename(leaderPath),
                   pathHash=pathHash,
                   url=jobStore.getSharedPublicUrl(sharedFileName=pathHash),
                   contentHash=contentHash.hexdigest())

    def refresh(self, jobStore):
        return type(self)(name=self.name,
                          pathHash=self.pathHash,
                          url=jobStore.getSharedPublicUrl(sharedFileName=self.pathHash),
                          contentHash=self.contentHash)

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
            # .. and register its location in an environment variable such that child processes
            # can find it.
            os.environ[cls.rootDirPathEnvName] = resourceRootDirPath
        assert os.path.isdir(resourceRootDirPath)

    @classmethod
    def cleanSystem(cls):
        """
        Removes all downloaded, localized resources
        """
        resourceRootDirPath = os.environ[cls.rootDirPathEnvName]
        os.environ.pop(cls.rootDirPathEnvName)
        shutil.rmtree(resourceRootDirPath)
        for k, v in os.environ.items():
            if k.startswith(cls.resourceEnvNamePrefix):
                os.environ.pop(k)

    def register(self):
        """
        Register this resource for later retrieval via lookup(), possibly in a child process.
        """
        os.environ[self.resourceEnvNamePrefix + self.pathHash] = self.pickle()

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
            self = cls.unpickle(s)
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
                # If dirPath already exists & is non-empty either ENOTEMPTY or EEXIST will be raised
                if e.errno == errno.ENOTEMPTY or e.errno == errno.EEXIST:
                    # Another process beat us to it.
                    # TODO: This is correct but inefficient since multiple processes download the resource redundantly
                    pass
                else:
                    raise

    @property
    def localPath(self):
        """
        The path to resource on the worker. The file or directory at the returned path may or may
        not yet exist. Invoking download() will ensure that it does.
        """
        raise NotImplementedError

    @property
    def localDirPath(self):
        """
        The path to the directory containing the resource on the worker.
        """
        rootDirPath = os.environ[self.rootDirPathEnvName]
        return os.path.join(rootDirPath, self.contentHash)

    def pickle(self):
        return self.__class__.__module__ + "." + self.__class__.__name__ + ':' + json.dumps(self)

    @classmethod
    def unpickle(cls, s):
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
        :rtype: io.FileIO
        """
        raise NotImplementedError()

    def _save(self, dirPath):
        """
        Save this resource to the directory at the given parent path.

        :type dirPath: str
        """
        raise NotImplementedError()

    def _download(self, dstFile):
        """
        Download this resource from its URL to the given file object.

        :type dstFile: io.BytesIO|io.FileIO
        """
        for attempt in retry(predicate=lambda e: isinstance(e, HTTPError) and e.code == 400):
            with attempt:
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
    directory. Only Python script/modules will be included. The directory may be a package but it
    does not need to be.
    """

    @classmethod
    def _load(cls, path):
        """
        :type path: str
        """
        bytesIO = BytesIO()
        # PyZipFile compiles .py files on the fly, filters out any non-Python files and
        # distinguishes between packages and simple directories.
        with PyZipFile(file=bytesIO, mode='w') as zipFile:
            zipFile.writepy(path)
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


class VirtualEnvResource(DirectoryResource):
    """
    A resource read from a virtualenv on the leader. All modules and packages found in the
    virtualenv's site-packages directory will be included. Any .pth or .egg-link files will be
    ignored.
    """

    @classmethod
    def _load(cls, path):
        sitePackages = path
        assert os.path.basename(sitePackages) == 'site-packages'
        bytesIO = BytesIO()
        with PyZipFile(file=bytesIO, mode='w') as zipFile:
            # This adds the .py files but omits subdirectories since site-packages is not a package
            zipFile.writepy(sitePackages)
            # Now add the missing packages
            for name in os.listdir(sitePackages):
                path = os.path.join(sitePackages, name)
                if os.path.isdir(path) and os.path.isfile(os.path.join(path, '__init__.py')):
                    zipFile.writepy(path)
        bytesIO.seek(0)
        return bytesIO


class ModuleDescriptor(namedtuple('ModuleDescriptor', ('dirPath', 'name', 'fromVirtualEnv'))):
    """
    A path to a Python module decomposed into a namedtuple of three elements, namely

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
    ...     f.write('from toil.resource import ModuleDescriptor\\n'
    ...             'print ModuleDescriptor.forModule(__name__)')
    >>> subprocess.check_output([ sys.executable, path ]) # doctest: +ELLIPSIS
    "ModuleDescriptor(dirPath='...', name='foo', fromVirtualEnv=False)\\n"

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
        require(extension in ('.py', '.pyc'),
                'The name of a user script/module must end in .py or .pyc.')
        if name == '__main__':
            log.debug("Discovering real name of module")
            # User script/module was invoked as the main program
            if module.__package__:
                # Invoked as a module via python -m foo.bar
                log.debug("Script was invoked as a module")
                name = [filePath.pop()]
                for package in reversed(module.__package__.split('.')):
                    dirPathTail = filePath.pop()
                    assert dirPathTail == package
                    name.append(dirPathTail)
                name = '.'.join(reversed(name))
                dirPath = os.path.sep.join(filePath)
            else:
                # Invoked as a script via python foo/bar.py
                name = filePath.pop()
                dirPath = os.path.sep.join(filePath)
                cls._check_conflict(dirPath, name)
        else:
            # User module was imported. Determine the directory containing the top-level package
            if filePath[-1] == '__init__':
                # module is a subpackage
                filePath.pop()

            for package in reversed(name.split('.')):
                dirPathTail = filePath.pop()
                assert dirPathTail == package
            dirPath = os.path.sep.join(filePath)
        log.debug("Module dir is %s", dirPath)
        require(os.path.isdir(dirPath),
                'Bad directory path %s for module %s. Note that hot-deployment does not support \
                .egg-link files yet, or scripts located in the root directory.', dirPath, name)
        fromVirtualEnv = inVirtualEnv() and dirPath.startswith(sys.prefix)
        return cls(dirPath=dirPath, name=name, fromVirtualEnv=fromVirtualEnv)

    @classmethod
    def _check_conflict(cls, dirPath, name):
        """
        Check whether the module of the given name conflicts with another module on the sys.path.

        :param dirPath: the directory from which the module was originally loaded
        :param name: the mpdule name
        """
        old_sys_path = sys.path
        try:
            sys.path = [d for d in old_sys_path if os.path.realpath(d) != os.path.realpath(dirPath)]
            try:
                colliding_module = importlib.import_module(name)
            except ImportError:
                pass
            else:
                raise ResourceException(
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

    def saveAsResourceTo(self, jobStore):
        """
        Store the file containing this module--or even the Python package directory hierarchy
        containing that file--as a resource to the given job store and return the
        corresponding resource object. Should only be called on a leader node.

        :type jobStore: toil.jobStores.abstractJobStore.AbstractJobStore
        :rtype: toil.resource.Resource
        """
        return self._getResourceClass().create(jobStore, self._resourcePath)

    def _getResourceClass(self):
        """
        Return the concrete subclass of Resource that's appropriate for hot-deploying this module.
        """
        if self.fromVirtualEnv:
            subcls = VirtualEnvResource
        elif os.path.isdir(self._resourcePath):
            subcls = DirectoryResource
        elif os.path.isfile(self._resourcePath):
            subcls = FileResource
        elif os.path.exists(self._resourcePath):
            raise AssertionError("Neither a file or a directory: '%s'" % self._resourcePath)
        else:
            raise AssertionError("No such file or directory: '%s'" % self._resourcePath)
        return subcls

    def localize(self):
        """
        Check if this module was saved as a resource. If it was, return a new module descriptor
        that points to a local copy of that resource. Should only be called on a worker node. On
        the leader, this method returns this resource, i.e. self.

        :rtype: toil.resource.Resource
        """
        if not self._runningOnWorker():
            log.warn('The localize() method should only be invoked on a worker.')
        resource = Resource.lookup(self._resourcePath)
        if resource is None:
            log.warn("Can't localize module %r", self)
            return self
        else:
            def stash(tmpDirPath):
                # Save the original dirPath such that we can restore it in globalize()
                with open(os.path.join(tmpDirPath, '.stash'), 'w') as f:
                    f.write('1' if self.fromVirtualEnv else '0')
                    f.write(self.dirPath)

            resource.download(callback=stash)
            return self.__class__(dirPath=resource.localDirPath,
                                  name=self.name,
                                  fromVirtualEnv=self.fromVirtualEnv)

    def _runningOnWorker(self):
        try:
            mainModule = sys.modules['__main__']
        except KeyError:
            log.warning('Cannot determine main program module.')
            return False
        else:
            mainModuleFile = os.path.basename(mainModule.__file__)
            workerModuleFiles = concat(('worker' + ext for ext in self.moduleExtensions),
                                       '_toil_worker')  # the setuptools entry point
            return mainModuleFile in workerModuleFiles

    def globalize(self):
        """
        Reverse the effect of localize().
        """
        try:
            with open(os.path.join(self.dirPath, '.stash')) as f:
                fromVirtualEnv = [False, True][int(f.read(1))]
                dirPath = f.read()
        except IOError as e:
            if e.errno == errno.ENOENT:
                if self._runningOnWorker():
                    log.warn("Can't globalize module %r.", self)
                return self
            else:
                raise
        else:
            return self.__class__(dirPath=dirPath,
                                  name=self.name,
                                  fromVirtualEnv=fromVirtualEnv)

    @property
    def _resourcePath(self):
        """
        The path to the directory that should be used when shipping this module and its siblings
        around as a resource.
        """
        if self.fromVirtualEnv:
            return self.dirPath
        elif '.' in self.name:
            return os.path.join(self.dirPath, self._rootPackage())
        else:
            initName = self._initModuleName(self.dirPath)
            if initName:
                raise ResourceException(
                    "Toil does not support loading a user script from a package directory. You "
                    "may want to remove %s from %s or invoke the user script as a module via "
                    "'PYTHONPATH=\"%s\" python -m %s.%s'." %
                    tuple(concat(initName, self.dirPath, os.path.split(self.dirPath), self.name)))
            return self.dirPath

    moduleExtensions = ('.py', '.pyc', '.pyo')

    @classmethod
    def _initModuleName(cls, dirPath):
        for extension in cls.moduleExtensions:
            name = '__init__' + extension
            if os.path.exists(os.path.join(dirPath, name)):
                return name
        return None

    def _rootPackage(self):
        try:
            head, tail = self.name.split('.', 1)
        except ValueError:
            raise ValueError('%r is stand-alone module.' % self)
        else:
            return head

    def toCommand(self):
        return tuple(map(str, self))

    @classmethod
    def fromCommand(cls, command):
        assert len(command) == 3
        return cls(dirPath=command[0], name=command[1], fromVirtualEnv=strict_bool(command[2]))

    def makeLoadable(self):
        module = self if self.belongsToToil else self.localize()
        if module.dirPath not in sys.path:
            sys.path.append(module.dirPath)
        return module

    def load(self):
        module = self.makeLoadable()
        try:
            return importlib.import_module(module.name)
        except ImportError:
            log.error('Failed to import user module %r from sys.path (%r).', module, sys.path)
            raise


class ResourceException(Exception):
    pass
