#!/usr/bin/env python2.7
'''
Author : Arjun Arkal Rao
Affiliation : UCSC BME, UCSC Genomics Institute
File : toy_code_for_unfulfilled_promise_error.py
'''
from __future__ import print_function
from toil.job import Job
from toil.test import ToilTest

import unittest
import random
import time
import os
from struct import unpack


class jobCacheTest(ToilTest):
    '''
    Tests the various cache functions
    '''

    def setUp(self):
        super(jobCacheTest, self).setUp()
        testDir = self._createTempDir()
        self.options = Job.Runner.getDefaultOptions(self._getTestJobStorePath())
        self.options.logLevel = 'INFO'
        self.options.workDir = testDir
        self.options.useSharedCache=True

    # sanity
    def testToilIsNotBroken(self):
        '''
        Make a job, make a child, make merry.
        '''
        F = Job.wrapJobFn(_uselessFunc)
        G = Job.wrapJobFn(_uselessFunc)
        H = Job.wrapJobFn(_uselessFunc)
        I = Job.wrapJobFn(_uselessFunc)
        F.addChild(G)
        F.addChild(H)
        G.addChild(I)
        H.addChild(I)
        Job.Runner.startToil(F, self.options)

    # Cache
    def testCacheLockRace(self):
        '''
        Make 2 threads compete for the same cache lock file.
        :return:
        '''
        E = Job.wrapJobFn(_setUpLockFile)
        F = Job.wrapJobFn(_mth, cores=1)
        G = Job.wrapJobFn(_mth, cores=1)
        H = Job.wrapJobFn(_mth, cores=1)
        E.addChild(F)
        E.addChild(G)
        E.addChild(H)
        Job.Runner.startToil(E, self.options)
        with open(os.path.join(self.options.workDir, 'cache/.cacheLock'), 'r') as x:
            values = unpack('iddd', x.read())
            # value of the first entry has to be zero for successful run
            assert values[0] == 0

    @unittest.skip('Needs 2 filesystems')
    def testCacheEviction(self):
        '''
        Ensure the cache eviction happens as expected. The cache max is force set to 200MB. Two jobs
        each write files that combined take more than 150MB. The Third Job asks for 100MB thereby
        requiring a cache eviction. If cache eviction doesn't occur, the caching equation will
        become imbalanced.

        Unfortunately, this test will always pass if thelocal temp dir and the job store are on the
        same filesystem as the caching logic doesn't increment the cached total in that case (file
        sizes are accounted for my the job store).
        '''

        for i in xrange(1, 10):
            file1 = random.choice(xrange(0,150))
            F = Job.wrapJobFn(_writeToFileStore, isLocalFile=True, fileMB=file1)
            G = Job.wrapJobFn(_writeToFileStore, isLocalFile=True, fileMB=(150-file1))
            H = Job.wrapJobFn(_forceModifyCacheLockFile, newTotalMB=200, disk='10M')
            I = Job.wrapJobFn(_uselessFunc, disk='100M')
            # set it to > 2GB such that the cleanup jobs don't die
            J = Job.wrapJobFn(_forceModifyCacheLockFile, newTotalMB=5000, disk='10M')
            F.addChild(G)
            G.addChild(H)
            H.addChild(I)
            I.addChild(J)
            Job.Runner.startToil(F, self.options)
            with open(os.path.join(self.options.workDir, 'cache/.cacheLock'), 'r') as x:
                values = unpack('iddd', x.read())
                assert values[1] <= 5000 * 1024 * 1024
                assert values[2] <= 100 * 1024 * 1024

    # writeGlobalFile tests
    def testWriteNonLocalFileToJobStore(self):
        '''
        Write a file not in localTempDir to the job store. Ensure the file is not
        cached.
        '''
        workdir = self._createTempDir(purpose='nonLocalDir')
        currwd = os.getcwd()
        os.chdir(workdir)
        try:
            F = Job.wrapJobFn(_writeToFileStore, isLocalFile=False)
            Job.Runner.startToil(F, self.options)
        finally:
            os.chdir(currwd)

    def testWriteLocalFileToJobStore(self):
        '''
        Write a file not in localTempDir to the job store. Ensure the file is not
        cached.
        '''
        F = Job.wrapJobFn(_writeToFileStore, isLocalFile=True)
        Job.Runner.startToil(F, self.options)

    # readGlobalFile tests
    def testReadUncachedFileFromJobStore1(self):
        '''
        Read a file from the file store that does not have a corresponding cached copy. Do not cache
        the read file. Ensure the number of links on the file are appropriate.
        '''
        workdir = self._createTempDir(purpose='nonLocalDir')
        currwd = os.getcwd()
        os.chdir(workdir)
        try:
            F = Job.wrapJobFn(_writeToFileStore, isLocalFile=False)
            G = Job.wrapJobFn(_readFromJobStore, isCachedFile=False, cacheReadFile=False, fsID=F.rv())
            F.addChild(G)
            Job.Runner.startToil(F, self.options)
        finally:
            os.chdir(currwd)

    def testReadUncachedFileFromJobStore2(self):
        '''
        Read a file from the file store that does not have a corresponding cached copy. Cache the
        read file. Ensure the number of links on the file are appropriate.
        '''
        workdir = self._createTempDir(purpose='nonLocalDir')
        currwd = os.getcwd()
        os.chdir(workdir)
        try:
            F = Job.wrapJobFn(_writeToFileStore, isLocalFile=False)
            G = Job.wrapJobFn(_readFromJobStore, isCachedFile=False, cacheReadFile=True, fsID=F.rv())
            F.addChild(G)
            Job.Runner.startToil(F, self.options)
        finally:
            os.chdir(currwd)

    def testReadCachedFileFromJobStore(self):
        '''
        Read a file from the file store that has a corresponding cached copy. Ensure the number of
        links on the file are appropriate.
        '''
        F = Job.wrapJobFn(_writeToFileStore, isLocalFile=True)
        G = Job.wrapJobFn(_readFromJobStore, isCachedFile=True, cacheReadFile=None, fsID=F.rv())
        F.addChild(G)
        Job.Runner.startToil(F, self.options)

    def testMultipleJobsReadSameCachedGlobalFile(self):
        '''
        Write a local file to the job store (hence adding a copy to cache), then have 10 jobs read
        it. Assert cached file size in the cache lock file never goes up, assert sigma job reqs is
        always (a multiple of job reqs) - (number of files linked to the cachedfile * filesize). At
        the end, assert the cache lock file shows sigma job = 0.
        :return:
        '''
        temp_dir = self._createTempDir(purpose='tempWrite')
        with open(os.path.join(temp_dir, 'test'), 'w') as x:
            x.write(str(0))
        F = Job.wrapJobFn(_writeToFileStore, isLocalFile=True, fileMB=200)
        G = Job.wrapJobFn(_probeJobReqs, diskMB=100, disk='100M')
        jobs = {}
        for i in xrange(0,10):
            jobs[i] = Job.wrapJobFn(_multipleReader, diskMB=200, fileInfo=F.rv(),
                                    maxWriteFile=os.path.abspath(x.name), disk='200M', memory='10M',
                                    cores=1)
            F.addChild(jobs[i])
            jobs[i].addChild(G)
        Job.Runner.startToil(F, self.options)
        with open(x.name, 'r') as y:
            assert int(y.read()) > 2

    def testMultipleJobsReadSameUnachedGlobalFile(self):
        '''
        Write a non-local file to the job store(hence no cached copy), then have 10 jobs read it.
        Assert cached file size in the cache lock file never goes up, assert sigma job reqs is
        always (a multiple of job reqs) - (number of files linked to the cachedfile * filesize). At
        the end, assert the cache lock file shows sigma job = 0.
        :return:
        '''
        workdir = self._createTempDir(purpose='nonLocalDir')
        with open(os.path.join(workdir, 'test'), 'w') as x:
            x.write(str(0))
        currwd = os.getcwd()
        os.chdir(workdir)
        try:
            F = Job.wrapJobFn(_writeToFileStore, isLocalFile=False, fileMB=1024)
            G = Job.wrapJobFn(_probeJobReqs, diskMB=100, disk='100M')
            jobs = {}
            for i in xrange(0,10):
                jobs[i] = Job.wrapJobFn(_multipleReader, diskMB=1024, fileInfo=F.rv(),
                                        maxWriteFile=os.path.abspath(x.name), disk='2G', memory='10M',
                                        cores=1)
                F.addChild(jobs[i])
                jobs[i].addChild(G)
            Job.Runner.startToil(F, self.options)
        finally:
            os.chdir(currwd)
        with open(x.name, 'r') as y:
            assert int(y.read()) > 2


################################################################################
# Utility functions
################################################################################

def _writeToFileStore(job, isLocalFile, fileMB=1):
    '''
    This function creates a file and writes it to filestore.
    :param bool isLocalFile: Flag. Is the file local(T)?
    :param int fileMB: Size of the created file in MB
    '''
    if isLocalFile:
        work_dir = job.fileStore.getLocalTempDir()
    else:
        work_dir = os.getcwd()
    with open(os.path.join(work_dir, 'testfile.test'), 'w') as testFile:
        #with open('/dev/urandom', 'r') as randFile:
        #    (randFile, testFile, fileMB * 1024 * 1024)
        testFile.write(os.urandom(fileMB * 1024 * 1024))

    fsID = job.fileStore.writeGlobalFile(testFile.name)

    if isLocalFile:
        # Since the file has been hard linked it should have
        # nlink_count = threshold +1 (local, cached, and possibly job store)
        x = job.fileStore.nlinkThreshold + 1
        assert os.stat(testFile.name).st_nlink == x, 'Should have %s ' % x + 'nlinks. Got ' + \
            '%s' % os.stat(testFile.name).st_nlink
    else:
        # Since the file hasn't been hard linked it should have
        # nlink_count = 1
        assert os.stat(testFile.name).st_nlink == 1, 'Should have 1 nlink. Got ' + \
            '%s' % os.stat(testFile.name).st_nlink
    return fsID


def _readFromJobStore(job, isCachedFile, cacheReadFile, fsID, isTest=True):
    '''
    Read a file from the filestore. If the file was cached, ensure it was hard
    linked right. If it wasn't, ensure it was put into cache
    :param bool isCachedFile: Flag. Was the read file read from cache(T)? This defines the nlink
     count to be asserted.
    :param bool cacheReadFile: Flag. Is the file to be cached(T)?
    :param str fsID: job store file ID
    :param bool isTest: Flag. Is this being run as a test(T) or an accessory to another test(F)?

    '''
    work_dir = job.fileStore.getLocalTempDir()
    x = job.fileStore.nlinkThreshold
    if isCachedFile:
        outfile = job.fileStore.readGlobalFile(fsID, '/'.join([work_dir, 'temp']), modifiable=False)
        expectedNlinks = x + 1
    else:
        if cacheReadFile:
            outfile = job.fileStore.readGlobalFile(fsID, '/'.join([work_dir, 'temp']), cache=True,
                                                   modifiable=False)
            expectedNlinks = x + 1
        else:
            outfile = job.fileStore.readGlobalFile(fsID, '/'.join([work_dir, 'temp']), cache=False,
                                                   modifiable=False)
            expectedNlinks = x
    if isTest:
        assert os.stat(outfile).st_nlink == expectedNlinks, 'Should have %s ' % expectedNlinks + \
            'nlinks. Got %s.' % os.stat(outfile).st_nlink
        return None
    else:
        return outfile


def _probeJobReqs(job, diskMB):
    '''
    Probes the cacheLockFile to ensure the previous job returned it's requirements correctly. If
    everything went well, sigmaJob should be equal to the diskMB for this job.
    :param int diskMB: disk requirements provided to the job.
    '''
    with job.fileStore.cacheLock() as x:
        cacheInfo = job.fileStore.CacheStats.load(x)
        expectedMB = diskMB * 1024 * 1024
        assert cacheInfo.sigmaJob == expectedMB, 'Expected %s ' % expectedMB + 'got ' + \
            '%s.' % cacheInfo.sigmaJob


def _multipleReader(job, diskMB, fileInfo, maxWriteFile):
    '''
    Read fsID from file store and add to cache. Assert cached file size in the cache lock file never
    goes up, assert sum of job reqs is always
    (a multiple of job reqs) - (number of files linked to the cachedfile * filesize).

    :param int diskMB: disk requirements provided to the job
    :param str fsID: job store file ID
    :param str maxWriteFile: path to file where the max number of concurrent readers of cache lock
    file will be written
    '''
    fsID = fileInfo
    work_dir = job.fileStore.getLocalTempDir()
    outfile = job.fileStore.readGlobalFile(fsID, '/'.join([work_dir, 'temp']), cache=True,
                                           modifiable=False)
    twohundredmb = diskMB * 1024 * 1024
    with job.fileStore.cacheLock() as lockFileHandle:
        fileStats = os.stat(outfile)
        fileSize = fileStats.st_size
        fileNlinks = fileStats.st_nlink
        with open(maxWriteFile, 'r+') as x:
            prev_max = int(x.read())
            x.seek(0)
            x.truncate()
            x.write(str(max(prev_max, fileNlinks)))
        cacheInfo = job.fileStore.CacheStats.load(lockFileHandle)
        if cacheInfo.nlink == 2:
            assert cacheInfo.cached == 0.0 # since fileJobstore on same filesystem
        else:
            assert cacheInfo.cached == fileSize
        assert ((cacheInfo.sigmaJob + (fileNlinks - cacheInfo.nlink) * fileSize) %
                twohundredmb) == 0.0


def _mth(job):
    '''
    Try to acquire a lock on the lock file. if 2 threads have the lock concurrently, then abort.
    This test abuses the CacheStats class and modifies values in the lock file.
    :return: None
    '''
    for i in xrange(0,1000):
        with job.fileStore.cacheLock() as x:
            cacheInfo = job.fileStore.CacheStats.load(x)
            cacheInfo.nlink += 1
            cacheInfo.cached = max(cacheInfo.nlink, cacheInfo.cached)
            cacheInfo.write(x)
        time.sleep(0.001)
        with job.fileStore.cacheLock() as x:
            cacheInfo = job.fileStore.CacheStats.load(x)
            cacheInfo.nlink -= 1
            cacheInfo.write(x)

def _setUpLockFile(job):
    '''
    set nlink=0 for the cache test
    '''
    with job.fileStore.cacheLock() as x:
        cacheInfo = job.fileStore.CacheStats.load(x)
        cacheInfo.nlink=0
        cacheInfo.write(x)

def _uselessFunc(job):
    '''
    I do nothing. Don't judge me.
    '''
    return None

def _forceModifyCacheLockFile(job, newTotalMB):
    '''
    This function opens and modifies the cache lock file to reflect a new "total" value = newTotalMB
    and thereby fooling the cache logic into believing only newTotalMB is allowed for the run.
    :param int newTotalMB: New value for "total" in the cacheLockFile
    :return:
    '''
    with job.fileStore.cacheLock() as lockFileHandle:
        cacheInfo = job.fileStore.CacheStats.load(lockFileHandle)
        cacheInfo.total = float(newTotalMB * 1024 * 1024)
        cacheInfo.write(lockFileHandle)


