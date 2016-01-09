#!/usr/bin/env python2.7
'''
Author : Arjun Arkal Rao
Affiliation : UCSC BME, UCSC Genomics Institute
File : toy_code_for_unfulfilled_promise_error.py
'''
from __future__ import print_function
from toil.job import Job
from toil.test import ToilTest
from toil.cache import Cache
from multiprocessing import Process

import unittest
import tempfile
import time
import os





################################################################################
# Tests
################################################################################

class jobCacheTest(ToilTest):
    '''
    Tests the various cache functions
    '''

    def test_cache_race(self):
        '''
        Make 2 threads compete for the same cache lock file. If they have the lock at the same time, FAIL.
        :return:
        '''
        threads = {}
        cacheDir = tempfile.mkdtemp()
        for i in xrange(0,3):
            threads[i] = Process(target=mth, args=[cacheDir, i])
            threads[i].start()
        for i in xrange(0,3):
            threads[i].join()
    def test_release_flock():
        '''
        Make 2 threads compete for the same cache lock file. If they have the lock at the same time, FAIL.
        :return:
        '''
        threads = {}
        cacheDir = tempfile.mkdtemp()
        thread1 = Process(target=mth1, args=[cacheDir, 'thread1'])
        thread2 = Process(target=mth2, args=[cacheDir, 'thread2'])
        thread1.start()
        thread2.start()
        thread1.join()
        thread2.join()

    @unittest.skip("demonstrating skipping")
    def test_toil_isnt_broken(self):
        '''
        Make a job, make a child, make merry.
        '''
        F = Job.wrapJobFn(sleep_func)
        G = Job.wrapJobFn(sleep_func)
        H = Job.wrapJobFn(sleep_func)
        I = Job.wrapJobFn(sleep_func)
        F.addChild(G)
        F.addChild(H)
        G.addChild(I)
        H.addChild(I)

        Job.Runner.startToil(F, params)

    @unittest.skip("demonstrating skipping")
    def test_getting_file_into_fs(self):
        '''
        Write a file not in localTempDIr to the fileStore. Ensure the file is not
        cached.
        '''
        F = Job.wrapJobFn(write_to_fs, False)
        Job.Runner.startToil(F, params)

    @unittest.skip("demonstrating skipping")
    def test_write_local_file_to_cache(self):
        '''
        Make a job, make a child, make merry.
        '''
        F = Job.wrapJobFn(write_to_fs, True)
        Job.Runner.startToil(F, params)

    @unittest.skip("demonstrating skipping")
    def child_reads_uncached_file_from_filestore(self):
        '''
        Write a file not in localTempDIr to the fileStore. Ensure the file is not
        cached.
        '''
        F = Job.wrapJobFn(write_to_fs, False)
        G = Job.wrapJobFn(read_from_fs, False, F.rv())
        F.addChild(G)
        Job.Runner.startToil(F, params)

    @unittest.skip("demonstrating skipping")
    def child_reads_cached_file_from_filestore(self):
        '''
        Write a file not in localTempDIr to the fileStore. Ensure the file is not
        cached.
        '''
        F = Job.wrapJobFn(write_to_fs, True)
        G = Job.wrapJobFn(read_from_fs, True, F.rv())
        F.addChild(G)
        Job.Runner.startToil(F, params)

################################################################################
# Utility functions
################################################################################
def write_to_fs(job, local_file):
    '''
    This function creates a file and writes it to filestore.
    '''
    if local_file:
        work_dir = job.fileStore.getLocalTempDir()
    else:
        work_dir = os.path.abspath('.')
    with open(os.path.join(work_dir, 'testfile.test'), 'w') as testfile:
        testfile.write(os.urandom(100000))
    fsID = job.fileStore.writeGlobalFile(testfile.name)

    if local_file:
        # Since the file has been hard linked it should have nlink_count=3 (local, cached, and filestore)
        assert os.stat(testfile.name).st_nlink == 3, 'Should have 3 nlinks. Got %s' % os.stat(testfile.name).st_nlink
    else:
        # Since the file hasn't been hard linked it should have nlink_count=1
        assert os.stat(testfile.name).st_nlink == 1, 'Should have 1 nlinks. Got %s' % os.stat(testfile.name).st_nlink
    os.remove(testfile.name)
    return fsID

def read_from_fs(job, cached_file, fsID):
    '''
    Read a file from the filestore. If the file was cached, ensure it was hard linked right. If it wasn't, ensure it was
    put into cache
    '''
    work_dir = job.fileStore.getLocalTempDir()
    if cached_file:
        outfile = job.fileStore.readGlobalFile(fsID, '/'.join([work_dir, 'temp']))
        assert os.stat(outfile).st_nlink == 3, 'Should have 3 nlinks. Got %s' % os.stat(testfile.name).st_nlink
    else:
        outfile = job.fileStore.readGlobalFile(fsID, '/'.join([work_dir, 'temp']), cache=False)
        assert os.stat(outfile).st_nlink == 2, 'Should have 2 nlinks. Got %s' % os.stat(testfile.name).st_nlink
        os.remove(outfile)
        outfile = job.fileStore.readGlobalFile(fsID, '/'.join([work_dir, 'temp']), cache=True)
        assert os.stat(outfile).st_nlink == 3, 'Should have 3 nlinks. Got %s' % os.stat(testfile.name).st_nlink
    return None


def sleep_func(job):
    '''
    sleep for 5s.
    '''
    time.sleep(5)
    return None

def mth(cacheDir, z):
    '''
    Try to acquire a lock on the lock file. if 2 threads have the lock concurrently, then abort.
    :return: None
    '''
    cacheObject = Cache(cacheDir, 5)
    flag = False
    print('%s started' % z)
    for i in xrange(0,10000):
        with cacheObject.cacheLock() as x:
            assert flag == False
            flag = True
            time.sleep(0.001)
            flag=False
    print('%s ended' % z)

def mth1(cacheDir, z):
    '''
    Try to acquire a lock on the lock file. Then release the lock, then reacquire it in the same context manager construct. If 2 threads have the lock concurrently, then abort.
    :return: None
    '''
    cacheObject = Cache(cacheDir, 5)
    flag = False
    print('%s started' % z)
    for i in xrange(0,1000):
        with cacheObject.cacheLock() as x:
            print(z, 'has the lock.', sep=' ')
            assert flag == False
            flag = True
            time.sleep(0.001)
            flag=False
            flock(x, LOCK_UN)
            time.sleep(0.001)
            flock(x, LOCK_EX)
            print(z, 'has the lock again.', sep=' ')
            assert flag == False
            flag = True
            time.sleep(0.001)
            flag=False
    print('%s ended' % z)

def mth2(cacheDir, z):
    '''
    Try to acquire a lock on the lock file. if 2 threads have the lock concurrently, then abort.
    :return: None
    '''
    cacheObject = Cache(cacheDir, 5)
    flag = False
    print('%s started' % z)
    for i in xrange(0,1000):
        with cacheObject.cacheLock() as x:
            print(z, 'has the lock.', sep=' ')
            assert flag == False
            flag = True
            time.sleep(0.001)
            flag=False
    print('%s ended' % z)