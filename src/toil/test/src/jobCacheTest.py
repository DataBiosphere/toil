#!/usr/bin/env python2.7
'''
Author : Arjun Arkal Rao
Affiliation : UCSC BME, UCSC Genomics Institute
File : toy_code_for_unfulfilled_promise_error.py
'''
from __future__ import print_function
from toil.job import Job
from toil.test import ToilTest

import time
import os




################################################################################
# Tests
################################################################################

class jobCacheTest(ToilTest):
    '''
    Tests the various cache functions
    '''
    def test_toil_isnt_broken():
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

    def test_getting_file_into_fs():
        '''
        Write a file not in localTempDIr to the fileStore. Ensure the file is not
        cached.
        '''
        F = Job.wrapJobFn(write_to_fs, False)
        Job.Runner.startToil(F, params)

    def test_write_local_file_to_cache():
        '''
        Make a job, make a child, make merry.
        '''
        F = Job.wrapJobFn(write_to_fs, True)
        Job.Runner.startToil(F, params)

    def child_reads_uncached_file_from_filestore():
        '''
        Write a file not in localTempDIr to the fileStore. Ensure the file is not
        cached.
        '''
        F = Job.wrapJobFn(write_to_fs, False)
        G = Job.wrapJobFn(read_from_fs, False, F.rv())
        F.addChild(G)
        Job.Runner.startToil(F, params)

    def child_reads_cached_file_from_filestore():
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
