#!/usr/bin/env python2.7
'''
Author : Arjun Arkal Rao
Affiliation : UCSC BME, UCSC Genomics Institute
File : toy_code_for_unfulfilled_promise_error.py
'''
from __future__ import print_function
from toil.job import Job

import tempfile
import time
import os
import argparse

################################################################################
# Tests
################################################################################

# Sanity
def test_toil_isnt_broken():
    '''
    Make a job, make a child, make merry.
    '''
    parser = argparse.ArgumentParser()
    parser.add_argument('--dummy', dest='dummy', default=None, required=False)
    Job.Runner.addToilOptions(parser)
    params = parser.parse_args(['--logDebug', '--workDir',
                                '/tmp/test_toil/filestore',
                                '/tmp/test_toil/jobstore'])
    F = Job.wrapJobFn(sleep_func)
    G = Job.wrapJobFn(sleep_func)
    H = Job.wrapJobFn(sleep_func)
    I = Job.wrapJobFn(sleep_func)
    F.addChild(G)
    F.addChild(H)
    G.addChild(I)
    H.addChild(I)
    Job.Runner.startToil(F, params)


# Cache functions specific
def test_cache_race():
    '''
    Make 2 threads compete for the same cache lock file. If they have the lock
    at the same time, FAIL.
    :return:
    '''
    E = Job.wrapJobFn(sleep_func)
    F = Job.wrapJobFn(mth, cores=1)
    G = Job.wrapJobFn(mth, cores=1)
    H = Job.wrapJobFn(mth, cores=1)
    E.addChild(F)
    E.addChild(G)
    E.addChild(H)

    parser = argparse.ArgumentParser()
    parser.add_argument('--dummy', dest='dummy', default=None, required=False)
    Job.Runner.addToilOptions(parser)
    params = parser.parse_args(['--workDir', '/tmp/test_toil/filestore',
                                '/tmp/test_toil/jobstore'])
    Job.Runner.startToil(E, params)

# WriteGlobalFile tests

def test_getting_file_into_fs():
    '''
    Write a file not in localTempDIr to the fileStore. Ensure the file is not
    cached.
    '''
    parser = argparse.ArgumentParser()
    parser.add_argument('--dummy', dest='dummy', default=None, required=False)
    Job.Runner.addToilOptions(parser)
    params = parser.parse_args(['--logDebug', '--workDir',
                                '/tmp/test_toil/filestore',
                                '/tmp/test_toil/jobstore'])
    F = Job.wrapJobFn(write_to_fs, False)
    Job.Runner.startToil(F, params)


def test_write_local_file_to_cache():
    '''
    Make a job, make a child, make merry.
    '''
    parser = argparse.ArgumentParser()
    parser.add_argument('--dummy', dest='dummy', default=None, required=False)
    Job.Runner.addToilOptions(parser)
    params = parser.parse_args(['--logDebug', '--workDir',
                                '/tmp/test_toil/filestore',
                                '/tmp/test_toil/jobstore'])
    F = Job.wrapJobFn(write_to_fs, True)
    Job.Runner.startToil(F, params)


# readGlobalFile tests
def child_reads_uncached_file_from_filestore_1():
    '''
    Read a file from the file store that does not have a corresponding cached copy. Do not cache the read file. Ensure
    the number of links on the file are appropriate.
    '''
    parser = argparse.ArgumentParser()
    parser.add_argument('--dummy', dest='dummy', default=None, required=False)
    Job.Runner.addToilOptions(parser)
    params = parser.parse_args(['--logDebug', '--workDir',
                                '/tmp/test_toil/filestore',
                                '/tmp/test_toil/jobstore'])
    F = Job.wrapJobFn(write_to_fs, False)
    G = Job.wrapJobFn(read_from_fs, False, False, F.rv())
    F.addChild(G)
    Job.Runner.startToil(F, params)


def child_reads_uncached_file_from_filestore_2():
    '''
    Read a file from the file store that does not have a corresponding cached copy. Cache the read file. Ensure the
    number of links on the file are appropriate.
    '''
    parser = argparse.ArgumentParser()
    parser.add_argument('--dummy', dest='dummy', default=None, required=False)
    Job.Runner.addToilOptions(parser)
    params = parser.parse_args(['--logDebug', '--workDir',
                                '/tmp/test_toil/filestore',
                                '/tmp/test_toil/jobstore'])
    F = Job.wrapJobFn(write_to_fs, False)
    G = Job.wrapJobFn(read_from_fs, False, True, F.rv())
    F.addChild(G)
    Job.Runner.startToil(F, params)


def child_reads_cached_file_from_filestore():
    '''
    Write a file not in localTempDIr to the fileStore. Ensure the file is not
    cached.
    '''
    parser = argparse.ArgumentParser()
    parser.add_argument('--dummy', dest='dummy', default=None, required=False)
    Job.Runner.addToilOptions(parser)
    params = parser.parse_args(['--logDebug', '--workDir',
                                '/tmp/test_toil/filestore',
                                '/tmp/test_toil/jobstore'])
    F = Job.wrapJobFn(write_to_fs, True)
    G = Job.wrapJobFn(read_from_fs, True, None, F.rv())
    F.addChild(G)
    Job.Runner.startToil(F, params)


def test_multiple_readers_of_cached_file():
    '''
    Write a file, then have 10 jobs read it. Assert cached file size in the cache lock file never goes up, assert
    sigma job reqs is always (a multiple of job reqs) - (number of files linked to the cachedfile * filesize). At the
    end, assert the cache lock file shows sigma job = 0.
    :return:
    '''
    parser = argparse.ArgumentParser()
    parser.add_argument('--dummy', dest='dummy', default=None, required=False)
    Job.Runner.addToilOptions(parser)
    params = parser.parse_args(['--logInfo', '--workDir',
                                '/tmp/test_toil/filestore',
                                '/tmp/test_toil/jobstore'])
    F = Job.wrapJobFn(write_to_fs, True, 200)
    G = Job.wrapJobFn(probe_job_reqs, disk='100M')
    jobs = {}
    for i in xrange(0,50):
        jobs[i] = Job.wrapJobFn(multiple_reader, 200, F.rv(), disk='200M', memory='10M', cores=1)
        F.addChild(jobs[i])
        jobs[i].addChild(G)
    Job.Runner.startToil(F, params)


def test_multiple_readers_of_uncached_file():
    '''
    Write a file, then have 10 jobs read it. Assert cached file size in the cache lock file never goes up, assert
    sigma job reqs is always (a multiple of job reqs) - (number of files linked to the cachedfile * filesize). At the
    end, assert the cache lock file shows sigma job = 0.
    :return:
    '''
    parser = argparse.ArgumentParser()
    parser.add_argument('--dummy', dest='dummy', default=None, required=False)
    Job.Runner.addToilOptions(parser)
    params = parser.parse_args(['--logDebug', '--workDir',
                                '/tmp/test_toil/filestore',
                                '/tmp/test_toil/jobstore'])
    F = Job.wrapJobFn(write_to_fs, False, filemb=1024)
    G = Job.wrapJobFn(probe_job_reqs, disk='100M')
    jobs = {}
    for i in xrange(0,10):
        jobs[i] = Job.wrapJobFn(multiple_reader, 1024, F.rv(), disk='2G', memory='10M', cores=1)
        F.addChild(jobs[i])
        jobs[i].addChild(G)
    Job.Runner.startToil(F, params)


# deleteLocalFile
def delete_self_written_uncached_file():
    '''
    Write a file to filestore that is not cached. Attempt to delete it. Ensure the cache lock file holds the right
    values
    :return:
    '''
    parser = argparse.ArgumentParser()
    parser.add_argument('--dummy', dest='dummy', default=None, required=False)
    Job.Runner.addToilOptions(parser)
    params = parser.parse_args(['--logDebug', '--workDir',
                                '/tmp/test_toil/filestore',
                                '/tmp/test_toil/jobstore'])
    F = Job.wrapJobFn(local_delete, True, False, disk='200M')
    Job.Runner.startToil(F, params)


def delete_self_written_cached_file():
    '''
    Write a file to filestore that is also cached. Attempt to delete it. Ensure the cache lock file holds the right
    values
    :return:
    '''
    parser = argparse.ArgumentParser()
    parser.add_argument('--dummy', dest='dummy', default=None, required=False)
    Job.Runner.addToilOptions(parser)
    params = parser.parse_args(['--logDebug', '--workDir',
                                '/tmp/test_toil/filestore',
                                '/tmp/test_toil/jobstore'])
    F = Job.wrapJobFn(local_delete, True, True, disk='200M')
    Job.Runner.startToil(F, params)


def delete_globally_read_uncached_file():
    '''
    Write a file to filestore that is not cached. Attempt to delete it. Ensure the cache lock file holds the right
    values
    :return:
    '''
    parser = argparse.ArgumentParser()
    parser.add_argument('--dummy', dest='dummy', default=None, required=False)
    Job.Runner.addToilOptions(parser)
    params = parser.parse_args(['--logDebug', '--workDir',
                                '/tmp/test_toil/filestore',
                                '/tmp/test_toil/jobstore'])
    F = Job.wrapJobFn(write_to_fs, False)
    G = Job.wrapJobFn(local_delete, False, False, F.rv(), disk='200M')
    F.addChild(G)
    Job.Runner.startToil(F, params)


def delete_globally_read_cached_file():
    '''
    Write a file to filestore that is also cached. Attempt to delete it. Ensure the cache lock file holds the right
    values
    :return:
    '''
    parser = argparse.ArgumentParser()
    parser.add_argument('--dummy', dest='dummy', default=None, required=False)
    Job.Runner.addToilOptions(parser)
    params = parser.parse_args(['--logDebug', '--workDir',
                                '/tmp/test_toil/filestore',
                                '/tmp/test_toil/jobstore'])
    F = Job.wrapJobFn(write_to_fs, False)
    G = Job.wrapJobFn(local_delete, False, True, F.rv(), disk='200M')
    F.addChild(G)
    Job.Runner.startToil(F, params)


# Runtime tests
def test_jobreqs_being_returned():
    '''
    Make a job, make a child, make merry.
    '''
    parser = argparse.ArgumentParser()
    parser.add_argument('--dummy', dest='dummy', default=None, required=False)
    Job.Runner.addToilOptions(parser)
    params = parser.parse_args(['--logDebug', '--workDir',
                                '/tmp/test_toil/filestore',
                                '/tmp/test_toil/jobstore'])
    F = Job.wrapJobFn(write_to_fs, True, disk='200M')
    G = Job.wrapJobFn(probe_job_reqs, disk='100M')
    F.addChild(G)
    Job.Runner.startToil(F, params)

################################################################################
# Utility functions
################################################################################

def sleep_func(job):
    '''
    sleep for 5s.
    '''
    assert TypeError('poo')
    time.sleep(5)
    return None

def mth(job):
    '''
    Try to acquire a lock on the lock file. if 2 threads have the lock
    concurrently, then abort.
    :return: None
    '''
    for i in xrange(0,1000):
        with job.fileStore.cacheLock() as x:
            cacheInfo = job.fileStore.CacheStats.load(x)
            assert cacheInfo.nlink != '0', 'FAIL'
            cacheInfo.nlink = 0
            cacheInfo.write(x)
            time.sleep(0.001)
            cacheInfo = job.fileStore.CacheStats.load(x)
            cacheInfo.nlink = 2
            cacheInfo.write(x)

def write_to_fs(job, local_file, test=True, filemb=1):
    '''
    This function creates a file and writes it to filestore.
    '''
    if local_file:
        work_dir = job.fileStore.getLocalTempDir()
    else:
        work_dir = os.path.abspath('.')
    with open(os.path.join(work_dir, 'testfile.test'), 'w') as testfile:
        testfile.write(os.urandom(filemb*1024*1024))
    fsID = job.fileStore.writeGlobalFile(testfile.name)

    if local_file:
        # Since the file has been hard linked it should have
        # nlink_count = threshold +1 (local, cached, and possibly filestore)
        x = job.fileStore.nlinkThreshold + 1
        assert os.stat(testfile.name).st_nlink == x, 'Should have %s ' % x + \
            'nlinks. Got %s' % os.stat(testfile.name).st_nlink
    else:
        # Since the file hasn't been hard linked it should have
        # nlink_count = 1
        assert os.stat(testfile.name).st_nlink == 1, 'Should have 1 nlink.' + \
            ' Got %s' % os.stat(testfile.name).st_nlink
    if test:
        return fsID
    else:
        return fsID, testfile.name

def read_from_fs(job, cached_file, cache_read_file, fsID, test=True):
    '''
    Read a file from the filestore. If the file was cached, ensure it was hard
    linked right. If it wasn't, ensure it was put into cache
    '''
    work_dir = job.fileStore.getLocalTempDir()
    x = job.fileStore.nlinkThreshold
    if cached_file:
        outfile = job.fileStore.readGlobalFile(fsID, '/'.join([work_dir,
                                                               'temp']))
        assert os.stat(outfile).st_nlink == x + 1, 'Should have %s' % (x+1) + \
            ' nlinks. Got %s.' % os.stat(outfile).st_nlink
    else:
        if not cache_read_file:
            outfile = job.fileStore.readGlobalFile(fsID, '/'.join([work_dir,
                                                                   'temp']),
                                                   cache=False)
            assert os.stat(outfile).st_nlink == x, 'Should have %s nlinks.' % x + \
                ' Got %s.' % os.stat(outfile).st_nlink
        else:
            outfile = job.fileStore.readGlobalFile(fsID, '/'.join([work_dir,
                                                                   'temp']),
                                                   cache=True)
            assert os.stat(outfile).st_nlink == x + 1, 'Should have %s' % (x+1) + \
                ' nlinks. Got %s.' % os.stat(outfile).st_nlink
    if test:
        return None
    else:
        return outfile


def local_delete(job, write_new_file, cache_test_file, fsID=None):
    '''
    try to delete
    :param job:
    :param bool write_new_file: Flag to indicate whether this function does the file write as well
    :param bool cache_test_file: Flag to indicate whether the test file should be cached or not
    :param fsID: FileStore ID. Not required if write_new_file == True
    :return: None
    '''
    # Setup the file to be tested
    if write_new_file:
        # piggyback off other functions
        fsID, outfile = write_to_fs(job, cache_test_file, test=False)
    else:
        assert fsID is not None, 'You done fucked up, son'
        outfile = read_from_fs(job, False, cache_test_file, fsID, test=False)
    # Now test
    job.fileStore.deleteLocalFile(fsID)
    with job.fileStore.cacheLock() as lockFileHandle:
        cacheInfo = job.fileStore.CacheStats.load(lockFileHandle)
        assert cacheInfo.sigmaJob == 200 * 1024 * 1024


def probe_job_reqs(job):
    '''
    Probes the cacheLockFile to ensure the previous job returned it's
    requirements correctly
    '''
    with job.fileStore.cacheLock() as x:
        cacheInfo = job.fileStore.CacheStats.load(x)
        hmb = 100 * 1024 * 1024
        assert cacheInfo.sigmaJob == hmb, 'Expected %s ' % hmb + \
            'got %s.' % cacheInfo.sigmaJob


def multiple_reader(job, diskmb, fsID):
    '''
    Read fsID from file store. Assert cached file size in the cache lock file never goes up, assert
    sigma job reqs is always (a multiple of job reqs) - (number of files linked to the cachedfile * filesize).
    :param job:
    :param int diskmb:
    :param fsID:
    :return:
    '''
    work_dir = job.fileStore.getLocalTempDir()
    outfile = job.fileStore.readGlobalFile(fsID, '/'.join([work_dir, 'temp']), cache=True)
    #time.sleep(20)
    twohundredmb = diskmb * 1024 * 1024
    with job.fileStore.cacheLock() as lockFileHandle:
        fileStats = os.stat(outfile)
        fileSize = fileStats.st_size
        fileNlinks = fileStats.st_nlink
        cacheInfo = job.fileStore.CacheStats.load(lockFileHandle)
        try:
            if cacheInfo.nlink == 2:
                assert cacheInfo.cached == 0.0 # since fileJobstore on same filesystem
            else:
                assert cacheInfo.cached == fileSize
            assert (cacheInfo.sigmaJob + (fileNlinks - cacheInfo.nlink) * fileSize) % twohundredmb == 0.0
        except AssertionError:
            lockFileHandle.close()
            raise


if __name__ == '__main__':
    # sanity
    #test_toil_isnt_broken() # PASS

    # cache
    #test_cache_race() # PASS

    # writeGlobalFile
    #test_getting_file_into_fs() # PASS
    #test_write_local_file_to_cache() # PASS

    # readGlobalFile
    #child_reads_uncached_file_from_filestore_1() # PASS
    #child_reads_uncached_file_from_filestore_2() # PASS
    #child_reads_cached_file_from_filestore() # PASS
    #test_multiple_readers_of_cached_file() # PASS
    #test_multiple_readers_of_uncached_file() # PASS

    # deleteLocalFile
    #delete_self_written_uncached_file() # PASS
    #delete_self_written_cached_file() # PASS
    #delete_globally_read_uncached_file() # PASS
    #delete_globally_read_cached_file() # PASS

    # Runtime
    #test_jobreqs_being_returned() # PASS

