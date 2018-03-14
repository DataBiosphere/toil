import glob
import itertools
import logging
import os
import subprocess
import sys

log = logging.getLogger(__name__)

# A test suite represents an entry in the dictionary below. The entry's key is the name of the
# suite, the entry's value is a list of keywords. A "keyword" is an argument to pytest's -k
# option. It acts as a selector for tests. Each of the keywords in the list will be run
# concurrently. If the list contains None, everything else will be run sequentially once the
# concurrently run tests are finished. Please note that keywords are matched as substrings: Foo
# will match Foo, FooBar and BarFoo.
#

test_suites = {
    'test': [
        'SortTest',
        'AWSJobStoreTest',
        'AzureJobStoreTest',
        'FileJobStoreTest',
        'GoogleJobStoreTest',
        'NonCachingFileStoreTestWithFileJobStore',
        'CachingFileStoreTestWithFileJobStore',
        'NonCachingFileStoreTestWithAwsJobStore',
        'CachingFileStoreTestWithAwsJobStore',
        'NonCachingFileStoreTestWithAzureJobStore',
        'CachingFileStoreTestWithAzureJobStore',
        'NonCachingFileStoreTestWithGoogleJobStore',
        'CachingFileStoreTestWithGoogleJobStore',
        None],
    'integration-test': [
        'AWSRestartTest',
        'PreemptableDeficitCompensationTest',
        'UtilsTest and testAWSProvisionerUtils',
        'AWSAutoscaleTest',
        'AWSStaticAutoscaleTest',
        'AzureRestartTest',
        'AzureAutoscaleTest',
        'AzureStaticAutoscaleTest',
        'GCERestartTest',
        'GCEAutoscaleTest',
        'GCEStaticAutoscaleTest'
    ]}

pytest_errors = ['All tests were collected and passed successfully.',
                 'Tests were collected and run but some of the tests failed.',
                 'Test execution was interrupted by the user.',
                 'Internal error happened while executing tests.',
                 'pytest command line usage error.',
                 'No tests were collected.']


def run_to_xml(keywords, index, args):
    args = [sys.executable, '-m', 'pytest', '-vv', '--timeout=600',
            '--junitxml', 'test-report-%s.xml' % index,
            '-k', keywords] + args
    log.info('Running %r', args)
    return subprocess.Popen(args)


def run_parallel_to_xml(suite, args):
    """
    Runs tests parallel and outputs XML files to be read by Jenkins
    :param suite: Entry in dictionary above
    :param args: auxiliary arguments to pass to pyTest
    :return:  exit status (number of failures)
    """
    suite = test_suites[suite]
    for name in glob.glob('test-report-*.xml'):
        os.unlink(name)
    num_failures = 0
    index = itertools.count()
    pids = set()
    pids_to_keyword = {}
    try:
        for keyword in suite:
            if keyword:
                process = run_to_xml(keyword, str(next(index)), args)
                pids.add(process.pid)
                pids_to_keyword[process.pid] = keyword
        while pids:
            pid, status = os.wait()
            pids.remove(pid)
            if os.WIFEXITED(status):
                status = os.WEXITSTATUS(status)
                if status:
                    num_failures += 1
                    log.info('Test keyword %s failed: %s', pids_to_keyword[pid], pytest_errors[status])
                else:
                    log.info('Test keyword %s passed successfully', pids_to_keyword[pid])
            else:
                num_failures += 1
                log.info('Test keyword %s failed: abnormal exit', pids_to_keyword[pid])

            del pids_to_keyword[pid]
    except:
        for pid in pids:
            os.kill(pid, 15)
        raise

    if None in suite:
        everything_else = ' and '.join('not (%s)' % keyword
                                       for keyword in itertools.chain(*test_suites.values()))
        process = run_to_xml(everything_else, str(next(index)), args)
        if process.wait():
            num_failures += 1
            log.info('Test keyword %s failed which was running in series', everything_else)

    import xml.etree.ElementTree as ET
    testsuites = ET.Element('testsuites')
    for name in glob.glob('test-report-*.xml'):
        log.info("Reading test report %s", name)
        tree = ET.parse(name)
        testsuites.append(tree.getroot())
        os.unlink(name)
    name = 'test-report.xml'
    log.info('Writing aggregate test report %s', name)
    ET.ElementTree(testsuites).write(name, xml_declaration=True)

    if num_failures:
        log.error('%i out %i child processes failed', num_failures, next(index))

    return num_failures


def run_series(suite, args):
    """
    Runs the tests in series and output goes to stdout. To be used when running
    integration tests locally
    :param suite: an entry in dict at top of file
    :param args: auxiliary args to pyTest
    :return: exit status
    """
    keyword = '"' + ' and '.join(keyword for keyword in test_suites[suite]) + '"'
    args = [sys.executable, '-m', 'pytest', '-vv', '--timeout=600', '-s',
            '-k', keyword] + args
    log.info('Running %r in series', args)
    return subprocess.Popen(args)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    if sys.argv[1] == '--local':
        sys.exit(run_series(suite=sys.argv[2], args=sys.argv[3:]))
    else:
        sys.exit(run_parallel_to_xml(suite=sys.argv[1], args=sys.argv[2:]))
