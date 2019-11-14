import glob
import itertools
import logging
import os
import subprocess
import sys
import time
from future.utils import iteritems

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
        'FileJobStoreTest',
        'GoogleJobStoreTest',
        'NonCachingFileStoreTestWithFileJobStore',
        'CachingFileStoreTestWithFileJobStore',
        'NonCachingFileStoreTestWithAwsJobStore',
        'CachingFileStoreTestWithAwsJobStore',
        'NonCachingFileStoreTestWithGoogleJobStore',
        'CachingFileStoreTestWithGoogleJobStore',
        None],
    'integration-test': [
        'AWSRestartTest',
        'PreemptableDeficitCompensationTest',
        'UtilsTest and testAWSProvisionerUtils',
        'AWSAutoscaleTest',
        'AWSStaticAutoscaleTest',
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

    # Keep track of the running popen objects by keyword
    keyword_to_process = {}
    try:
        for keyword in suite:
            if keyword:
                keyword_to_process[keyword] = run_to_xml(keyword, str(next(index)), args)

        # We will periodically poll to see what is running, and every so many
        # loops we will announce it, so watching the Jenkins log isn't boring.
        loops = 0

        while len(keyword_to_process) > 0:
            # Make a list of finished keywords
            finished = []
            for keyword, process in iteritems(keyword_to_process):
                if process.poll() is not None:
                    # This keyword has finished!
                    finished.append(keyword)
                    status = process.returncode

                    if status > 0:
                        num_failures += 1
                        if status < len(pytest_errors):
                            log.info('Test keyword %s failed: %s', keyword, pytest_errors[status])
                        else:
                            log.info('Test keyword %s failed with unassigned exit code %d', keyword, status)
                    elif status == 0:
                        log.info('Test keyword %s passed successfully', keyword)
                    else:
                        # We got a signal
                        num_failures += 1
                        log.info('Test keyword %s failed with code %d: abnormal exit', keyword, status)

            for keyword in finished:
                # Clean up the popen objects for finished test runs
                del keyword_to_process[keyword]

            if loops % 30 == 0:
                # Announce what is still running about every 5 minutes
                log.info('Still running at %d: %s', loops, str(list(keyword_to_process.keys())))

            loops += 1
            time.sleep(10)

    except:
        for process in keyword_to_process.values():
            process.terminate()
        raise

    if None in suite:
        everything_else = ' and '.join('not (%s)' % keyword
                                       for keyword in itertools.chain(*test_suites.values()))
        log.info('Starting other tests')
        process = run_to_xml(everything_else, str(next(index)), args)

        loops = 0
        while process.poll() is None:
            if loops % 30 == 0:
                log.info('Still running other tests at %d', loops)
            loops += 1
            time.sleep(10)
        if process.returncode:
            num_failures += 1
            log.info('Test keyword %s failed which was running in series', everything_else)
        log.info('Finished other tests')

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
