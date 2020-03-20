from builtins import range
import errno
import logging
import multiprocessing
import os
import random
import tempfile
import time
import traceback
from functools import partial

from toil.lib.threading import global_mutex, LastProcessStandingArena, cpu_count
from toil.test import ToilTest, travis_test

log = logging.getLogger(__name__)

class ThreadingTest(ToilTest):
    """
    Test Toil threading/synchronization tools
    """
    
    @travis_test
    def testGlobalMutexOrdering(self):
        for it in range(10):
            log.info('Iteration %d', it)
        
            scope = self._createTempDir()
            mutex = 'mutex'
            # Use processes (as opposed to threads) to prevent GIL from ordering things artificially
            pool = multiprocessing.Pool()
            try:
                numTasks = 100
                results = pool.map_async(
                    func=partial(_testGlobalMutexOrderingTask, scope, mutex),
                    iterable=list(range(numTasks)))
                results = results.get()
            finally:
                pool.close()
                pool.join()
            
            self.assertEqual(len(results), numTasks)
            for item in results:
                # Make sure all workers say they succeeded
                self.assertEqual(item, True)

def _testGlobalMutexOrderingTask(scope, mutex, number):
    try:
        # We will all fight over the potato
        potato = os.path.join(scope, 'potato')
        
        with global_mutex(scope, mutex):
            log.info('PID %d = num %d running', os.getpid(), number)
            assert not os.path.exists(potato), "We see someone else holding the potato file"
            
            # Put our name there
            out_stream = open(potato, 'w')
            out_stream.write(str(number))
            out_stream.close()
            
            # Wait
            time.sleep(random.random() * 0.01)
            
            # Make sure our name is still there
            in_stream = open(potato, 'r')
            seen = in_stream.read().rstrip() 
            assert seen == str(number), "We are {} but {} stole our potato!".format(number, seen)
            in_stream.close()
            
            os.unlink(potato)
            assert not os.path.exists(potato), "We left the potato behind"
            log.info('PID %d = num %d dropped potato', os.getpid(), number)
        return True
    except:
        traceback.print_exc()
        return False

