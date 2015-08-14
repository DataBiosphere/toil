from __future__ import absolute_import
use_multiprocessing = True

if use_multiprocessing:
    from multiprocessing import Process
    from multiprocessing import JoinableQueue as Queue
else:
    from threading import Thread as Process
    from Queue import Queue

# TODO: Do the same for pickle and cPickle
