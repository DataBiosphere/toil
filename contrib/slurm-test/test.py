#!/usr/bin/env python3

import time
import os
import socket
from datetime import datetime as dt

if __name__ == '__main__':
    print('Process started {}'.format(dt.now()))
    print('NODE : {}'.format(socket.gethostname()))
    print('PID  : {}'.format(os.getpid()))
    print('Executing for 15 secs')
    time.sleep(15)
    print('Process finished {}\n'.format(dt.now()))