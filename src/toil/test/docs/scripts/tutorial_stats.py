import os

from toil.common import Toil
from toil.job import Job

import math
import time

from multiprocessing import Process

def think(seconds):
    start = time.time()
    while time.time() - start < seconds:
        # Use CPU
        math.sqrt(123456)

class TimeWaster(Job):
    def __init__(self, time_to_think, time_to_waste, *args, **kwargs):
        self.time_to_think = time_to_think
        self.time_to_waste = time_to_waste
        super().__init__(*args, **kwargs)

    def run(self, fileStore):
        
        processes = []
        for core_number in range(max(1, self.cores)):
            # Use all the assigned cores to think
            p = Process(target=think, args=(self.time_to_think,))
            p.start()
            processes.append(p)
        for p in processes:
            p.join()

        # Also waste some time
        time.sleep(self.time_to_waste)


def main():
    options = Job.Runner.getDefaultArgumentParser().parse_args()

    job1 = TimeWaster(0, 0, displayName='doNothing')
    job2 = TimeWaster(10, 0, displayName='efficientJob')
    job3 = TimeWaster(1, 9, displayName='inefficientJob')
    job4 = TimeWaster(10, 0, cores=4, displayName='multithreadedJob')

    job1.addChild(job2)
    job1.addChild(job3)
    job1.addChild(job4)

    with Toil(options) as toil:
        if not toil.options.restart:
            toil.start(job1)
        else:
            toil.restart()


if __name__ == '__main__':
    main()
