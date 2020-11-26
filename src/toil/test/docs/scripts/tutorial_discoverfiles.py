import os
import subprocess

from toil.common import Toil
from toil.job import Job


class discoverFiles(Job):
    """Views files at a specified path using ls."""
    def __init__(self, path, *args, **kwargs):
        self.path = path
        super(discoverFiles, self).__init__(*args, **kwargs)

    def run(self, fileStore):
        if os.path.exists(self.path):
            subprocess.check_call(["ls", self.path])

def main():
    options = Job.Runner.getDefaultArgumentParser().parse_args()
    options.clean = "always"

    job1 = discoverFiles(path="/sys/", displayName='sysFiles')
    job2 = discoverFiles(path=os.path.expanduser("~"), displayName='userFiles')
    job3 = discoverFiles(path="/tmp/")

    job1.addChild(job2)
    job2.addChild(job3)

    with Toil(options) as toil:
        if not toil.options.restart:
            toil.start(job1)
        else:
            toil.restart()

if __name__ == '__main__':
    main()
