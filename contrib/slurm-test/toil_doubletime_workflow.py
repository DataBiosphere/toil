import time

from toil.common import Toil
from toil.job import Job


def slow_job(seconds):
    time.sleep(seconds)
    return f"Sleep completed successfully"


if __name__ == "__main__":
    parser = Job.Runner.getDefaultArgumentParser()
    options = parser.parse_args()
    options.clean = "always"
    with Toil(options) as toil:
        # Sleep for longer than the initial walltime.

        # Toil's Slurm integration will signal the job some time *before* the
        # time it asked for actually elapsed. So we need to make sure there's
        # space for that extra time.

        # TODO: Make Toil pad the time requested from the batch system and deal
        # with the user sometimes not getting the partition they expected?
        output = toil.start(
            Job.wrapFn(slow_job, 120, memory="1G", cores=1, disk="1G", walltime=100)
        )
    with open("doubletime_output.txt", "w") as f:
        f.write(output)
