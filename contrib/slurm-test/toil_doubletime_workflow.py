import time

from toil.common import Toil
from toil.job import Job


def slow_job(seconds):
    # Sleep past the point where Slurm will warn us that our first walltime is
    # nearly up, but not past the point where it would warn us about the
    # doubled one, so that the job can only finish on the retry.
    time.sleep(seconds)
    return f"Slept for {seconds} seconds"


if __name__ == "__main__":
    parser = Job.Runner.getDefaultArgumentParser()
    options = parser.parse_args()
    options.clean = "always"
    with Toil(options) as toil:
        # Sleep for exactly the first walltime. Slurm's warning always lands
        # before the limit it is warning about, and it can land a whole
        # scheduling tick early, so this is sure to be interrupted the first
        # time and sure to be finished long before the warning on the doubled
        # limit.
        output = toil.start(
            Job.wrapFn(slow_job, 120, memory="1G", cores=1, disk="1G", walltime=120)
        )
    with open("doubletime_output.txt", "w") as f:
        f.write(output)
