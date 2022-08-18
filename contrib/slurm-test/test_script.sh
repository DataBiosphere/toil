#!/bin/bash
#
#SBATCH --job-name=test
#SBATCH --output=result.out
#
#SBATCH --ntasks=6
#
squeue
#sbcast -f test.py /tmp/test.py
#srun python3 /tmp/test.py