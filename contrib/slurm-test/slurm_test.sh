#!/bin/bash
set -e
docker-compose up -d
docker ps
docker cp toil_workflow.py slurm-test_slurmmaster_1:/home/admin
docker cp -L sort.py slurm-test_slurmmaster_1:/home/admin
docker cp fileToSort.txt slurm-test_slurmmaster_1:/home/admin
docker cp toil_workflow.py slurm-test_slurmmaster_1:/home/admin
GIT_COMMIT=$(git rev-parse HEAD)
docker exec slurm-test_slurmmaster_1 sudo apt install python3-pip -y
docker exec slurm-test_slurmmaster_1 pip3 install "git+https://github.com/DataBiosphere/toil.git@${GIT_COMMIT}"
docker exec slurm-test_slurmmaster_1 sinfo -N -l
# Test 1: A really basic workflow to check Slurm is working correctly
docker exec slurm-test_slurmmaster_1 python3 /home/admin/toil_workflow.py file:my-job-store --batchSystem slurm --disableCaching --retryCount 0
docker cp slurm-test_slurmmaster_1:/home/admin/output.txt output_Docker.txt
# Test 2: Make sure that "sort" workflow runs under slurm
docker exec slurm-test_slurmmaster_1 python3 /home/admin/sort.py file:my-job-store --batchSystem slurm --disableCaching --retryCount 0
docker cp slurm-test_slurmmaster_1:/home/admin/sortedFile.txt sortedFile.txt
docker-compose stop
./check_out.sh
rm sort.py
echo "Sucessfully ran workflow on slurm cluster"
