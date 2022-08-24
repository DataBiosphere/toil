#!/bin/bash
set -e
docker-compose up -d
docker ps
docker cp toil_workflow.py slurm-test_slurmmaster_1:/home/admin
#Assumes sort.py copied to this directory during gitlab run for ease of access
docker cp sort.py slurm-test_slurmmaster_1:/home/admin
docker cp fileToSort.py slurm-test_slurmmaster_1:/home/admin
docker cp toil_workflow.py slurm-test_slurmmaster_1:/home/admin
GIT_COMMIT=$(git rev-parse HEAD)
docker exec slurm-test_slurmmaster_1 sudo apt install python3-pip -y
docker exec slurm-test_slurmmaster_1 pip3 install "git+https://github.com/DataBiosphere/toil.git@${GIT_COMMIT}"
docker exec slurm-test_slurmmaster_1 sinfo -N -l
docker exec slurm-test_slurmmaster_1 python3 /home/admin/toil_workflow.py file:my-job-store --batchSystem slurm --disableCaching --retryCount 0
docker cp slurm-test_slurmmaster_1:/home/admin/output.txt output_Docker.txt
docker exec slurm-test_slurmmaster_1 python3 /home/admin/sort.py file:my-job-store --batchSystem slurm --disableCaching --retryCount 0
docker cp slurm-test_slurmmaster_1:/home/admin/sortedFile.txt sortedFile.txt
docker-compose stop
./check_out.sh
rm sort.py
echo "Sucessfully ran workflow on slurm cluster"
