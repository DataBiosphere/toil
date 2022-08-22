#!/bin/bash
set -e
docker-compose up -d
docker ps
docker cp toilScript.py slurm-test_slurmmaster_1:/home/admin
#docker cp test_script.sh slurm-test_slurmmaster_1:/tmp
GIT_COMMIT=$(git rev-parse HEAD)
docker exec slurm-test_slurmmaster_1 sudo apt install python3-pip -y
docker exec slurm-test_slurmmaster_1 pip3 install "git+https://github.com/DataBiosphere/toil.git@${GIT_COMMIT}"
docker exec slurm-test_slurmmaster_1 sinfo -N -l
docker exec slurm-test_slurmmaster_1 rm -r my-job-store
docker exec slurm-test_slurmmaster_1 python3 /home/admin/toilScript.py file:my-job-store --batchSystem slurm --disableCaching --retryCount 0
docker cp slurm-test_slurmmaster_1:/home/admin/output.txt output_Docker.txt
docker-compose stop

#Outp