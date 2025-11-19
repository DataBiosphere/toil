#!/bin/bash
set -e
# With the docker compose plugin, containers are named like slurm-test-slurmmaster-1
# If your containers are named like ${LEADER} you have the old docker-compose Python version instead.
# Try running with NAME_SEP=_
NAME_SEP=${CONTAINER_NAME_SEP:--}
LEADER="slurm-test${NAME_SEP}slurmmaster${NAME_SEP}1"
docker compose up -d
docker ps
docker cp toil_workflow.py ${LEADER}:/home/admin
docker cp -L sort.py ${LEADER}:/home/admin
docker cp fileToSort.txt ${LEADER}:/home/admin
docker cp toil_workflow.py ${LEADER}:/home/admin
GIT_COMMIT=$(git rev-parse HEAD)
docker exec ${LEADER} python3.10 -m pip install "git+https://github.com/DataBiosphere/toil.git@${GIT_COMMIT}"
docker exec ${LEADER} sinfo -N -l
# Test 1: A really basic workflow to check Slurm is working correctly
docker exec ${LEADER} python3.10 /home/admin/toil_workflow.py file:my-job-store --batchSystem slurm --disableCaching --retryCount 0 --batchLogsDir ./nonexistent/paths
docker cp ${LEADER}:/home/admin/output.txt output_Docker.txt
# Test 2: Make sure that "sort" workflow runs under slurm
docker exec ${LEADER} python3.10 /home/admin/sort.py file:my-job-store --batchSystem slurm --disableCaching --retryCount 0
docker cp ${LEADER}:/home/admin/sortedFile.txt sortedFile.txt
docker compose stop
./check_out.sh
rm sort.py
echo "Sucessfully ran workflow on slurm cluster"
