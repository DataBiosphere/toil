#!/bin/bash
set -ex
# With the docker compose plugin, containers are named like slurm-test-slurmmaster-1
# If your containers are named like ${LEADER} you have the old docker-compose Python version instead.
# Try running with NAME_SEP=_
NAME_SEP=${CONTAINER_NAME_SEP:--}
LEADER="slurm-test${NAME_SEP}slurmmaster${NAME_SEP}1"
docker compose up -d
docker compose ps
docker cp toil_workflow.py ${LEADER}:/home/admin
docker cp -L sort.py ${LEADER}:/home/admin
docker cp fileToSort.txt ${LEADER}:/home/admin
docker cp toil_workflow.py ${LEADER}:/home/admin
GIT_COMMIT=$(git rev-parse HEAD)
# The base cluster image doesn't ship a working venv, or git
docker exec -e DEBIAN_FRONTEND=noninteractive ${LEADER} sudo apt-get update
docker exec -e DEBIAN_FRONTEND=noninteractive ${LEADER} sudo apt-get -qq -y install python3-venv git >/dev/null
docker exec ${LEADER} python3.12 -m venv /home/admin/venv
docker exec ${LEADER} /home/admin/venv/bin/python -m pip install "git+https://github.com/DataBiosphere/toil.git@${GIT_COMMIT}"
# This can sometimes report:
#   slurm_load_partitions: Unexpected message received
# In that case we need to wait and try again.
DELAY=1
docker exec ${LEADER} sinfo -N -l && STATUS=0 || STATUS="${?}"
while [[ "${STATUS}" != "0" && "${LOOP_COUNT}" != "10" ]] ; do
    echo "Waiting for Slurm to be up"
    sleep "${DELAY}"
    docker exec ${LEADER} sinfo -N -l && STATUS=0 || STATUS="${?}"
    ((LOOP_COUNT+=1))
    ((DELAY+=DELAY))
done
if [[ "${STATUS}" != "0" ]] ; then
    echo >&2 "Could not get Slurm info; did Slurm start successfully?"
    exit 1
fi
# Test 1: A really basic workflow to check Slurm is working correctly
docker exec -e TOIL_CHECK_ENV=True ${LEADER} /home/admin/venv/bin/python /home/admin/toil_workflow.py file:my-job-store --batchSystem slurm --slurmTime 2:00 --disableCaching --retryCount 0 --batchLogsDir ./nonexistent/paths
docker cp ${LEADER}:/home/admin/output.txt output_Docker.txt
# Test 2: Make sure that "sort" workflow runs under slurm
docker exec -e TOIL_CHECK_ENV=True ${LEADER} /home/admin/venv/bin/python /home/admin/sort.py file:my-job-store --batchSystem slurm --slurmTime 2:00 --disableCaching --retryCount 0
docker cp ${LEADER}:/home/admin/sortedFile.txt sortedFile.txt
docker compose down -v
./check_out.sh
echo "Sucessfully ran workflow on slurm cluster"
