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
docker cp toil_doubletime_workflow.py ${LEADER}:/home/admin
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

# Run test workflows in parallel
TEST_PIDS=()
# Test 1: A really basic workflow to check Slurm is working correctly
(
    docker exec -e TOIL_CHECK_ENV=True ${LEADER} /home/admin/venv/bin/python /home/admin/toil_workflow.py file:workflow-test --batchSystem slurm --defaultWalltime 120 --disableCaching --retryCount 0 --batchLogsDir ./nonexistent/paths
    docker cp ${LEADER}:/home/admin/output.txt output_Docker.txt
) &
TEST_PIDS+=($!)
# Test 2: Make sure that "sort" workflow runs under slurm
(
    docker exec -e TOIL_CHECK_ENV=True ${LEADER} /home/admin/venv/bin/python /home/admin/sort.py file:sort-test --batchSystem slurm --defaultWalltime 120 --disableCaching --retryCount 0
    docker cp ${LEADER}:/home/admin/sortedFile.txt sortedFile.txt
) &
TEST_PIDS+=($!)
# Test 3: Make sure --doubleTime works
# We need to make sure the default memory and disk fit on the cluster nodes.
(
    docker exec -e TOIL_CHECK_ENV=True ${LEADER} /home/admin/venv/bin/python /home/admin/toil_doubletime_workflow.py file:time-test --batchSystem slurm --doubleTime True --retryCount 1 --disableCaching --defaultMemory 1G --defaultDisk 1G --logFile doubletime_log.txt
    docker cp ${LEADER}:/home/admin/doubletime_output.txt doubletime_output.txt
    docker cp ${LEADER}:/home/admin/doubletime_log.txt doubletime_log.txt
) &
TEST_PIDS+=($!)

for TEST_PID in "${TEST_PIDS[@]}" ; do
    # This should fail the test run if a workflow subshell failed.
    wait "${TEST_PID}"
done

docker compose down -v
./check_out.sh
echo "Sucessfully ran workflow on slurm cluster"
