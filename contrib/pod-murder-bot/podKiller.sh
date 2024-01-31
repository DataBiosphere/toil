#!/bin/sh
NAMESPACE="toil"

CUTOFF=$(date -u -Iseconds --date='10 hours ago' | sed 's/+00:00/Z/' )

kubectl get po --namespace $NAMESPACE -o json | jq -r '.items[] | [.metadata.name, .status.startTime] | @tsv' | while IFS= read -r line
do
	NAME=$(echo $line | cut -d' ' -f 1)
	TIMESTAMP=$(echo $line | cut -d' ' -f 2)
	
	if [[ "$TIMESTAMP" < "$CUTOFF" ]]; #Timestamp is older than the cutoff
	then
	
		if [[ "$NAME" == "toiltest-toil"* || "$NAME" == "runner-"* ]]; #Check name of job if safe to be deleted
		then
			echo "$NAME"
			#Delete job
			kubectl delete pod $NAME --namespace $NAMESPACE
		fi	
	fi
			
done



	
