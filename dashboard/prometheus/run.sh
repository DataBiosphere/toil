#!/bin/sh


#Set cluster name

sed -i "s/^\([[:blank:]]*\) regex: CLUSTER_NAME/\1 regex: \'${1}\'/g" /etc/prometheus/prometheus.yml

exec /bin/prometheus --config.file=/etc/prometheus/prometheus.yml --storage.tsdb.path=/prometheus --web.console.libraries=/usr/share/prometheus/console_libraries --web.console.templates=/usr/share/prometheus/consoles
