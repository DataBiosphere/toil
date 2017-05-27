#!/bin/sh

#Set IP address of the toil leader
sed -i "s/^\([[:blank:]]*\)\- targets: LEADER_IP/\1\- targets: [\'${1}\']/g" /etc/prometheus/prometheus.yml

#Set cluster name

sed -i "s/^\([[:blank:]]*\) regex: CLUSTER_NAME/\1 regex: \'${2}\'/g" /etc/prometheus/prometheus.yml

exec /bin/prometheus -config.file=/etc/prometheus/prometheus.yml -storage.local.path=/prometheus -web.console.libraries=/usr/share/prometheus/console_libraries -web.console.templates=/usr/share/prometheus/consoles
