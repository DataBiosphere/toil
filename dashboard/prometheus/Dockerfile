FROM prom/prometheus:v2.24.1

COPY ./prometheus.yml /etc/prometheus/prometheus.yml
COPY ./run.sh /opt/run.sh

EXPOSE 9090 9090

ENTRYPOINT [ "/opt/run.sh" ]
