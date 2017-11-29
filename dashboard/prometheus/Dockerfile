FROM prom/prometheus

COPY ./prometheus.yml /etc/prometheus/prometheus.yml
COPY ./run.sh /opt/run.sh

EXPOSE 9090 9090

ENTRYPOINT [ "/opt/run.sh" ]
