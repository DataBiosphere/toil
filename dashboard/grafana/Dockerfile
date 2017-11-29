FROM grafana/grafana

EXPOSE 3000

ENV GF_AUTH_ANONYMOUS_ENABLED "true"


COPY ./toil_dashboard.json /usr/share/grafana/public/dashboards/home.json
