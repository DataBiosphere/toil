# syntax=docker/dockerfile:1
FROM agaveapi/gridengine:latest
ENV SGE_ROOT=/usr/share/gridengine/
RUN yum update
RUN yum install python3-pip python3-dev -y
