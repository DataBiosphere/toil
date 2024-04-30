# A Custom Docker Image for Running Toil's CI/CD (currently on Gitlab).
#
# To build this image, run:
#
#   docker login
#   docker build . -t {docker_username}/{tag_key}:{tag_value}
#   docker push {docker_username}/{tag_key}:{tag_value}
#
# For example:
#
#   docker login quay.io
#   docker build . -t quay.io/ucsc_cgl/toil_ci_prebake:latest
#   docker push quay.io/ucsc_cgl/toil_ci_prebake:latest
#
FROM quay.io/vgteam/dind

ENV DEBIAN_FRONTEND=noninteractive

RUN mkdir -p ~/.docker/cli-plugins/
RUN curl -L https://github.com/docker/buildx/releases/download/v0.6.3/buildx-v0.6.3.linux-amd64 > ~/.docker/cli-plugins/docker-buildx
RUN curl -L https://github.com/docker/compose/releases/download/v2.17.2/docker-compose-linux-x86_64 > ~/.docker/cli-plugins/docker-compose
RUN chmod u+x ~/.docker/cli-plugins/*

RUN apt-get -q -y update && \
    apt-get -q -y upgrade && \
    apt-get -q -y install \
        make git software-properties-common build-essential virtualenv libssl-dev libffi-dev \
        libncurses5-dev curl zip unzip cmake wget docker.io python3-pip python3-virtualenv \
        python-dev npm nodejs tzdata jq awscli graphviz && \
    apt-get clean

RUN sudo add-apt-repository -y ppa:deadsnakes/ppa && \
    sudo apt-get update && \
    sudo apt-get install -y \
        python3.8 python3.8-dev python3.8-venv python3.8-distutils \
        python3.9 python3.9-dev python3.9-venv python3.9-distutils \
        python3.10 python3.10-dev python3.10-venv python3.10-distutils \
        python3.11 python3.11-dev python3.11-venv python3.11-distutils \
        python3.12 python3.12-dev python3.12-venv python3.12-distutils

RUN rm -f /usr/bin/python
RUN ln -s /usr/bin/python3.11 /usr/bin/python
RUN curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py && python3.8 get-pip.py && python3.9 get-pip.py && python3.10 get-pip.py && python3.11 get-pip.py && python3.12 get-pip.py # for python 3.12, get-pip seems to remove other pip versions
RUN python3.12 -m pip install virtualenv -U && python3.8 -m pip install virtualenv && python3.9 -m pip install virtualenv && python3.10 -m pip install virtualenv && python3.11 -m pip install virtualenv # python3.12 needs latest version of virtualenv


LABEL maintainers="anovak@soe.ucsc.edu lblauvel@ucsc.edu"
