#!/usr/bin/env bash
while [ ! -f "$1" ]; do sleep 2; done; mesos-slave "${@:2}"
