#!/bin/bash

# Wrapper for Singularity that rewrites docker:// image specifiers that point to Docker Hub to use a registry mirror instead.
# Obeys SINGULARITY_DOCKER_HUB_MIRROR for the URL, including http:// or https:// protocol.

set -e

# Where is the real Singularity binary?
# The Dockerfile moves it from /usr/local/bin/singularity to here after installation
SINGULARITY_PATH=/usr/local/libexec/toil/singularity-real

# Read replacements from the environemnt
# TODO: really do that
MIRROR_HOST=""
MIRROR_HTTP=0

if [[ ! -z "${SINGULARITY_DOCKER_HUB_MIRROR}" ]] ; then
    MIRROR_HOST="${SINGULARITY_DOCKER_HUB_MIRROR##*://}"
    MIRROR_PROTO="${SINGULARITY_DOCKER_HUB_MIRROR%%://*}"
    if [[ "${MIRROR_PROTO}" == "http" ]] ; then
        MIRROR_HTTP=1
    fi
fi

# Collect command line arguments
ARGC=$((${#} + 1))
ARGV=($0 "${@}")

if [[ "${ARGC}" -ge "2" && "${ARGV[1]}" == "pull" && ! -z "${MIRROR_HOST}" ]] ; then
    # We are doing a pull

    # We will set this if we manage to replace a Docker name
    REPLACED=0

    INDEX=2
    while [[ "${INDEX}" -lt "${ARGC}" ]] ; do
        # For each argument other than the script name
        if [[ "${ARGV[$INDEX]}" == docker://* ]] ; then
            # If it doesn't have a / after the protocol, it needs "library/" inserted
            NEW_SPEC="$(echo "${ARGV[$INDEX]}" | sed 's!^docker://\([^/][^/]*$\)!docker://library/\1!')"

            # If it doesn't have a hostname with a dot before the first /, give it our hostname
            NEW_SPEC="$(echo "${NEW_SPEC}" | sed 's!^docker://\([^.][^.]*/\)!docker://'${MIRROR_HOST}'/\1!')"

            # Replace array item
            ARGV[$INDEX]="${NEW_SPEC}"
            REPLACED=1
        fi

        let INDEX+=1
    done

    # We will set this if we need to insert --nohttps for an insecure registry
    HTTP_ARG=""

    if [[ "${REPLACED}" == "1" && "${MIRROR_HTTP}" == "1" ]] ; then
        # We need to use HTTP and not HTTPS for the mirror, so we need to isnert the argument
        HTTP_ARG="--nohttps"
    fi

    # Run the pull with our extra args, and then all the args starting at 2
    "${SINGULARITY_PATH}" pull ${HTTP_ARG} "${ARGV[@]:2}"
else
    # Pass along all the args except the program name
    "${SINGULARITY_PATH}" "${ARGV[@]:1}"
fi


