# This file is sourced by Jenkins during a CI build for both PRs and master/release branches.
# A PR may *temporarily* modify this file but a PR will only be merged if this file is identical
# between the PR branch and the target branch. The make_targets variable will contain a space-
# separated list of Makefile targets to invoke.

# Passing --system-site-packages ensures that mesos.native and mesos.interface are included
virtualenv --system-site-packages venv
. venv/bin/activate
pip2.7 install sphinx
make develop extras=[aws,mesos,azure,encryption,cwl]
export LIBPROCESS_IP=127.0.0.1
export PYTEST_ADDOPTS="--junitxml=test-report.xml"
export make_targets

bash -ex <<-"END"
    # If there is an output directory, delete it.  Furthermore, if the directory is a mountpoint
    # then it must be on the loopback filesystem and needs to be unmounted.  Lastly, delete the
    # image used for the loopback system.
    cleanup() {
        trap - EXIT
        # Delete the temp dir
        if [ -d /mnt/ephemeral/tmp ]
        then
            if mountpoint -q /mnt/ephemeral/tmp
            then
                # If there are processes still interacting with the device, kill them before
                # unmounting
                lsof +D /mnt/ephemeral/tmp -t | xargs -r -n1 kill
                sudo umount /dev/loop0
            fi
        fi
        rm -rf /mnt/ephemeral/tmp*
    }

    # We place the workdir into a loopback file system to isolate it from the rest of the system.
    # The size of the loopback device is large enough to run tests that don't specify a value for
    # disk in jobs (default=2G)
    cleanup
    dd if=/dev/zero of=/mnt/ephemeral/tmp.img bs=1M count=10240
    mkfs.ext4 -F -E root_owner=$UID:$UID /mnt/ephemeral/tmp.img
    mkdir /mnt/ephemeral/tmp && sudo mount -o loop /mnt/ephemeral/tmp.img /mnt/ephemeral/tmp

    # Run cleanup on exit, and any SIGINT (used if a jenkins build is aborted by the user or by
    # jenkins if the build times out).  SIGKILL and SIGTERM might be overkill but hey, better safe
    # than sorry.
    trap cleanup EXIT

    export TMPDIR=/mnt/ephemeral/tmp
    make $make_targets
END
