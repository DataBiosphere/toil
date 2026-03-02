version 1.1

workflow singularity_wf {
    input {
        String leader_sif_path
    }

    call singularity_task {
    input:
        leader_sif_path = leader_sif_path
    }

    output {
        String value = singularity_task.value
    }
}

task singularity_task {
    input {
        String leader_sif_path
    }

    command <<<
        cat /etc/lsb-release | grep DISTRIB_CODENAME | cut -f2 -d'='
    >>>

    output {
        String value = read_string(stdout())
    }

    runtime {
        container: "singularity://" + leader_sif_path
    }
}

