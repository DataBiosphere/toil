version 1.0

workflow random {
    input {
        Int wf_input = 1
        Int task_1_input = 1
        Int task_2_input = 1
    }

    call dice_task {
    input:
        task_input=task_1_input
    }

    call read_task {
    input:
        to_read=dice_task.out,
        task_input=task_2_input
    }

    output {
        String value_seen = read_task.read
        String value_written = read_task.wrote
    }
}

task dice_task {
    input {
        Int task_input
    }

    command <<<
        cat /proc/sys/kernel/random/uuid
    >>>

    output {
        File out = stdout()
    }

    runtime {
        docker: "ubuntu:24.04"
    }
}

task read_task {
    input {
        File to_read
        Int task_input
    }

    String value_read = read_lines(to_read)[0]

    command <<<
        echo "~{value_read} $(cat /proc/sys/kernel/random/uuid)"
    >>>

    output {
        String read = value_read
        String wrote = read_lines(stdout())[0]
    }

    runtime {
        docker: "ubuntu:24.04"
    }
}

