version 1.0

workflow wait {
    input {
    }

    call waiter_task {
    input:
    }

    output {
        String result = read_lines(waiter_task.out)[0]
    }
}

task waiter_task {
    input {
    }

    command <<<
        sleep 10 &
        sleep 2 &
        wait
        echo "waited"
    >>>

    output {
        File out = stdout()
    }

    runtime {
        docker: "ubuntu:22.04"
    }
}
