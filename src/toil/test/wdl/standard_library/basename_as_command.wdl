workflow basenameWorkflow {
    String in_string

    call echo_string {input: in_string=in_string}
    call copy_output { input: in_string=echo_string.out_string }
}

task echo_string {
    String in_string

    command {
        echo "${in_string}"
    }

    output {
        String out_string = read_string(stdout())
    }
}

task copy_output {
    String in_string
    String name = basename(in_string)

    command {
        echo ${name} > output.txt
    }

    output {
        File the_output = 'output.txt'
    }
}
