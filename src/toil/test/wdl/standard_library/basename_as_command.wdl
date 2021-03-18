workflow basenameWorkflow {
    String in_string

    call copy_output { input: name=in_string }
}

task copy_output {
    String name

    command {
        echo ${basename(name)} > output.txt
    }

    output {
        File the_output = 'output.txt'
    }
}
