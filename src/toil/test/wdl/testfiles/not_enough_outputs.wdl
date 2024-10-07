version 1.0

workflow wf {
    input {
    }

    call do_math {
    input:
        number = 3
    }

    Int should_never_output = do_math.cube - 1

    output {
        Int only_result = do_math.square
    }
}

task do_math {
    input {
        Int number
    }
    
    # Not allowed to not have a command
    command <<<
    >>>

    output {
        Int square = number * number
        Int cube = number * number * number
    }
}

