version 1.0

workflow wf {
    meta {
        # Advertise as needing the Cromwell Output Organizer
        croo_out_def: 'https://storage.googleapis.com/encode-pipeline-output-definition/atac.croo.v5.json'
    }

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

