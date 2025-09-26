version 1.1

workflow gather {
    input {
    }

    scatter(number in [1, 2, 3]) {
        call task1 {
            input:
                number=number
        }
    }

    call task2 {
        input:
            files = task1.foo
    }

    output {
        File outfile = task2.outfile
    }
}

task task1 {

    input {
        Int number
    }

    command <<<
        echo ~{number} > foo.txt
    >>>

    output {
        File foo = "foo.txt"
    }
}

task task2 {
    input {
        Array[File] files
    }

    command <<<
        cat ~{sep=" " files} >out.txt
    >>>

    output {
        File outfile = "out.txt"
    }
}

