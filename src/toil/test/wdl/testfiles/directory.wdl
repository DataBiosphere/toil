version development

workflow directory {
    input {
    }
    call make_dir {
        input:
    }
    call list_dir {
        input:
            in_dir = make_dir.out_dir
    }
    output {
        Directory out_dir = make_dir.out_dir
        File out_log = list_dir.out_log
    }
}

task make_dir {
    input {
    }

    command <<<
        mkdir testdir
        echo "Hello" > testdir/a.txt
        mkdir testdir/subdir
        echo "World" > testdir/subdir/b.txt
    >>>

    output {
        Directory out_dir = "testdir"
    }

    runtime {
        container: "ubuntu:24.04"
    }
}

task list_dir {
    input {
        Directory in_dir
    }

    command <<<
        find ~{in_dir}
    >>>

    output {
        File out_log = stdout()
    }

    runtime {
        container: "ubuntu:24.04"
    }
}
