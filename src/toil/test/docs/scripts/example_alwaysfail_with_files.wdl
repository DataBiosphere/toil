version 1.0
workflow test {
    call make_file as f1
    call make_file as f2
    call hello {
        input:
            name_file=f1.out,
            unused_file=f2.out
    }
}
task make_file {
    input {
    }
    command <<<
        echo "These are the contents" >test.txt
    >>>
    output {
        File out = "test.txt"
    }
}
task hello {
    input {
        File name_file
        File? unused_file
    }
    command <<<
        set -e
        echoo "Hello" "$(cat ~{name_file})"
    >>>
    output {
        File out = stdout()
    }
}
