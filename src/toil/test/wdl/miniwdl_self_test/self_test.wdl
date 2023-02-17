# MiniWDL self-test workflow from https://github.com/chanzuckerberg/miniwdl/blob/ec4cceeb0cd277d2b4633a7fa81a9593c139e2dd/WDL/CLI.py#L1364
version 1.0
workflow hello_caller {
    input {
        File who
    }
    scatter (name in read_lines(who)) {
        call hello {
            input:
                who = write_lines([name])
        }
        if (defined(hello.message)) {
            String msg = read_string(select_first([hello.message]))
        }
    }
    output {
        Array[String] messages = select_all(msg)
        Array[File] message_files = select_all(hello.message)
    }
}
task hello {
    input {
        File who
    }
    command {
        set -x
        if grep -qv ^\# "${who}" ; then
            name="$(cat ${who})"
            mkdir messages
            echo "Hello, $name!" | tee "messages/$name.txt" 1>&2
        fi
    }
    output {
        File? message = select_first(flatten([glob("messages/*.txt"), ["nonexistent"]]))
    }
    runtime {
        docker: "ubuntu:18.04"
        memory: "1G"
    }
}
