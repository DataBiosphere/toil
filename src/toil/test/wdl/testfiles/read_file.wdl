version 1.0

# Workflow to read a file from a string path

workflow read_file {

    input {
        String input_string
    }

    Array[String] the_lines = read_lines(input_string)

    output {
        Array[String] lines = the_lines
    }

}
