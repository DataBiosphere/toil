version 1.0

workflow subwf {
    input {
        File kept_file
        Array[File] dropped_files
    }

    output {
        File keep = kept_file
    }
}

