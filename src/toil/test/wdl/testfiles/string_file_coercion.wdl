version 1.0

workflow StringFileCoercion {

    input {
        File? opt_file
    }

    if (defined(opt_file)) {
        call UseFile {
            input:
                # Exciting trick from https://github.com/broadinstitute/GATK-for-Microbes/blob/51f60135abd3f61efd0dcbe1e87c9aa89f5fb91c/wdl/shortReads/MicrobialGenomePipeline.wdl#L79C35-L79C44
                input_file = select_first([opt_file, ""])
        }
    }

    output {
        File? output_file = UseFile.output_file
    }
}


task UseFile {
    input {
        File input_file
    }

    command <<<
        cat ~{input_file} | wc -c > output.txt
    >>>

    output {
        File output_file = "output.txt"
    }
}
