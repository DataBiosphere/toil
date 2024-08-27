version 1.0

workflow url_to_file {
    input {
    }

    File the_file = "https://hgdownload.soe.ucsc.edu/goldenPath/hs1/bigZips/hs1.chrom.sizes"

    output {
        File out_file = the_file
        String first_line = read_lines(the_file)[0]
    }
}
