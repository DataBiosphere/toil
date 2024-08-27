version 1.0

workflow url_to_optional_file {
    input {
        Int http_code = 404
    }
    
    File? the_file = "https://httpstat.us/" + http_code

    output {
        File? out_file = the_file
    }
}
