version 1.0

workflow url_to_optional_file {
    input {
        Int http_code = 404
        String base_url = "https://httpstat.us/"
    }
    
    File? the_file = base_url + http_code

    output {
        File? out_file = the_file
    }
}
