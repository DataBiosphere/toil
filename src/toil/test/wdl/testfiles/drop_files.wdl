version 1.0

import "drop_files_subworkflow.wdl" as subwf

# Workflow for testing discarding un-output files at the end of a workflow.
# Does a bunch of unusual things with files.
workflow wf {
    input {
        File file_in
        File? file_in_default = "localfile.txt"
    }

    call make_file {
    input:
        number = 3,
        needed_only_for_task = write_lines(["This file is consumed by a task call"])
    }
    
    File will_discard = write_lines(["This file should be discarded"])
    File will_keep = write_lines(["This file should be kept"])
    File will_keep_because = will_keep

    call subwf.subwf {
    input:
        kept_file = write_lines(["This file is kept by a subworkflow"]),
        # Even though we pass will_keep to the subworkflow and it doesn't output it, it should not get deleted.
        dropped_files = [write_lines(["This file is dropped by a subworkflow"]), will_keep]
    }

    File will_remember_one_of = select_first([write_lines(["This file gets stored in a variable"]), write_lines(["This file never gets stored in a variable"])])

    output {
        Array[File] files_kept = [make_file.out_file_1, make_file.out_file_2, will_keep_because, subwf.keep]
    }
}

task make_file {
    input {
        Int number = 5
        File needed_only_for_task
        File file_with_default = write_lines(["This file is created in a task inputs section"])
    }
    
    command <<<
        echo ~{number}
        echo "This file is collected as a task output twice" >testfile.txt
        echo "This task file is not used" >badfile.txt
    >>>

    runtime {
        runtime_file: write_lines(["This file is created in a runtime section"])
    }

    output {
        File out_file_1 = stdout()
        File out_file_2 = "testfile.txt"
        File out_file_3 = "testfile.txt"
        File out_file_4 = write_lines(["Again", "~{number}"])
        File out_file_5 = "badfile.txt"
    }
}

