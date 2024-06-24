version 1.1

workflow TutorialDebugging {

    input {
        Array[String] messages = ["Uh-oh!", "Oh dear", "Oops"]
    }

    scatter(message in messages) {

        call WhaleSay {
            input:
                message = message
        }

        call CountLines {
            input:
                to_count = WhaleSay.result
        }
    }

    Array[File] to_compress = flatten([CountLines.result, WhaleSay.result])

    call CompressFiles {
        input:
            files = to_compress
    }

    output {
        File compressed = CompressFiles.result
    }

}

# Draw ASCII art
task WhaleSay {
    input {
        String message
    }

    command <<<
        cowsay "~{message}"
    >>>

    output {
        File result = stdout()
    }

    runtime {
        container: "docker/whalesay"
    }
}

# Count the lines in a file
task CountLines {
    input {
        File to_count
    }

    command <<<
        wc -l ~{to_count}
    >>>

    output {
        File result = stdout()
    }

    runtime {
        container: ["ubuntu:latest", "https://gcr.io/standard-images/ubuntu:latest"]
    }
}

# Compress files into a ZIP
task CompressFiles {
    input {
        Array[File] files
    }

    command <<<
    set -e
    cat >script.py <<'EOF'
    import sys
    from zipfile import ZipFile
    import os

    # Interpret command line arguments
    to_compress = list(reversed(sys.argv[1:]))

    with ZipFile("compressed.zip", "w") as z:
        while to_compress != []:
            # Grab the file to add off the end of the list
            input_filename = to_compress[-1]
            # Now we need to write this to the zip file.
            # What internal filename should we use?
            basename = os.path.basename(input_filename)
            disambiguation_number = 0
            while True:
                target_filename = str(disambiguation_number) + basename
                try:
                    z.getinfo(target_filename)
                except KeyError:
                    # Filename is free
                    break
                # Otherwise try another name
                disambiguation_number += 1
            # Now we can actually make the compressed file
            with z.open(target_filename, 'w') as out_stream:
                with open(input_filename) as in_stream:
                    for line in in_stream:
                        # Prefix each line of text with the original input file
                        # it came from.
                        # Also remember to encode the text as the zip file
                        # stream is in binary mode.
                        out_stream.write(f"{basename}: {line}".encode("utf-8"))
            # Even though we got distracted by zip file manipulation, remember
            # to pop off the file we just did.
            to_compress.pop()
    EOF
    python script.py ~{sep(" ", files)}
    >>>

    output {
        File result = "compressed.zip"
    }

    runtime {
        container: "python:3.11"
    }
}
