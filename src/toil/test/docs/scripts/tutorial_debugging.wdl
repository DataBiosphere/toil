version 1.1

workflow TutorialDebugging {

    input {
        String message = "Uh-oh!"
    }

    call WhaleSay {
        input:
            message = message
    }

    call CountLines {
        input:
            to_count = WhaleSay.result
    }

    call PlotChart {
        input:
            numbers = CountLines.result
    }

    output {
        File image = PlotChart.image
        File numbers = CountLines.result
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

# Plot a bar chart
task PlotChart {
    input {
        File numbers
    }

    command <<<
        set -ex
        python3 <<'EOF'
        import matplotlib
        matplotlib.use('Agg')
        from matplotlib import pyplot
        data = []
        for line in open("~{numbers}"):
            line = line.strip()
            if line != "":
                data.append(float(line))
        pyplot.bar(list(range(len(data))), data)
        pyplot.title("Bar Chart")
        pyplot.save("plot.png")
        EOF
    >>>

    output {
        File image = "plot.png"
    }

    runtime {
        container: "opencadc/matplotlib"
    }
}
