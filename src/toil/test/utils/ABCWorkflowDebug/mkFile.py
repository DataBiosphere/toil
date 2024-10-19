from configargparse import ArgumentParser


def main():
    parser = ArgumentParser(description="Creates a file and writes into it.")
    parser.add_argument("file_name", help="File name to be written to.")
    parser.add_argument("contents", help="A string to be written into the file.")

    args, unknown_args = parser.parse_known_args()

    with open(args.file_name, "w") as f:
        f.write(args.contents)


if __name__ == "__main__":
    main()
