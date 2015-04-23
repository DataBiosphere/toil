from jobTree.scriptTree.stack import Stack
from optparse import OptionParser
from long_test import long_test

def main():
    # Boilerplate -- startJobTree requires options
    # sys.argv.append()
    parser = OptionParser()
    Stack.addJobTreeOptions(parser)
    options, args = parser.parse_args()

    # Setup the job stack and launch jobTree job
    i = Stack(long_test()).startJobTree(options)

if __name__ == '__main__':
    main()