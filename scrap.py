with open('/Users/cbseuser/toil/src/toil/test/docs/scripts/out.txt', 'r') as f:
    for line in f:
        if line.startswith('2018'):
            pass
        else:
            print(line.strip())
