import sys


def log(level, msg):
    print(level, msg)
    print(str(level) + '\t' + str(msg))
    # if level == 'ERROR':
    #     sys.stderr.write(str(level) + '\t' + str(msg) + '\n')
    # else:
    #     sys.stdout.write(str(level) + '\t' + str(msg) + '\n')
