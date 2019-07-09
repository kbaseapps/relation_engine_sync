import sys


def log(level, msg):
    if level == 'ERROR':
        sys.stderr.write(str(level) + '\t' + str(msg) + '\n')
    else:
        sys.stdout.write(str(level) + '\t' + str(msg) + '\n')
