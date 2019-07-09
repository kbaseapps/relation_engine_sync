import sys


def log(level, msg):
    if level == 'ERROR':
        sys.stderr.write(level + '\t' + msg + '\n')
    else:
        sys.stdout.write(level + '\t' + msg + '\n')
