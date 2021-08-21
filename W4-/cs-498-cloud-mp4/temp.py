#!/usr/bin/env python3
import sys

des_list = []
src = ''
for line in sys.stdin:
    # TODO
    pp = line.strip().split(':', 1)

    src = pp[0].strip()

    des_list.extend(pp[1].strip().split(' '))
    des_list = [it.strip() for it in des_list if it.strip() != '']

    if src != '':
        print('%s\t%s' % (src, True))
    for it in des_list:
        print('%s\t%s' % (it, False))













#!/usr/bin/env python3
#from operator import itemgetter
import sys

current_pid = None
current_is_orp = True
pid = None

# input comes from STDIN
for line in sys.stdin:
    line = line.strip()

    # parse input from mappers
    pid, is_orp = line.split('\t', 1)

    # convert count (currently a string) to int
    try:
        pid = int(pid)
        is_orp = bool(is_orp)
    except ValueError:
        # count was not a bool, so silently
        # ignore/discard this line
        continue

    # this IF-switch only works because Hadoop sorts map output
    # by key (here: word) before it is passed to the reducer
    if current_pid == pid:
        current_is_orp &= is_orp
    else:
        if current_pid:
            # write result to STDOUT, only print out orphan pages
            if current_is_orp:
                print('%s' % current_pid)
        current_is_orp = is_orp
        current_pid = pid

# do not forget to output the last page ID if needed!
if current_pid == pid:
    if current_is_orp:
        print('%s' % current_pid)