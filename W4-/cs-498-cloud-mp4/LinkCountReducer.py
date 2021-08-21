#!/usr/bin/env python3
import sys

current_des = None
current_count = 0
des = None

# input comes from STDIN
for line in sys.stdin:
    line = line.strip()

    # parse input from mappers
    des, count = line.split('\t', 1)

    # convert count (currently a string) to int
    try:
        count = int(count)
    except ValueError:
        # count was not a number, so silently
        # ignore/discard this line
        continue

    # this IF-switch only works because Hadoop sorts map output
    # by key (here: word) before it is passed to the reducer
    if current_des == des:
        current_count += count
    else:
        if current_des:
            # write result to STDOUT
            print('%s\t%s' % (current_des, current_count))
        current_count = count
        current_des = des

# do not forget to output the last word if needed!
if current_des == des:
    print('%s\t%s' % (current_des, current_count))
