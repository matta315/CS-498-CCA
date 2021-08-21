#!/usr/bin/env python3
import re
import sys
import string

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

    print('%s\t%s' % (des, count))