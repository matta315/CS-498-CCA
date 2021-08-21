#!/usr/bin/env python3
import sys

leaguePath = sys.argv[1]

with open(leaguePath) as f:
    content = f.readlines()
    league_set = set([pid.strip() for pid in content if pid.strip() != ''])

for line in sys.stdin:
    # parse input from mappers
    pid, count = line.strip().split('\t', 1)
    if pid not in league_set:
        continue

    # convert count (currently a string) to int
    try:
        count = int(count)
    except ValueError:
        # count was not a number, so silently
        # ignore/discard this line
        continue

    print('%s\t%s' % (pid, count))
