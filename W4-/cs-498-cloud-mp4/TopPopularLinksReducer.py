#!/usr/bin/env python3
#from operator import itemgetter
import sys

des_to_count = {}

# input comes from STDIN
for line in sys.stdin:
    line = line.strip()

    des, count = line.split('\t', 1)
    try:
        des = int(des)
        count = int(count)
    except ValueError:
        # count was not a number, so silently
        # ignore/discard this line
        continue
    des_to_count[des] = count

sorted_LOW = sorted(des_to_count.items(), key=lambda x: (x[1], x[0]))
sorted_LOW.reverse()

result = [item for item in sorted_LOW[:10]]
result.reverse()

for i in result:
    print('%s\t%s' % (i[0], i[1]))
