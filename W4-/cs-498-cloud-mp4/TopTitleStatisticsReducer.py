#!/usr/bin/env python3
import sys

l = []
list_of_stat = []
list_of_stat_desc = ['Mean', 'Sum', 'Min', 'Max', 'Var']
word = None

for line in sys.stdin:
    word, count = line.split('\t', 1)

    # convert count (currently a string) to int
    try:
        count = int(count)
    except ValueError:
        # count was not a number, so silently
        # ignore/discard this line
        continue

    l.append(count)

mean = sum(l)// len(l)
var = sum((i - mean) ** 2 for i in l) / len(l)

list_of_stat.append(mean)
list_of_stat.append(sum(l))
list_of_stat.append(min(l))
list_of_stat.append(max(l))
list_of_stat.append(int(var))

result = list(zip(list_of_stat_desc, list_of_stat))

[print('%s\t%s' % (item[0], item[1])) for item in result]
#print as final output