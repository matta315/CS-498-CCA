#!/usr/bin/env python3
#from operator import itemgetter
import sys

word_to_count = {}

# input comes from STDIN
for line in sys.stdin:
    line = line.strip()

    word, count = line.split('\t', 1)
    try:
        count = int(count)
    except ValueError:
        # count was not a number, so silently
        # ignore/discard this line
        continue
    word_to_count[word] = count
    #print(line)

#for w in word_to_count.items():
#    print(w[0], w[1])

sorted_LOW = sorted(word_to_count.items(), key=lambda x: (x[1], x[0]))
sorted_LOW.reverse()

result = [item for item in sorted_LOW[:10]]
result.reverse()

for i in result:
    print('%s\t%s' % (i[0], i[1]))