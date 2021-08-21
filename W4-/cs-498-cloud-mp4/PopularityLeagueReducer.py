#!/usr/bin/env python3
import sys

res = []

# input comes from STDIN
for line in sys.stdin:
    pid, count = line.strip().split('\t', 1)
    try:
        pid = int(pid)
        count = int(count)
    except ValueError:
        continue

    res.append([pid, count, None])

# sort by increasing counts
res = sorted(res, key=lambda x: (x[1]))

# assign quick rank
rank = 0
for item in res:
    item[2] = rank
    rank += 1

# correct rank for same ranked items
for i in range(len(res)):
    if i == 0:
        continue
    if res[i][1] == res[i-1][1]:
        res[i][2] = res[i - 1][2]

# sort output by DESC pid
res = sorted(res, key=lambda x: (-x[0]))

for item in res:
    print('%s\t%s' % (item[0], item[2]))
