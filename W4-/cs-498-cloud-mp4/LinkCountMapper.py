#!/usr/bin/env python3
import sys

for line in sys.stdin:
    src, des_str = line.strip().split(':', 1)

    des_list = des_str.strip().split(' ')

    des_set = set([l.strip() for l in des_list if l.strip() != ''])

    if src in des_set:
        des_set.remove(src)

    for des in des_set:
        print('%s\t%s' % (des, 1))
