#!/usr/bin/env python3
import re
import sys
import string

stopWordsPath = sys.argv[1]
delimitersPath = sys.argv[2]

# Store Stopword from stopword path to a list
with open(stopWordsPath) as f:
    stop_word_content = f.readlines()
    stop_word_list = [word.lower().strip() for word in stop_word_content]
    #print(stop_word_list)

# Store delimiters
with open(delimitersPath) as f:
    delimiters = f.read().strip()
    #print(delimiters)

# Contruct regular expression to split word later
sepr_list = [" "]
for c in delimiters:
    sepr_list.append(c)
reg_exp = '|'.join(map(re.escape, sepr_list))

# Removing word
for line in sys.stdin:
    # Split using delimiters:
    words = re.split(reg_exp, line.strip())

    # Change all token to lower case
    words = [w.lower().strip() for w in words]

    # Remove word from Stopword
    words = list(filter(lambda w: w not in stop_word_list, words))

    # Remove empty word:
    words = [w for w in words if w != ""]

    for word in words:
        print('%s\t%s' % (word, 1))