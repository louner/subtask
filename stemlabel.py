#! /usr/bin/env python
import fileinput
import json
from nltk import PorterStemmer
import re

ps = PorterStemmer()
ferr = open('/tmp/stemlabel-err', 'w')

for line in fileinput.input():
    try:
        candQ = json.loads(line)
        tmpstr = candQ['label'].replace('u\'', '"').replace('\'', '"')
        labels = json.loads(tmpstr)
    except ValueError:
        ferr.write(line)
        continue

    VO, nonVO = [], []
    for label, substring in labels.iteritems():
        subtoks = substring.split()
        labels[label] = ' '.join([ps.stem(tok) for tok in subtoks])
        if label[:2] == 'VO':
            toks = labels[label].split()
            try:
                VO.append('{} {}'.format(toks[0], toks[-1]))
            except IndexError:
                ferr.write(line)
                break

        else:
            nonVO.append(labels[label])

    candQ['label'] = labels
    candQ['4search'] = ' '.join(nonVO+VO)
    print json.dumps(candQ)
