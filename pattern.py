from parameter import *
import sys
sys.path = [STANFORDPARSERPATH] + sys.path

from parser import Parser
from pymongo import MongoClient
import jpype
from gaia import FileReader

from collections import defaultdict, Counter
from time import time

import json
import pdb
import re
import copy

dependencyOrder = {   'amod': 2,
        'dobj': 5,
        'nn':   1,
        'det':  3,
        'npadvmod': 4
        }

dependenciesLabel = {   'amod': 'MN',
        'dobj': 'VO',
        'nn':   'MN',
        'det':  'MN',
        'npadvmod': 'VO'
        }

def toSortDependency(dep):
    if dep[0] in dependencyOrder:
        return dependencyOrder[dep[0]]
    else:
        return 6

class PatternTransformer:
    def __init__(self):
        self.p = Parser()

    def transformToPattern(self, query):
        try:
            dependencies, posTags, queryToks = self.p.parseToStanfordDependencies(query)
        except:
            print 'pattern transformer: parse error, {}'.format(query)
            return '', '', ''

        dependencies = sorted(dependencies, key=toSortDependency)

        labelCount = defaultdict(lambda: 0, {})
        labelSubstrings =  {}

        self.deriveSNVLabels(posTags, labelCount, labelSubstrings, queryToks)

        self.deriveVOMNLabels(dependencies, labelCount, labelSubstrings, queryToks)

        pattern = ' '.join([tok for tok in queryToks if tok])
        labelSubstrings2 = {}
        for tok in queryToks:
            if tok in labelSubstrings:
                labelSubstrings2[tok] = labelSubstrings[tok]

        return pattern, labelSubstrings2, query

    def deriveSNVLabels(self, posTags, labelCount, labelSubstrings, queryToks):

        for index, tag in posTags.iteritems():
            if tag[0].lower() == 'v':
                label = 'SV'
            elif tag[0].lower() == 'n':
                label = 'SN'

            else:
                continue
            try:
                substring = queryToks[index]
            except:
                queryToks = []
                return
        
            labelCount[label] += 1
            label = label + str(labelCount[label])
            labelSubstrings[label] = substring

            queryToks[index] = label

    def deriveVOMNLabels(self, dependencies, labelCount, labelSubstrings, queryToks):
        for dep in dependencies:
            if dep[0] in dependenciesLabel:
                for i in range(dep[1], dep[2]+1):
                    if queryToks[i] and queryToks[i] in labelSubstrings:
                        queryToks[i] = labelSubstrings[queryToks[i]]
                substring = ' '.join([tok for tok in queryToks[dep[1]:dep[2]+1] if tok])
                if not substring:
                    continue

                labelCount[dep[0]] += 1
                label = dependenciesLabel[dep[0]] + str(labelCount[dep[0]])
                labelSubstrings[label] = substring
                queryToks[dep[1]:dep[2]+1] = [label] + ['']*(dep[2]-dep[1])

def findSubtaskByPatternsFromQuery(query='lose weight'):
    patterns = Patterns()
    candidateQueries = findCandidateQueries(query)
    subtasks = patterns.findSubtasks(candidateQueries)
    return subtasks

#testFindSubtaskByPatternsFromQueries()

class CandidateQuery:
    def __init__(self, data):
        self.pattern = data['pattern']
        self.label = data['label']
        self.query = data['query']

class Patterns:
    def __init__(self, fname=PATTERNFILE, mostN=20):
        self.patterns = self.loadPatterns(fname)
        self.mostN = mostN

    def loadPatterns(self, fname):
        fin = open(fname, 'r')
        patterns = [Pattern(json.loads(line)) for line in fin]
        fin.close()
        return patterns

    def findSubtasks(self, candidateQueries):
        subtasks = Counter()
        for pattern in self.patterns:
            subtasks += pattern.findSubtasks(candidateQueries)

        return [element[0] for element in subtasks.most_common(self.mostN)]

class Pattern:
    def __init__(self, data=('T by SUBT', 1)):
        toks = data[0].split()
        self.weight = data[1]

        toks[toks.index('T')] = '(?P<T>[\w]+)'
        toks[toks.index('SUBT')] = '(?P<SUBT>[\w]+)'
        self.pat = re.compile(r'{}'.format(' '.join(toks)))

    def findSubtasks(self, candidateQueries):
        count = Counter()
        for candq in candidateQueries:
            if self.match(candq):
                 count[candq.label[self.subtaskLabel]] += self.weight
        return count

    def match(self, candq):
        matches = self.pat.finditer(candq.pattern)
        if matches:
            for match in matches:
                if match.group('T')[:2] == 'VO' and match.group('SUBT') in candq.label:
                    self.subtaskLabel = match.group('SUBT')
                    return True
        return False

def findCandidateQueries(query):
    mc = MongoClient()
    db = mc['query']
    collectionName = 'query'

    results = db.command('text', collectionName, search=query, project={'pattern': 1, 'label': 1, 'query': 1, '_id': 0}, limit=1000000)
    candidateQueries = [CandidateQuery(result['obj']) for result in results['results']]
    return candidateQueries
