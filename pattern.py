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

# example: given task 'lose weight', return subtasks
def findSubtaskByPatternsFromQuery(task='lose weight'):
    # load patterns
    patterns = Patterns()

    # by task, find queries that may contain subtasks
    candidateQueries = findCandidateQueries(task)

    # extract subtasks from queries, using patterns
    subtasks = patterns.findSubtasks(candidateQueries)

    return subtasks

def toSortDependency(dep):
    if dep[0] in dependencyOrder:
        return dependencyOrder[dep[0]]
    else:
        return 6

# transform a query to a pattern
class PatternTransformer:
    def __init__(self):
        self.p = Parser()

    # query 'lose weight by herbs' will be transformed to a pattern 'VO1 by SN2'
    # while pattern itself remembers label VO1 means 'lose weight', 'SN2' means herbs
    def transformToPattern(self, query):
        try:
            # parse the query with stanford parser
            dependencies, posTags, queryToks = self.p.parseToStanfordDependencies(query)
        except:
            print 'pattern transformer: parse error, {}'.format(query)
            return '', '', ''

        dependencies = sorted(dependencies, key=toSortDependency)

        labelCount = defaultdict(lambda: 0, {})
        labelSubstrings =  {}

        # derive SN(single noun), SV(single verb) labels
        self.deriveSNVLabels(posTags, labelCount, labelSubstrings, queryToks)

        # derive VO(verb and its direct object), MN(compound noun, like 'oil price') labels
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

# pattern that matches and extracts possible subtask from a query
class Pattern:
    def __init__(self, data=('T by SUBT', 1)):
        toks = data[0].split()
        self.weight = data[1]

        # match and extract by regular expression
        # 'T by SUBT' -> (?P<T>[\w]+) by (?P<SUBT>[\w]+)
        toks[toks.index('T')] = '(?P<T>[\w]+)'
        toks[toks.index('SUBT')] = '(?P<SUBT>[\w]+)'
        self.pat = re.compile(r'{}'.format(' '.join(toks)))

    # for each given query, match and extract subtasks
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

# by querying mongoDB, find queries related with given task
def findCandidateQueries(task):
    mc = MongoClient()
    db = mc['query']
    collectionName = 'query'

    results = db.command('text', collectionName, search=task, project={'pattern': 1, 'label': 1, 'query': 1, '_id': 0}, limit=1000000)
    candidateQueries = [CandidateQuery(result['obj']) for result in results['results']]
    return candidateQueries
