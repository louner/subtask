from parameter import *
import sys
sys.path = [STANFORDPARSERPATH] + sys.path

from parser import Parser
from pymongo import MongoClient
import jpype
from gaia import FileReader

from collections import defaultdict
from time import time

import json

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
        dependencies, posTags = self.p.parseToStanfordDependencies(query)
        dependencies = sorted(dependencies, key=toSortDependency)

        labelCount = defaultdict(lambda: 1, {})
        labelSubstrings =  {}
        queryToks = query.split(' ')

        for index, tag in posTags.iteritems():
            if tag[0].lower() == 'v':
                tmp = 'SV'
            elif tag[0].lower() == 'n':
                tmp = 'SN'

            else:
                continue
            try:
                substring = queryToks[index]
            except:
                return '', '', ''

        
            label = tmp + str(labelCount[tmp])
            labelSubstrings[label] = substring
            labelCount[tmp] += 1

            queryToks[index] = label

        for dep in dependencies:
            if dep[0] in dependenciesLabel:
                for i in range(dep[1], dep[2]+1):
                    if queryToks[i] and queryToks[i] in labelSubstrings:
                        queryToks[i] = labelSubstrings[queryToks[i]]
                substring = ' '.join([tok for tok in queryToks[dep[1]:dep[2]+1] if tok])
                if not substring:
                    continue

                label = dependenciesLabel[dep[0]] + str(labelCount[dep[0]])
                labelSubstrings[label] = substring
                labelCount[dep[0]] += 1
                queryToks[dep[1]:dep[2]+1] = [label] + ['']*(dep[2]-dep[1])

        pattern = ' '.join([tok for tok in queryToks if tok])
        labelSubstrings2 = {}
        for tok in queryToks:
            if tok in labelSubstrings:
                labelSubstrings2[tok] = labelSubstrings[tok]

        return pattern, labelSubstrings2, query

#print PatternTransformer().transformToPattern('hot naked girls sucking fucking dogs')

class TransformQueriesToPattern(FileReader):
    def __init__(self, fname):
        FileReader.__init__(self, fname)
        self.pt = PatternTransformer()
        self.st = time()
        self.fout = open(DATADIR+'patterns', 'w')

    def processLine(self, line):
        try:
            pattern, labelSubstrings, query = self.pt.transformToPattern(line)
        except:
            print line
            return

        if pattern and labelSubstrings:
            onePattern = {}
            onePattern['pattern'] = pattern
            onePattern['label'] = labelSubstrings
            onePattern['query'] = query
            json.dump(onePattern, self.fout)
            self.fout.write('\n')

        if self.lineNum % 10000 == 0:
            print time() - self.st

    def finalize(self):
        self.fout.close()

#print PatternTransformer().transformToPattern('I want to lose weight and eat red meat and sing')
tqtp = TransformQueriesToPattern(QUERIESFILE)
tqtp.readLines()

def foo(fname, coreNumber, threadID):
    tqtp = TransformQueriesToPatternMultiThread(fname, coreNumber, threadID)
    tqtp.readLines()

class TransformQueriesToPatternMultiThread(TransformQueriesToPattern):
    def __init__(self, fname, coreNumber, threadID):
        TransformQueriesToPattern.__init__(self, fname)
        self.coreNumber = coreNumber
        self.threadID = threadID

        self.fout.close()
        self.fout = open(DATADIR+'patterns.'+str(coreNumber), 'w')

    def processLine(self, line):
        if self.lineNum % self.coreNumber == self.threadID:
            print line
            TransformQueriesToPattern.processLine(self, line)

    @staticmethod
    def transform(fname, coreNumber):
        from multiprocessing import Process

        processes = [0]*coreNumber
        for i in range(coreNumber):
            processes[i] = Process(target=foo, args=(fname, coreNumber, i, ))
            processes[i].start()

        for i in range(coreNumber):
            processes[i].join()

        import os

        for i in range(coreNumber):
            os.system('cat '+DATADIR+'patterns.'+str(i)+'>> '+DATADIR+'patterns.all')

#TransformQueriesToPatternMultiThread.transform(QUERIESFILE, 8)

def testFindSubtaskByPatternsFromQueries():
    query = 'lose weight'
    patterns = Patterns(DATADIR+'100Patterns')
    candidateQueries = findCandidateQueries(query)

    subtasks = patterns.findSubtasks(candidateQueries)
    for st in subtasks[:10]:
        print st

class CandidateQuery:
    def __init__(self, data):
        self.queryPattern = data['pattern']
        self.labelSubstrings = data['label']
        self.query = data['query']
        self.toks = self.queryPattern.split(' ')

class Patterns:
    def __init__(self, fname):
        self.patterns = self.loadPatterns(fname)

    def loadPatterns(self, fname):
        fin = open(fname, 'r')
        pats = json.load(fin)
        patterns = []
        for pat in pats:
            patterns.append(Pattern(pat))

        fin.close()
        return patterns

    def findSubtasks(self, candidateQueries):
        subtasks = []
        for pattern in self.patterns:
            subtasks += pattern.findSubtasks(candidateQueries)

        return list(set(subtasks))

class Pattern:
    def __init__(self, data):
        self.text = data[0]
        self.toks = self.text.split(' ')
        self.subtaskLabel = data[1]
        self.TIndex = self.toks.index('T')

    def findSubtasks(self, candidateQueries):
        subtasks = []
        for cq in candidateQueries:
            if self.match(cq):
                subtasks.append(cq.labelSubstrings[self.subtaskLabel])

        return subtasks

    def match(self, candidateQuery):
        if candidateQuery.toks[self.TIndex][:2] in ['VO', 'SV']:
            tmp = candidateQuery.toks[:]
            tmp[self.TIndex] = 'T'
            return self.text in ' '.join(tmp)
        return False

def findCandidateQueries(query):
    mc = MongoClient()
    db = mc['query']
    collectionName = 'candQ'

    results = db.command('text', collectionName, search=query, project={'pattern': 1, 'label': 1, 'query': 1}, limit=1000000)
    results = [result['obj'] for result in results['results']]
    
    candidateQueries = [0]*len(results)
    for i in range(len(results)):
        candidateQueries[i] = CandidateQuery(results[i])

    return candidateQueries

#testFindSubtaskByPatternsFromQueries()
