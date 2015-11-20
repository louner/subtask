from pymongo import MongoClient
from nltk.stem.wordnet import WordNetLemmatizer
lemmatizer = WordNetLemmatizer()

from collections import defaultdict, Counter
import os 
from parameter import *
import re
from gaia import *
from pattern import TSUBTAndQueryToPattern
import json
import sys
sys.path = [STANFORDPARSERPATH] + sys.path
from parser import Parser
import pdb

def refineQuery(query):
    return query.lower()

class ImportQueries(FileReader):
    def __init__(self, fname=ADSFILE):
        FileReader.__init__(self, fname)

        self.db = MongoClient().query
        self.collection = db.query

        self.batch = [0]*10000
        self.i = 0

    def processLine(self, line):
        query = line.split('\t')[1]
        query = refineQuery(query)
        self.batch.append({'query': query})
        
        if self.i > 10000:
            self.collection.insert_many(batch)
            self.i = 0

            print db.command('collstats', 'query')

class ExtractQuery(FileReader):
    def __init__(self, fname=ADSFILE):
        FileReader.__init__(self, fname)
        self.fout = open(QUERIESFILE, 'w')
        self.nonCharacterProg = re.compile('[^a-zA-Z0-9\s]')

    def processLine(self, line):
        query = line.split('\t')[1]
        if self.nonCharacterProg.search(query):
            return

        query = refineQuery(query)
        self.fout.write(query+'\n')

class RefineTaskSubtask(FileReader):
    def __init__(self, fname=PATTERNFILE):
        FileReader.__init__(self, fname)
        self.fout = open(PATTERNFILE+'.refined', 'w')
        self.p = Parser()

    def processLine(self, line):
        tsparis = json.loads(line)
        for task, subtasks in tsparis.iteritems():
            task = self.refineTask(task)
            taskVO = self.getVOInTask(task)
            if not taskVO:
                continue

            for subtask in subtasks:
                if not subtask:
                    continue
                subtask = self.refineSubtask(subtask)
                subtaskVNs = self.getVNInSubtask(subtask)
                if not subtaskVNs:
                    continue
                
                self.fout.write(json.dumps([taskVO, subtaskVNs])+'\n')
    
    def getVOInTask(self, task):
        dependencies, _, taskToks = self.p.parseToStanfordDependencies(task)
        for dep in dependencies:
            if dep[0] in ['dobj', 'npadvmod']:
                return ' '.join(taskToks[dep[1]:dep[2]+1])

    def getVNInSubtask(self, subtask):
        _, posTags, subtaskToks = self.p.parseToStanfordDependencies(subtask)
        try:
            return [subtaskToks[pos] for pos, tag in posTags.iteritems() if tag[0].lower() in ['v', 'n']]
        except IndexError:
            print posTags, subtaskToks
            assert False

    def refineTask(self, task):
        return task.replace('-', ' ').lower()
    
    def refineSubtask(self, subtask):
        return subtask.replace('-', ' ').lower()

class AccumulateQueries(FileReader):
    def __init__(self, fname):
        FileReader.__init__(self, fname)
        self.fout = open(DATADIR+QUERYFREQUENCE, 'w')

        self.lastQuery = ''
        self.qcount = 1

        self.batch = [0]*10000
        self.batchLen = 0
    
    def processLine(self, line):
        if line != self.lastQuery:
            self.batch[self.batchLen] = '\t'.join(tuple((self.lastQuery, str(self.qcount))))
            self.batchLen += 1

            self.lastQuery = line
            self.qcount = 1

            if self.batchLen == 10000:
                self.fout.write('\n'.join(self.batch))
                self.batchLen = 0

        else:
            self.qcount += 1

    def finalize(self):
        self.fout.write('\n'.join(self.batch))
        self.fout.close()

class QueriesIndex:
    def __init__(self, fname):
        self.qfin = open(fname, 'r')

    def buildIndex(self):
        indexes = defaultdict(lambda: [], {})
        qID = 0

        for query in self.qfin:
            qToks = query.split()
            for tok in qToks:
                indexes[tok].append(qID)

            qID += 1

        for index in indexes:
            if len(index) == 1:
                index.append(-1)

            indexes[index] = tuple(indexes[index])

        json.dump(indexes, open(DATADIR+'queries/indexes', 'w'))
        self.indexes = indexes

    def loadIndexes(self):
        self.indexes = json.load(open(DATADIR+'queries/indexes'))
        self.queries = self.qfin.readlines()

    def searchQuery(self, words):
        qIDFinal = self.indexes[words[0]]
        for i in xrange(1, len(words)):
            qID = self.indexes[words[i]]
            tmp = []
            j = 0
            for qid in qIDFinal:
                try:
                    while qID[j] < qid:
                        j += 1
                except IndexError:
                    continue

                if qID[j] == qid:
                    tmp.append(qid)

            qIDFinal = tmp

        return [self.queries[ID] for ID in qIDFinal]
