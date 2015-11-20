from pymongo import MongoClient
from pattern import PatternTransformer
import re
import json
import pdb

from parameter import *


class TSUBTAndQueryToPattern:
    def __init__(self):
        mc = MongoClient()
        self.db = mc['query']
        self.col = self.db['candQ']
        self.ferr = open('/tmp/{}'.format(self.__class__), 'w')
        
    def test(self):
        query = 'losing much weights by herbs'
        tsubt = ['lose weight', ['sport', 'herb']]

        mc = MongoClient()
        db = mc['test']
        collectionName = 'test'

        db[collectionName].drop()

        pt = PatternTransformer()
        pattern, label, query = pt.transformToPattern(query)
        from nltk import PorterStemmer
        ps = PorterStemmer()

        VO, nonVO = [], []
        for k in label:
            toks = label[k].split()
            label[k] = ' '.join([ps.stem(word) for word in toks])
            if k[:2] == 'VO':
                toks = label[k].split()
                VO.append('{} {}'.format(toks[0], toks[-1]))

            else:
                nonVO.append(label[k])

        record = {'pattern': pattern, 'label': label, 'query': query, '4search': ' '.join(VO+nonVO)}
	#record = json.loads('{"pattern": "VO1 by SN2", "4search": "lose weight herb", "query": "losing much weights by herbs", "label": {"SN2": "herb", "VO1": "lose much weight"}}')
        db[collectionName].insert_one(record)
        db[collectionName].create_index([('4search', 'text')])

        
        self.db = db
        self.col = db[collectionName]

        queries = self.findQueriesByTSUBT(tsubt)
        pattern = self.transformToPatterns(tsubt, queries)[0]

        assert pattern['pattern'] == 'T by SUBT'
        assert pattern['label']['T'] == 'lose much weight'
        assert pattern['label']['SUBT'] == 'herb'

    #@profile
    def findQueriesByTSUBT(self, tsubt):
        # task contains only V and O
        # subtasks are terms, V or O, both stemmed
        task, subtasks = tsubt
        
        # candQ: {pattern: '...', label: {...}, query: '...', '4search': '...'}
        # text index in 4search, which is like 'lose weight eat fruit'(from query 'losing lots of  weight by eating fruit')
        # words in label and 4search stemmed
        query = '\"{}\" {}'.format(task, ' '.join(subtasks))
        results = self.db.command('text', self.col.name, search=query, project={'pattern':1, 'label':1, 'query':1, '_id':0}, limit=1000000)
        candQueries = [result['obj'] for result in results['results']]
        return candQueries

    #@profile
    def transformToPatterns(self, tsubt, queries):
        task, subtasks = tsubt
        subtasks = subtasks.split()

        tasktoks = task.split()
        taskpat = re.compile(r'^{}.*{}$'.format(tasktoks[0], tasktoks[-1]))
        subtaskterm = ''
        
        queriesWithTSUBT = []
        for query in queries:
            labels = query['label']
            if len(labels) == 1:
                continue
            hasTaskNotReplaced, hasSubtaskNotReplaced = True, True
            for label, substring in labels.iteritems():
                if not (hasTaskNotReplaced or hasSubtaskNotReplaced):
                    break

                if hasTaskNotReplaced and taskpat.match(substring):
                    query = self.replaceLabel(query, label, substring, 'T')
                    hasTaskNotReplaced = False
                    continue

                if hasSubtaskNotReplaced:
                    for subtask in subtasks:
                        if subtask in substring:
                            try:
                                query = self.replaceLabel(query, label, substring, 'SUBT')
                            except:
                                self.ferr.write(json.dumps(query)+'\n')
                                break
                            hasSubtaskNotReplaced = False
                            subtaskterm = subtask
                            break
                        
            if 'T' in query['label'] and 'SUBT' in query['label']
                query['T'] = '{} {}'.format(tasktoks[0], tasktoks[-1])
                query['SUBT'] = '{}'.format(subtaskterm)
                queriesWithTSUBT.append(query)

        return queriesWithTSUBT

    #@profile
    def replaceLabel(self, query, label, substring, newLabel):
        del query['label'][label]
        query['label'][newLabel] = substring
        query['pattern'] = query['pattern'].replace(label, newLabel)
        return query

def extractPatterns():
    topat = TSUBTAndQueryToPattern()
    ftsubt = open(TASKSUBTASKFILE, 'r')
    ferr = open('/tmp/extractPatterns-err', 'w')

    for line in ftsubt:
        tsubt = json.loads(line)
        try:
            queries = topat.findQueriesByTSUBT(tsubt)
            patterns = topat.transformToPatterns(tsubt, queries)
        except:
            ferr.write(line)
            continue

        for pattern in patterns:
            print '{}\t{}\t{}'.format(pattern['pattern'], pattern['T'], pattern['SUBT'])
extractPatterns()
