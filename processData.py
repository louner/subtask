from pymongo import MongoClient
from collections import defaultdict
import os 
from parameter import *
import re
from gaia import *

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

        self.fout = [0]*256
        for i in range(256):
            self.fout[i] = open('../data/queries/queries.'+str(i), 'w')

    def processLine(self, line):
        query = line.split('\t')[1]
        query = refineQuery(query)

        self.fout[self.lineNum%256].write(query+'\n')

def refineQueries():
    queries = open(QUERIESFILE, 'r').readlines()
    fout = open(QUERIESFILE+'.refined', 'w')
    nonCharacterProg = re.compile('[^a-zA-Z0-9\s]')

    for query in queries:
        if nonCharacterProg.search(query):
            continue

        query = re.sub('\s+', ' ', query)
        query = query.strip()
        fout.write(query+'\n')

    fout.close()
