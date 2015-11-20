from pattern import *
from train import *

def testPatternTransformer():
    def testAll():
        pt = PatternTransformer()
        query = 'lose weights by herbs'
        pattern, labelSubstrings, _ = pt.transformToPattern(query)
        assert pattern == 'VO1 by SN2'
        assert labelSubstrings['VO1'] == 'lose weights'
        assert labelSubstrings['SN2'] == 'herbs'

    def testderiveSNVLabels():
        queryToks = 'lose weights by herbs'.split()
        posTags = {0: u'VB', 1: u'NNS', 2: u'IN', 3: u'NNS'}
        labelCount = defaultdict(lambda: 0, {})
        labelSubstrings = {}

        pt = PatternTransformer()
        pt.deriveSNVLabels(posTags, labelCount, labelSubstrings, queryToks)

        assert labelSubstrings == {"SN1": "weights", "SN2": "herbs", "SV1": "lose"}
        assert labelCount == {'SV': 1, 'SN': 2}
        assert queryToks == ['SV1', 'SN1', 'by', 'SN2']

    def testderiveVOMNLabels():
        dependencies = [('dobj', 0, 1), ('prep_by', 0, 3)]
        labelSubstrings = {"SN1": "weights", "SN2": "herbs", "SV1": "lose"}
        labelCount = defaultdict(lambda: 0, {'SV': 1, 'SN': 2})
        queryToks = ['SV1', 'SN1', 'by', 'SN2']

        pt = PatternTransformer()
        pt.deriveVOMNLabels(dependencies, labelCount, labelSubstrings, queryToks)

        assert labelSubstrings['VO1'] == 'lose weights'
        assert queryToks == ['VO1', '', 'by', 'SN2']

    testAll()
    testderiveSNVLabels()
    testderiveVOMNLabels()

def testPattern():
    candq = CandidateQuery({'query': 'lose weight by herb', 'pattern': 'VO1 by SN1', 'label': {'VO1': 'lose weight', 'SN1': 'herb'}})
    pat = Pattern(('T by SUBT', 1))
    assert pat.findSubtasks([candq])['herb'] == 1

    candq = CandidateQuery({'query': 'how to lose weight by herb and sports', 'pattern': 'how to VO1 by SN1 and SN2', 'label': {'VO1': 'lose weight', 'SN1': 'herb', 'SN2': 'sport'}})
    pat = Pattern(('T by SUBT', 1))
    assert pat.findSubtasks([candq])['herb'] == 1

def testTSUBTAndQueryToPattern():
    topat = TSUBTAndQueryToPattern()
    query = 'losing much weights by herbs'
    tsubt = ['lose weight', ['sport', 'herb']]

    topat.mc = MongoClient()
    topat.db = topat.mc['test']
    collectionName = 'test'
    topat.col = topat.db[collectionName]

    topat.db[collectionName].drop()

    record = json.loads('{"pattern": "VO1 by SN2", "4search": "lose weight herb", "query": "losing much weights by herbs", "label": {"SN2": "herb", "VO1": "lose much weight"}}')

    topat.db[collectionName].insert_one(record)
    topat.db[collectionName].create_index([('4search', 'text')])

    queries = topat.findQueriesByTSUBT(tsubt)
    pattern = topat.transformToPatterns(tsubt, queries)[0]

    assert pattern['pattern'] == 'T by SUBT'
    assert pattern['label']['T'] == 'lose much weight'
    assert pattern['label']['SUBT'] == 'herb'

testPatternTransformer()
testPattern()
testTSUBTAndQueryToPattern()
