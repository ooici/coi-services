#!/usr/bin/env python

'''
@author Luke Campbell <LCampbell@ASAScience.com>
@file discovery_test
@date 05/21/12
@description Testing for Query Language
'''

from pyon.util.containers import DotDict
from pyon.core.exception import BadRequest
from pyon.util.unit_test import PyonTestCase
from nose.plugins.attrib import attr
from ion.services.dm.presentation.discovery_service import QueryLanguage


@attr('UNIT', group='dm')
class QueryLanguageUnitTest(PyonTestCase):
    def setUp(self):
        super(QueryLanguageUnitTest,self).setUp()

        self.parser = QueryLanguage()

    def test_basic_search(self):
        test_string = 'SEARCH "model" IS "high" FROM "instrument.model"'
        retval = self.parser.parse(test_string)
        struct = DotDict(retval)
        self.assertTrue(struct.query.value == 'high')
        self.assertTrue(struct.query.index == 'instrument.model')
        self.assertTrue(struct.query.field == 'model')

    def test_single_quote_search(self):
        test_string = "SEARCH 'model' IS 'high' FROM 'instrument.model'"
        retval = self.parser.parse(test_string)
        struct = DotDict(retval)
        self.assertTrue(struct.query.value == 'high')
        self.assertTrue(struct.query.index == 'instrument.model')
        self.assertTrue(struct.query.field == 'model')

    def test_bad_search(self):
        with self.assertRaises(BadRequest):
            self.parser.parse('BAD STRING')

    def test_caseless_literal(self):
        test_string = 'search "cache" is "empty" from "bins"'
        retval = self.parser.parse(test_string)
        struct = DotDict(retval)
        self.assertTrue(struct.query.value == 'empty')
        self.assertTrue(struct.query.field == 'cache')
        self.assertTrue(struct.query.index == 'bins')

    def test_range_query(self):
        test_string = 'SEARCH "model" VALUES FROM 1.14 TO 1024 FROM "instrument.model"'

        retval = self.parser.parse(test_string)
        struct = DotDict(retval)
        self.assertTrue(struct.query['range']['from'] == 1.14)
        self.assertTrue(struct.query['range']['to'] == 1024, '%s\n%s' % (self.parser.tokens, struct))

    def test_belongs_to(self):
        test_string = 'BELONGS TO "resourceID"'
        retval = self.parser.parse(test_string)
        struct = DotDict(retval)
        self.assertTrue(struct.query.association == 'resourceID')

    def test_simple_compound(self):
        test_string = "search 'instrument.model' is 'throwable' from 'indexID' and search 'model' values from 3.1415 to 69 from 'weapons'"
        retval = self.parser.parse(test_string)
        struct = DotDict(retval)
        self.assertTrue(struct.query.value == 'throwable')
        self.assertTrue(struct.query.field == 'instrument.model')
        self.assertTrue(struct.query.index == 'indexID')
        self.assertTrue(struct['and'][0]['range']['from']== 3.1415)
        self.assertTrue(struct['and'][0]['range']['to'] == 69)

    def test_association_search(self):
        test_string = "search 'instrument.model' is 'throwable' from 'indexID' and belongs to 'rsn'"
        retval = self.parser.parse(test_string)
        struct = DotDict(retval)
        self.assertTrue(struct.query.field == 'instrument.model')
        self.assertTrue(struct.query.value == 'throwable')
        self.assertTrue(struct.query.index == 'indexID')
        print struct
        self.assertTrue(struct['and'][0]['association'] == 'rsn')

    def test_union_search(self):
        test_string = "search 'instrument.family' is 'submersible' from 'devices' or search 'model' is 'abc1*' from 'instruments'"
        retval = self.parser.parse(test_string)
        struct = DotDict(retval)
        self.assertTrue(struct.query.field == 'instrument.family')
        self.assertTrue(struct.query.value == 'submersible')
        self.assertTrue(struct.query.index == 'devices')
        self.assertTrue(struct['or'][0].field == 'model')
        self.assertTrue(struct['or'][0].value == 'abc1*')
        self.assertTrue(struct['or'][0].index == 'instruments')

    def test_collection_search(self):
        test_string = "in 'collectionID'"
        retval = self.parser.parse(test_string)
        struct = DotDict(retval)
        self.assertTrue(struct.query.collection == 'collectionID')

    def test_order_by(self):
        test_string = "search 'field' is 'value' from 'index' order by 'blah' limit 2"
        test_reverse_string = "search 'field' is 'value' from 'index' limit 2 order by 'blah'"

        retval = self.parser.parse(test_string)

        struct = DotDict(retval)
        self.assertTrue(struct.query.field == 'field')
        self.assertTrue(struct.query.value == 'value')
        self.assertTrue(struct.query.index == 'index')
        self.assertTrue(struct.query.order == {'blah' : 'asc'})
        self.assertTrue(struct.query.limit == 2)

        retval = self.parser.parse(test_reverse_string)

        struct = DotDict(retval)
        self.assertTrue(struct.query.field == 'field')
        self.assertTrue(struct.query.value == 'value')
        self.assertTrue(struct.query.index == 'index')
        self.assertTrue(struct.query.order == {'blah' : 'asc'})
        self.assertTrue(struct.query.limit == 2)

    def test_offset(self):
        test_string = "search 'field' is 'value' from 'index' skip 3"
        retval = self.parser.parse(test_string)
        self.assertTrue(retval == {'and':[], 'or':[], 'query':{'field':'field', 'index':'index', 'value':'value', 'offset':3}})

    def test_geo_distance(self):
        test_string = "search 'location' geo distance 20 km from lat 20 lon 30.0 from 'index'"
        retval = self.parser.parse(test_string)
        self.assertTrue(retval == {'and':[], 'or':[], 'query':{'lat':20.0, 'lon':30.0, 'units':'km', 'field':'location', 'index':'index', 'dist':20.0}}, '%s' % retval)

    def test_geo_bbox(self):
        test_string = "search 'location' geo box top-left lat 40 lon 0 bottom-right lat 0 lon 40 from 'index'"
        retval = self.parser.parse(test_string)
        self.assertTrue(retval == {'and':[], 'or':[], 'query':{'field':'location', 'top_left':[0.0, 40.0], 'bottom_right': [40.0, 0.0], 'index':'index'}})

    def  test_time_search(self):
        test_string = "search 'ts_timestamp' time from '2012-01-01' to '2012-02-01' from 'index'"
        retval = self.parser.parse(test_string)
        self.assertTrue(retval == {'and':[], 'or':[], 'query':{'field':'ts_timestamp', 'time':{'from':'2012-01-01', 'to':'2012-02-01'}, 'index':'index'}})

    def test_open_range(self):
        test_string = 'SEARCH "model" VALUES FROM 1.14 FROM "instrument.model"'

        retval = self.parser.parse(test_string)
        struct = DotDict(retval)
        self.assertTrue(struct.query['range']['from'] == 1.14)
    
    def  test_open_time_search(self):
        test_string = "search 'ts_timestamp' time from '2012-01-01' from 'index'"
        retval = self.parser.parse(test_string)
        self.assertTrue(retval == {'and':[], 'or':[], 'query':{'field':'ts_timestamp', 'time':{'from':'2012-01-01'}, 'index':'index'}})

    def test_extensive(self):

        cases = [
            ( "SEARCH 'model' IS 'abc*' FROM 'models' AND BELONGS TO 'platformDeviceID'", 
                {'and':[{'association':'platformDeviceID'}], 'or':[], 'query':{'field':'model', 'value':'abc*', 'index':'models'}}),
            ( "SEARCH 'model' IS 'sbc*' FROM 'devices' ORDER BY 'name' LIMIT 30 AND BELONGS TO 'platformDeviceID'", 
                {'and':[{'association':'platformDeviceID'}],'or':[],'query':{'field':'model', 'value':'sbc*', 'index':'devices', 'order':{'name':'asc'}, 'limit':30}}),
            ( "SEARCH 'runtime' VALUES FROM 1. TO 100 FROM 'devices' AND BELONGS TO 'RSN'",
                {'and':[{'association':'RSN'}], 'or': [], 'query':{'field':'runtime', 'range':{'from':1, 'to':100}, 'index':'devices'}}),
            ( "BELONGS TO 'org'",
                {'and':[], 'or':[], 'query':{'association':'org'}}),

        ]


        for case in cases:
            retval = self.parser.parse(case[0])
            self.assertTrue(retval == case[1], 'Expected %s, received %s' % (case[1], retval))
