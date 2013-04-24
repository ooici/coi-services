#!/usr/bin/env python
'''
@author Luke C
@date Wed Mar 27 09:27:08 EDT 2013
'''

from pyon.util.int_test import IonIntegrationTestCase
from ion.util.stored_values import StoredValueManager
from ion.util.parsers.trend_test import trend_parser
from nose.plugins.attrib import attr

@attr('INT',group='dm')
class TestStuckValueTestParser(IonIntegrationTestCase):
    def setUp(self):
        self._start_container()

    def test_stuck_value_test(self):
        svm = StoredValueManager(self.container)
        for key,doc in trend_parser(trend_value_test_sample_doc):
            svm.stored_value_cas(key,doc)
            self.addCleanup(svm.delete_stored_value,key)

        doc = svm.read_value('trend_ssxbt-ssn719_PRESSURE')
        self.assertEquals(doc['time_interval'], 1.0)
        self.assertEquals(doc['standard_deviation'], 0.0)

        doc = svm.read_value('trend_ssxbt-ssn719_COND')
        self.assertEquals(doc['time_interval'], 3.0)
        self.assertEquals(doc['polynomial_order'], 'third')


trend_value_test_sample_doc='''Array,Instrument Class,Reference Designator,Data Product,Time interval length in days,Polynomial order,Standard deviation reduction factor (nstd)
array 1,SSXBT,ssxbt-ssn719,PRESSURE,1.0,first,0.0
array 1,SSXBT,ssxbt-ssn719,SOUND_VELO,2.0,second,0.2
array 1,SSXBT,ssxbt-ssn719,COND,3.0,third,0.3'''