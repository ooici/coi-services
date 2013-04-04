#!/usr/bin/env python
'''
@author Luke C
@date Wed Mar 27 09:27:08 EDT 2013
'''

from pyon.util.int_test import IonIntegrationTestCase
from ion.util.stored_values import StoredValueManager
from ion.util.parsers.stuck_value_test import stuck_value_test_parser
from nose.plugins.attrib import attr

@attr('INT',group='dm')
class TestStuckValueTestParser(IonIntegrationTestCase):
    def setUp(self):
        self._start_container()

    def test_stuck_value_test(self):
        svm = StoredValueManager(self.container)
        for key,doc in stuck_value_test_parser(stuck_value_test_sample_doc):
            svm.stored_value_cas(key,doc)
            self.addCleanup(svm.delete_stored_value,key)

        doc = svm.read_value('svt_ssxbt-ssn719_PRESSURE')
        self.assertEquals(doc['svt_resolution'], 1.2)
        self.assertEquals(doc['svt_n'], 10.)

        doc = svm.read_value('svt_ssxbt-ssn719_COND')
        self.assertEquals(doc['svt_resolution'], 0.2)
        self.assertEquals(doc['units'], 'S m-1')


stuck_value_test_sample_doc='''Array,Instrument Class,Reference Designator,Data Product,Units,Resolution (R),Number of repeat values N
array 1,SSXBT,ssxbt-ssn719,PRESSURE,dbar,1.2,10
array 1,SSXBT,ssxbt-ssn719,SOUND_VELO,ft/s2,100.0,10
array 1,SSXBT,ssxbt-ssn719,COND,S m-1,0.2,10'''

