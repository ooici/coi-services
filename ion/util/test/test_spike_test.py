#!/usr/bin/env python
'''
@author Tim G
@date Wed Mar 27 09:27:08 EDT 2013
'''

from pyon.util.int_test import IonIntegrationTestCase
from ion.util.stored_values import StoredValueManager
from ion.util.parsers.spike_test import spike_parser
from nose.plugins.attrib import attr

@attr('INT',group='dm')
class TestStuckValueTestParser(IonIntegrationTestCase):
    def setUp(self):
        self._start_container()

    def test_stuck_value_test(self):
        svm = StoredValueManager(self.container)
        for key,doc in spike_parser(spike_value_test_sample_doc):
            svm.stored_value_cas(key,doc)
            self.addCleanup(svm.delete_stored_value,key)

        doc = svm.read_value('spike_ssxbt-ssn719_PRESSURE')
        self.assertEquals(doc['spike_n'], 1.2)
        self.assertEquals(doc['spike_l'], 10.)

        doc = svm.read_value('spike_ssxbt-ssn719_COND')
        self.assertEquals(doc['spike_n'], 0.2)
        self.assertEquals(doc['units'], 'S m-1')


spike_value_test_sample_doc='''Array,Instrument Class,Reference Designator,Data Product,Units,ACC,N,L
array 1,SSXBT,ssxbt-ssn719,PRESSURE,dbar,5.0,1.2,10
array 1,SSXBT,ssxbt-ssn719,SOUND_VELO,ft/s2,5.0,100.0,10
array 1,SSXBT,ssxbt-ssn719,COND,S m-1,5.0,0.2,10'''