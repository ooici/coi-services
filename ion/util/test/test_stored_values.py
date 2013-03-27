#!/usr/bin/env python
'''
@author Luke C
@date Tue Mar 26 11:03:38 EDT 2013
'''

from pyon.util.int_test import IonIntegrationTestCase
from ion.util.stored_values import StoredValueManager
from ion.util.parsers.global_range_test import grt_parser
from nose.plugins.attrib import attr

@attr('INT',group='dm')
class TestStoredValueManager(IonIntegrationTestCase):
    def setUp(self):
        self._start_container()

    def test_basic_stored_docs(self):

        doc = {'key':'value'}
        doc_key = 'doc_id'

        svm = StoredValueManager(self.container)
        doc_id, rev = svm.stored_value_cas(doc_key, doc)
        self.addCleanup(svm.delete_stored_value, doc_key)

        doc = svm.read_value(doc_key)
        self.assertTrue('key' in doc and doc['key']=='value')
        svm.stored_value_cas(doc_key,{'key2':'value2'})
        doc = svm.read_value(doc_key)
        self.assertTrue('key' in doc and doc['key']=='value')
        self.assertTrue('key2' in doc and doc['key2']=='value2')

    def test_global_range_test_parser(self):
        svm = StoredValueManager(self.container)
        for key,doc in grt_parser(grt_sample_doc):
            svm.stored_value_cas(key,doc)
            self.addCleanup(svm.delete_stored_value,key)

        doc = svm.read_value('grt_sbe37-abc123_TEMPWAT_UHHH')
        self.assertEquals(doc['grt_min_value'], 0.)
        self.assertEquals(doc['array'],'array 1')
        doc = svm.read_value('grt_sbe37-abc123_PRESWAT_flagged')
        self.assertEquals(doc['grt_max_value'], 689.47)

grt_sample_doc ='''Array,Instrument Class,Reference Designator,Data Product In,Units,Data Product Flagged,Min Value (lim(1)),Max Value (lim(2))
array 1,SBE-37,sbe37-abc123,TEMPWAT,deg_C,UHHH,0,20
array 1,SBE-37,sbe37-abc123,CONDWAT,S m-1,????,0,30
array 1,SBE-37,sbe37-abc123,PRESWAT,dbar,flagged,0,689.47'''

