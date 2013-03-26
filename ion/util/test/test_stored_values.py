#!/usr/bin/env python
'''
@author Luke C
@date Tue Mar 26 11:03:38 EDT 2013
'''

from pyon.util.int_test import IonIntegrationTestCase
from ion.util.stored_values import StoredValueManager, simple_csv_lookup_parser
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

    def test_csv_parser(self):
        svm = StoredValueManager(self.container)
        simple_csv_lookup_parser(self.container, 'constants', csv_document_example)
        self.addCleanup(svm.delete_stored_value, 'constants')
        doc = svm.read_value('constants')
        self.assertIn('universe', doc)
        self.assertEquals(doc['gamma'],0.57721566 )


csv_document_example='''coefficient name,value
pi,3.14159265
e,2.71828
gamma,0.57721566
universe,42'''

