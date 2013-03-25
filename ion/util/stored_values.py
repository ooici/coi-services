#!/usr/bin/env python
'''
@author Luke C
@date Mon Mar 25 09:57:59 EDT 2013
@file ion/util/stored_values.py
'''

import csv
from StringIO import StringIO
from pyon.core.exception import NotFound
from pyon.util.containers import get_ion_ts


def simple_csv_lookup_parser(container, key, csv_doc):
    '''
    Parses a very simple CSV file which contains key/value pairs for coefficients, an example file would be:
    name,value
    coeffA,12.3
    coeffB,13.2
    psiDefault,3.068
    '''
    sio = StringIO()
    sio.write(csv_doc)
    sio.seek(0)
    lookup_table = {'updated':get_ion_ts()}
    dr = csv.reader(sio)
    dr.next() # Skip header
    for row in dr:
        lookup_table[row[0]] = float(row[1])
    svm = StoredValueManager(container)
    return svm.stored_value_cas(key,lookup_table)

class StoredValueManager(object):
    def __init__(self, container):
        self.store = container.object_store

    def stored_value_cas(self, doc_key, document_updates):
        '''
        Performs a check and set for a lookup_table in the object store for the given key
        '''
        try:
            doc = self.store.read_doc(doc_key)
        except NotFound:
            doc_id, rev = self.store.create_doc(document_updates, object_id=doc_key)
            return doc_id, rev
        for k,v in document_updates.iteritems():
            doc[k] = v
        doc_id, rev = self.store.update_doc(doc)
        return doc_id, rev

    def read_value(self, doc_key):
        doc = self.store.read_doc(doc_key)
        return doc




