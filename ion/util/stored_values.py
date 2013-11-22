#!/usr/bin/env python
'''
@author Luke C
@date Mon Mar 25 09:57:59 EDT 2013
@file ion/util/stored_values.py
'''

from pyon.core.exception import NotFound
import gevent



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
        except KeyError as e:
            if 'http' in e.message:
                doc_id, rev = self.store.create_doc(document_updates, object_id=doc_key)
                return doc_id, rev

        for k,v in document_updates.iteritems():
            doc[k] = v
        doc_id, rev = self.store.update_doc(doc)
        return doc_id, rev

    def read_value(self, doc_key):
        doc = self.store.read_doc(doc_key)
        return doc

    def read_value_mult(self, doc_keys, strict=False):
        doc_list = self.store.read_doc_mult(doc_keys, strict=strict)
        return doc_list

    def delete_stored_value(self, doc_key):
        self.store.delete_doc(doc_key)


