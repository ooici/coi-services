#!/usr/bin/env python

__author__ = 'Thomas R. Lennan'
__license__ = 'Apache 2.0'

from pyon.core.bootstrap import sys_name
from pyon.core.exception import NotFound
from pyon.datastore.couchdb.couchdb_datastore import CouchDB_DataStore
from pyon.datastore.mockdb.mockdb_datastore import MockDB_DataStore
from interface.services.coi.iresource_registry_service import BaseResourceRegistryService

class ResourceRegistryService(BaseResourceRegistryService):

    def on_init(self):
        resource_registry_name = sys_name + "_resources"
        resource_registry_name = resource_registry_name.lower()
        persistent = False
        force_clean = False
        if 'resource_registry' in self.CFG:
            resource_registry_cfg = self.CFG['resource_registry']
            if 'persistent' in resource_registry_cfg:
                if resource_registry_cfg['persistent'] == True:
                    persistent = True
            if 'force_clean' in resource_registry_cfg:
                if resource_registry_cfg['force_clean'] == True:
                    force_clean = True
        if persistent:
            self.resource_registry = CouchDB_DataStore(datastore_name=resource_registry_name)
        else:
            self.resource_registry = MockDB_DataStore(datastore_name=resource_registry_name)
        if force_clean:
            try:
                self.resource_registry.delete_datastore(resource_registry_name)
            except NotFound:
                pass
        if not self.resource_registry.datastore_exists(resource_registry_name):
            self.resource_registry.create_datastore(resource_registry_name)

    def create(self, object={}):
        return self.resource_registry.create(object)

    def read(self, object_id='', rev_id=''):
        return self.resource_registry.read(object_id, rev_id)

    def update(self, object={}):
        return self.resource_registry.update(object)

    def delete(self, object={}):
        return self.resource_registry.delete_doc(object)

    def find(self, criteria=[]):
        return self.resource_registry.find(criteria)

    def create_association(self, subject_id=None, predicate=None, object_id=None):
        return self.resource_registry.create_association(subject_id, predicate, object_id)

    def delete_association(self, association_id=''):
        return self.resource_registry.delete_association(tuple, datastore_name)

    def find_associations(self, subject_id=None, predicate=None, object_id=None):
        return self.resource_registry.find_associations(subject_id, predicate, object_id)

    def find_objects(self, subject_id=None, predicate=None):
        return self.resource_registry.find_objects(subject_id, predicate)

    def find_subjects(self, object_id=None, predicate=None):
        return self.resource_registry.find_subjects(object_id, predicate)
