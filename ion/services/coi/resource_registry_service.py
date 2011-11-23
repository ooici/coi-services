#!/usr/bin/env python

__author__ = 'Thomas R. Lennan, Michael Meisinger'
__license__ = 'Apache 2.0'

from pyon.core.bootstrap import sys_name
from pyon.core.exception import NotFound
from pyon.datastore.couchdb.couchdb_datastore import CouchDB_DataStore
from pyon.datastore.mockdb.mockdb_datastore import MockDB_DataStore
from pyon.ion.public import LCS

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
        # Do an check whether LCS has been modified
        res_obj = self.read(object._id, object._rev)
        assert res_obj.lcstate == object.lcstate, "Cannot modify life cycle state in update!"
        return self.resource_registry.update(object)

    def delete(self, object={}):
        return self.resource_registry.delete_doc(object)

    def execute_lifecycle_transition(resource_id='', lcstate=''):
        res_obj = self.read(resource_id)
        res_obj.lcstate = lcstate
        assert lcstate in LCS, "Unknown life-cycle state %s" % lcstate
        return self.update(res_obj)

    def create_association(self, subject=None, predicate=None, object=None):
        return self.resource_registry.create_association(subject, predicate, object)

    def delete_association(self, association=''):
        return self.resource_registry.delete_association(association, datastore_name)

    def find(self, **kwargs):
        raise NotImplementedError("Do not use find. Use a specific find operation instead.")

    def find_objects(self, subject=None, predicate=None, object_type=None, id_only=False):
        return self.resource_registry.find_objects(subject, predicate, object_type, id_only=id_only)

    def find_subjects(self, object=None, predicate=None, subject_type=None, id_only=False):
        return self.resource_registry.find_subjects(object, predicate, subject_type, id_only=id_only)

    def find_by_type(self, restype, lcstate=None, id_only=False):
        return self.resource_registry.find_res_by_type(restype, lcstate, id_only=id_only)

    def find_by_lcstate(self, lcstate, restype=None, id_only=False):
        return self.resource_registry.find_res_by_lcstate(lcstate, restype, id_only=id_only)

    def find_by_name(self, name, restype=None, id_only=False):
        return self.resource_registry.find_res_by_name(name, restype, id_only=id_only)
