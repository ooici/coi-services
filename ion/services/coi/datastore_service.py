#!/usr/bin/env python

__author__ = 'Thomas R. Lennan, Michael Meisinger'
__license__ = 'Apache 2.0'

from pyon.datastore.datastore import DataStore

from interface.services.coi.idatastore_service import BaseDatastoreService


class DataStoreService(BaseDatastoreService):
    """
    The Data Store Service stores and retrieves persistent objects in the system.
    These object have less overhead with them than resources with their full life-cycle.
    """

    def on_init(self):
        # Get an instance of datastore configured for persistent objects.
        # May be persistent or mock, forced clean, with indexes
        self.obj_store = self.container.datastore_manager.get_datastore("objects", DataStore.DS_PROFILE.OBJECTS, self.CFG)

        # For easier interactive shell debugging
        self.dss = self.obj_store.server[self.obj_store.datastore_name] if hasattr(self.obj_store, 'server') else None
        self.ds = self.obj_store


    def on_quit(self):
        BaseDatastoreService.on_quit(self)
        self.obj_store.close()

    def create_datastore(self, datastore_name=''):
        return self.obj_store.create_datastore(datastore_name)

    def delete_datastore(self, datastore_name=''):
        return self.obj_store.delete_datastore(datastore_name)

    def list_datastores(self):
        return self.obj_store.list_datastores()

    def info_datastore(self, datastore_name=''):
        return self.obj_store.info_datastore(datastore_name)

    def datastore_exists(self, datastore_name=''):
        return self.obj_store.datastore_exists(datastore_name)

    def list_objects(self, datastore_name=''):
        return self.obj_store.list_objects(datastore_name)

    def list_object_revisions(self, object_id='', datastore_name=''):
        return self.obj_store.list_object_revisions(object_id, datastore_name)

    def create(self, object={}, object_id='', datastore_name=''):
        return self.obj_store.create(object, object_id=object_id, datastore_name=datastore_name)

    def create_doc(self, object={}, object_id='', datastore_name=''):
        return self.obj_store.create_doc(object, object_id=object_id, datastore_name=datastore_name)

    def read(self, object_id='', rev_id='', datastore_name=''):
        return self.obj_store.read(object_id, rev_id, datastore_name)

    def read_doc(self, object_id='', rev_id='', datastore_name=''):
        return self.obj_store.read_doc(object_id, rev_id, datastore_name)

    def update(self, object={}, datastore_name=''):
        return self.obj_store.update(object, datastore_name)

    def update_doc(self, object={}, datastore_name=''):
        return self.obj_store.update_doc(object, datastore_name)

    def delete(self, object={}, datastore_name=''):
        return self.obj_store.delete(object, datastore_name)

    def delete_doc(self, object={}, datastore_name=''):
        return self.obj_store.delete_doc(object, datastore_name)

    def find(self, criteria=[], datastore_name=''):
        return self.obj_store.find(criteria, datastore_name)

    def find_doc(self, criteria=[], datastore_name=''):
        return self.obj_store.find_doc(criteria, datastore_name)

    def find_by_idref(self, criteria=[], association="", datastore_name=""):
        return self.obj_store.find_by_idref(criteria, association, datastore_name)

    def find_by_idref_doc(self, criteria=[], association="", datastore_name=""):
        return self.obj_store.find_by_idref_doc(criteria, association, datastore_name)

    def resolve_idref(self, subject="", predicate="", object="", datastore_name=""):
        return self.obj_store.resolve_idref(subject, predicate, object, datastore_name)

    def resolve_idref_doc(self, subject="", predicate="", object="", datastore_name=""):
        return self.obj_store.resolve_idref_doc(subject, predicate, object, datastore_name)

    def create_association(self, subject=None, predicate=None, object=None, assoc_type=None):
        return self.obj_store.create_association(subject, predicate, object, assoc_type)

    def delete_association(self, association=''):
        return self.obj_store.delete_association(association)

    def find_objects(self, subject="", predicate="", object_type="", id_only=False):
        return self.obj_store.find_objects(subject, predicate, object_type, id_only=id_only)

    def find_subjects(self, subject_type="", predicate="", object="", id_only=False):
        return self.obj_store.find_subjects(subject_type, predicate, object, id_only=id_only)

    def find_associations(self, subject="", predicate="", object="", id_only=False):
        return self.obj_store.find_associations(subject, predicate, object, id_only=id_only)
