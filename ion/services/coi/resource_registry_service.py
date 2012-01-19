#!/usr/bin/env python

__author__ = 'Thomas R. Lennan, Michael Meisinger'
__license__ = 'Apache 2.0'

from pyon.core.bootstrap import sys_name
from pyon.core.exception import NotFound, Inconsistent
from pyon.datastore.couchdb.couchdb_datastore import CouchDB_DataStore
from pyon.datastore.mockdb.mockdb_datastore import MockDB_DataStore
from pyon.ion.resource import lcs_workflows
from pyon.public import LCS
from pyon.util.containers import get_ion_ts

from interface.services.coi.iresource_registry_service import BaseResourceRegistryService

class ResourceRegistryService(BaseResourceRegistryService):

    """
    Service that manages the definition of types of resources and all cross-cutting concerns of all system resources.
    The resource registry uses the underlying push and pull ops of the datastore to fetch, retrieve and create
    resource objects.
    """
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
        # For easier interactive debugging
        self.dss = self.resource_registry.server[resource_registry_name] if persistent else None
        self.ds = self.resource_registry

    def create(self, object={}):
        cur_time = get_ion_ts()
        object.ts_created = cur_time
        object.ts_updated = cur_time
        return self.resource_registry.create(object)

    def read(self, object_id='', rev_id=''):
        return self.resource_registry.read(object_id, rev_id)

    def update(self, object={}):
        # Do an check whether LCS has been modified
        res_obj = self.read(object._id, object._rev)
        self.assert_condition(res_obj.lcstate == object.lcstate, "Cannot modify life cycle state in update!")
        object.ts_updated = get_ion_ts()
        return self.resource_registry.update(object)

    def delete(self, object_id=''):
        res_obj = self.read(object_id)
        if not res_obj:
            raise NotFound("Resource %s does not exist" % object_id)
        return self.resource_registry.delete(res_obj)

    def execute_lifecycle_transition(self, resource_id='', transition_event='', current_lcstate=''):
        self.assert_condition(not current_lcstate or current_lcstate in LCS, "Unknown life-cycle state %s" % current_lcstate)
        res_obj = self.read(resource_id)

        if current_lcstate and res_obj.lcstate != current_lcstate:
            raise Inconsistent("Resource id=%s lcstate is %s, expected was %s" % (
                                resource_id, res_obj.lcstate, current_lcstate))

        restype = type(res_obj).__name__
        restype_workflow = lcs_workflows.get(restype, None)
        if not restype_workflow:
            restype_workflow = lcs_workflows['Resource']

        new_state = restype_workflow.get_successor(res_obj.lcstate, transition_event)
        if not new_state:
            raise Inconsistent("Resource id=%s, type=%s, lcstate=%s has no transition for event %s" % (
                                resource_id, restype, res_obj.lcstate, transition_event))

        res_obj.lcstate = new_state
        res_obj.ts_updated = get_ion_ts()
        updres = self.resource_registry.update(res_obj)
        return new_state

    def create_association(self, subject=None, predicate=None, object=None, assoc_type=None):
        return self.resource_registry.create_association(subject, predicate, object, assoc_type)

    def delete_association(self, association=''):
        return self.resource_registry.delete_association(association)

    def find(self, **kwargs):
        raise NotImplementedError("Do not use find. Use a specific find operation instead.")

    def find_objects(self, subject="", predicate="", object_type="", id_only=False):
        return self.resource_registry.find_objects(subject, predicate, object_type, id_only=id_only)

    def find_subjects(self, subject_type="", predicate="", object="", id_only=False):
        return self.resource_registry.find_subjects(subject_type, predicate, object, id_only=id_only)

    def find_associations(self, subject="", predicate="", object="", id_only=False):
        return self.resource_registry.find_associations(subject, predicate, object, id_only=id_only)

    def get_association(self, subject="", predicate="", object=""):
        assoc = self.resource_registry.find_associations(subject, predicate, object, id_only=True)
        if not assoc:
            raise NotFound("Association for subject/predicate/object %s/%s/%s not found" % (str(subject),str(predicate),str(object)))
        elif len(assoc) > 1:
            raise Inconsistent("Duplicate associations found for subject/predicate/object %s/%s/%s" % (str(subject),str(predicate),str(object)))
        return assoc[0]

    def find_resources(self, restype="", lcstate="", name="", id_only=False):
        
        return self.resource_registry.find_resources(restype, lcstate, name, id_only=id_only)
