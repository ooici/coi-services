#!/usr/bin/env python

__author__ = 'Thomas R. Lennan, Michael Meisinger'
__license__ = 'Apache 2.0'

from pyon.core.exception import BadRequest, NotFound, Inconsistent
from pyon.datastore.datastore import DataStore
from pyon.ion.resource import lcs_workflows
from pyon.public import log, LCS, AT
from pyon.util.containers import get_ion_ts

from interface.services.coi.iresource_registry_service import BaseResourceRegistryService

class ResourceRegistryService(BaseResourceRegistryService):
    """
    Service that manages resources instances and all cross-cutting concerns of
    system resources. Uses a datastore instance for resource object persistence.
    """
    def on_init(self):
        # Get an instance of datastore configured for resource registry.
        # May be persistent or mock, forced clean, with indexes
        self.rr_store = self.container.datastore_manager.get_datastore("resources", DataStore.DS_PROFILE.RESOURCES, self.CFG)

        # For easier interactive shell debugging
        self.dss = self.rr_store.server[self.rr_store.datastore_name] if hasattr(self.rr_store, 'server') else None
        self.ds = self.rr_store

    def on_quit(self):
        BaseResourceRegistryService.on_quit(self)
        self.rr_store.close()

    def create(self, object={}):
        cur_time = get_ion_ts()
        object.ts_created = cur_time
        object.ts_updated = cur_time
        return self.rr_store.create(object)

    def read(self, object_id='', rev_id=''):
        if not object_id:
            raise BadRequest("The object_id parameter is an empty string")

        return self.rr_store.read(object_id, rev_id)

    def update(self, object={}):
        if not hasattr(object, "_id") or not hasattr(object, "_rev"):
            raise BadRequest("Object does not have required '_id' or '_rev' attribute")
        # Do an check whether LCS has been modified
        res_obj = self.read(object._id)
        self.assert_condition(res_obj.lcstate == object.lcstate, "Cannot modify life cycle state in update!")
        object.ts_updated = get_ion_ts()
        return self.rr_store.update(object)

    def delete(self, object_id=''):
        res_obj = self.read(object_id)
        if not res_obj:
            raise NotFound("Resource %s does not exist" % object_id)
        return self.rr_store.delete(res_obj)

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
        updres = self.rr_store.update(res_obj)
        return new_state

    def create_association(self, subject=None, predicate=None, object=None, assoc_type=None):
        return self.rr_store.create_association(subject, predicate, object, assoc_type)

    def delete_association(self, association=''):
        return self.rr_store.delete_association(association)

    def find(self, **kwargs):
        raise NotImplementedError("Do not use find. Use a specific find operation instead.")

    def find_objects(self, subject="", predicate="", object_type="", id_only=False):
        return self.rr_store.find_objects(subject, predicate, object_type, id_only=id_only)

    def find_subjects(self, subject_type="", predicate="", object="", id_only=False):
        return self.rr_store.find_subjects(subject_type, predicate, object, id_only=id_only)

    def find_associations(self, subject="", predicate="", object="", assoc_type=None, id_only=False):
        return self.rr_store.find_associations(subject, predicate, object, assoc_type, id_only=id_only)

    def get_association(self, subject="", predicate="", object="", assoc_type=None, id_only=False):
        assoc_type = assoc_type or AT.H2H
        assoc = self.rr_store.find_associations(subject, predicate, object, assoc_type, id_only=id_only)
        if not assoc:
            raise NotFound("Association for subject/predicate/object/type %s/%s/%s/%s not found" % (
                        str(subject),str(predicate),str(object),str(assoc_type)))
        elif len(assoc) > 1:
            raise Inconsistent("Duplicate associations found for subject/predicate/object/type %s/%s/%s/%s" % (
                        str(subject),str(predicate),str(object),str(assoc_type)))
        return assoc[0]

    def find_resources(self, restype="", lcstate="", name="", id_only=False):
        return self.rr_store.find_resources(restype, lcstate, name, id_only=id_only)
