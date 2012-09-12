#!/usr/bin/env python


__author__ = 'Thomas R. Lennan, Michael Meisinger, Stephen Henrie'
__license__ = 'Apache 2.0'

from pyon.core.exception import BadRequest, ServerError
from pyon.ion.resource import ExtendedResourceContainer
from pyon.public import log

from interface.services.coi.iresource_registry_service import BaseResourceRegistryService

# The following decorator is needed because the couchdb client used in the RR backend
# does not catch error conditions related to datastore/view clear properly. Here, we catch
# this type of error (TypeError) and reraise as known exception for the caller to deal with.
def mask_couch_error(fn):
    def rr_call(*args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except TypeError as ex:
            # HACK HACK
            # catch error in couchdb client, log and reraise as ServerError
            log.exception("CouchDB access error")
            raise ServerError(ex)
    return rr_call

class ResourceRegistryService(BaseResourceRegistryService):
    """
    Service that manages resources instances and all cross-cutting concerns of
    system resources. Uses a datastore instance for resource object persistence.
    """
    def on_init(self):
        self.resource_registry = self.container.resource_registry

        # For easier interactive debugging
        self.dss = None
        self.ds = self.resource_registry.rr_store
        try:
            self.dss = self.resource_registry.rr_store.server[self.resource_registry.rr_store.datastore_name]
        except Exception:
            pass

    @mask_couch_error
    def create(self, object=None):
        ctx = self.get_context()
        ion_actor_id = ctx.get('ion-actor-id', None) if ctx else None
        return self.resource_registry.create(object=object, actor_id=ion_actor_id)

    @mask_couch_error
    def read(self, object_id='', rev_id=''):
        return self.resource_registry.read(object_id=object_id, rev_id=rev_id)

    @mask_couch_error
    def update(self, object=None):
        return self.resource_registry.update(object=object)

    @mask_couch_error
    def delete(self, object_id=''):
        return self.resource_registry.delete(object_id=object_id)

    @mask_couch_error
    def retire(self, resource_id=''):
        return self.resource_registry.retire(resource_id=resource_id)

    @mask_couch_error
    def execute_lifecycle_transition(self, resource_id='', transition_event=''):
        return self.resource_registry.execute_lifecycle_transition(resource_id=resource_id,
                                                                   transition_event=transition_event)

    @mask_couch_error
    def set_lifecycle_state(self, resource_id='', target_lcstate=''):
        return self.resource_registry.set_lifecycle_state(resource_id=resource_id, target_lcstate=target_lcstate)

    @mask_couch_error
    def create_attachment(self, resource_id='', attachment=None):
        return self.resource_registry.create_attachment(resource_id=resource_id, attachment=attachment)

    @mask_couch_error
    def read_attachment(self, attachment_id=''):
        return self.resource_registry.read_attachment(attachment_id=attachment_id)

    @mask_couch_error
    def delete_attachment(self, attachment_id=''):
        return self.resource_registry.delete_attachment(attachment_id=attachment_id)

    @mask_couch_error
    def find_attachments(self, resource_id='', limit=0, descending=False, include_content=False, id_only=True):
        return self.resource_registry.find_attachments(resource_id=resource_id, limit=limit,
                                                       descending=descending, include_content=include_content,
                                                       id_only=id_only)

    @mask_couch_error
    def create_association(self, subject=None, predicate=None, object=None, assoc_type=None):
        return self.resource_registry.create_association(subject=subject, predicate=predicate,
                                                         object=object, assoc_type=assoc_type)

    @mask_couch_error
    def delete_association(self, association=''):
        return self.resource_registry.delete_association(association=association)

    @mask_couch_error
    def read_object(self, subject="", predicate="", object_type="", assoc="", id_only=False):
        return self.resource_registry.read_object(subject=subject, predicate=predicate,
            object_type=object_type, assoc=assoc, id_only=id_only)

    @mask_couch_error
    def find_objects(self, subject="", predicate="", object_type="", id_only=False):
        return self.resource_registry.find_objects(subject=subject, predicate=predicate,
            object_type=object_type, id_only=id_only)

    @mask_couch_error
    def read_subject(self, subject_type="", predicate="", object="", assoc="", id_only=False):
        return self.resource_registry.read_subject(subject_type=subject_type, predicate=predicate,
            object=object, assoc=assoc, id_only=id_only)

    @mask_couch_error
    def find_subjects(self, subject_type="", predicate="", object="", id_only=False):
        return self.resource_registry.find_subjects(subject_type=subject_type, predicate=predicate,
            object=object, id_only=id_only)

    @mask_couch_error
    def find_associations(self, subject="", predicate="", object="", assoc_type=None, id_only=False):
        return self.resource_registry.find_associations(subject=subject, predicate=predicate,
                                                        object=object, assoc_type=assoc_type, id_only=id_only)
    @mask_couch_error
    def find_associations_mult(self, subjects=[], id_only=False):
        return self.resource_registry.find_associations_mult(subjects=subjects, id_only=id_only)

    @mask_couch_error
    def get_association(self, subject="", predicate="", object="", assoc_type=None, id_only=False):
        return self.resource_registry.get_association(subject=subject, predicate=predicate,
                                                      object=object, assoc_type=assoc_type, id_only=id_only)

    @mask_couch_error
    def find_resources(self, restype="", lcstate="", name="", id_only=False):
        return self.resource_registry.find_resources(restype=restype, lcstate=lcstate, name=name, id_only=id_only)

    @mask_couch_error
    def find_resources_ext(self, restype='', lcstate='', name='', keyword='', nested_type='', limit=0, skip=0, descending=False, id_only=False):
        # @TODO Remove if and else clause after pyon update
        if 'find_resources_ext' in self.resource_registry:
            return self.resource_registry.find_resources_ext(restype=restype, lcstate=lcstate, name=name,
                keyword=keyword, nested_type=nested_type, limit=limit, skip=skip, descending=descending,
                id_only=id_only)
        else:
            return self.resource_registry.find_resources(restype=restype, lcstate=lcstate, name=name, id_only=id_only)

    @mask_couch_error
    def read_mult(self, object_ids=[]):
        return self.resource_registry.read_mult(object_ids)

    @mask_couch_error
    def get_resource_extension(self, resource_id='', resource_extension='', ext_associations=None, ext_exclude=None):
        """Returns any ExtendedResource object containing additional related information derived from associations

        @param resource_id    str
        @param resource_extension    str
        @param ext_associations    dict
        @param ext_exclude    list
        @retval actor_identity    ExtendedResource
        @throws BadRequest    A parameter is missing
        @throws NotFound    An object with the specified resource_id does not exist
        """
        if not resource_id:
            raise BadRequest("The resource_id parameter is empty")

        if not resource_extension:
            raise BadRequest("The extended_resource parameter not set")

        extended_resource_handler = ExtendedResourceContainer(self, self)

        extended_resource = extended_resource_handler.create_extended_resource_container(resource_extension,
                                        resource_id, None, ext_associations, ext_exclude)

        return extended_resource
