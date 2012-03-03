#!/usr/bin/env python

__author__ = 'Thomas R. Lennan, Michael Meisinger'
__license__ = 'Apache 2.0'

from interface.services.coi.iresource_registry_service import BaseResourceRegistryService

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

    def create(self, object=None):
        ctx = self.get_context()
        ion_actor_id = ctx.get('ion-actor-id', None) if ctx else None
        return self.resource_registry.create(object=object, actor_id=ion_actor_id)

    def read(self, object_id='', rev_id=''):
        return self.resource_registry.read(object_id=object_id, rev_id=rev_id)

    def update(self, object=None):
        return self.resource_registry.update(object=object)

    def delete(self, object_id=''):
        return self.resource_registry.delete(object_id=object_id)

    def execute_lifecycle_transition(self, resource_id='', transition_event=''):
        return self.resource_registry.execute_lifecycle_transition(resource_id=resource_id,
                                                                   transition_event=transition_event)

    def set_lifecycle_state(self, resource_id='', target_lcstate=''):
        return self.resource_registry.set_lifecycle_state(resource_id=resource_id, target_lcstate=target_lcstate)

    def create_attachment(self, resource_id='', attachment=None):
        return self.resource_registry.create_attachment(resource_id=resource_id, attachment=attachment)

    def read_attachment(self, attachment_id=''):
        return self.resource_registry.read_attachment(attachment_id=attachment_id)

    def delete_attachment(self, attachment_id=''):
        return self.resource_registry.delete_attachment(attachment_id=attachment_id)

    def find_attachments(self, resource_id='', limit=0, descending=False, include_content=False, id_only=True):
        return self.resource_registry.find_attachments(resource_id=resource_id, limit=limit,
                                                       descending=descending, include_content=include_content,
                                                       id_only=id_only)

    def create_association(self, subject=None, predicate=None, object=None, assoc_type=None):
        return self.resource_registry.create_association(subject=subject, predicate=predicate,
                                                         object=object, assoc_type=assoc_type)

    def delete_association(self, association=''):
        return self.resource_registry.delete_association(association=association)

    def find_objects(self, subject="", predicate="", object_type="", id_only=False):
        return self.resource_registry.find_objects(subject=subject, predicate=predicate,
            object_type=object_type, id_only=id_only)

    def find_subjects(self, subject_type="", predicate="", object="", id_only=False):
        return self.resource_registry.find_subjects(subject_type=subject_type, predicate=predicate,
            object=object, id_only=id_only)

    def find_associations(self, subject="", predicate="", object="", assoc_type=None, id_only=False):
        return self.resource_registry.find_associations(subject=subject, predicate=predicate,
                                                        object=object, assoc_type=assoc_type, id_only=id_only)

    def get_association(self, subject="", predicate="", object="", assoc_type=None, id_only=False):
        return self.resource_registry.get_association(subject=subject, predicate=predicate,
                                                      object=object, assoc_type=assoc_type, id_only=id_only)

    def find_resources(self, restype="", lcstate="", name="", id_only=False):
        return self.resource_registry.find_resources(restype=restype, lcstate=lcstate, name=name, id_only=id_only)
