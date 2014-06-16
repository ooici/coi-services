#!/usr/bin/env python

__author__ = 'Thomas R. Lennan, Michael Meisinger, Stephen Henrie'


from pyon.core.governance import ORG_MANAGER_ROLE, DATA_OPERATOR, OBSERVATORY_OPERATOR, INSTRUMENT_OPERATOR, \
    GovernanceHeaderValues, has_org_role
from pyon.ion.resregistry import ResourceRegistryServiceWrapper
from pyon.public import log, OT, RT, PRED, Inconsistent

from interface.services.coi.iresource_registry_service import BaseResourceRegistryService


class ResourceRegistryService(BaseResourceRegistryService):
    """
    Service that manages resources instances and all cross-cutting concerns of
    system resources. Uses a datastore instance for resource object persistence.
    """

    def on_init(self):
        # Use the wrapper to adapt the container resource registry to the service interface.
        # It also provides mapping from process context actor_id to function arguments.
        self.resource_registry = ResourceRegistryServiceWrapper(self.container.resource_registry, self)


    # -------------------------------------------------------------------------
    # Resource CRUDs and mults

    def create(self, object=None):
        return self.resource_registry.create(object=object)

    def read(self, object_id='', rev_id=''):
        return self.resource_registry.read(object_id=object_id, rev_id=rev_id)

    def read_mult(self, object_ids=None):
        return self.resource_registry.read_mult(object_ids)

    def update(self, object=None):
        return self.resource_registry.update(object=object)

    def delete(self, object_id=''):
        return self.resource_registry.delete(object_id=object_id)


    # -------------------------------------------------------------------------
    # Resource LCS change

    def retire(self, resource_id=''):
        return self.resource_registry.retire(resource_id=resource_id)

    def lcs_delete(self, resource_id=''):
        return self.resource_registry.lcs_delete(resource_id=resource_id)

    def execute_lifecycle_transition(self, resource_id='', transition_event=''):
        return self.resource_registry.execute_lifecycle_transition(resource_id=resource_id,
            transition_event=transition_event)

    def set_lifecycle_state(self, resource_id='', target_lcstate=''):
        return self.resource_registry.set_lifecycle_state(resource_id=resource_id, target_lcstate=target_lcstate)


    # -------------------------------------------------------------------------
    # Attachments

    def create_attachment(self, resource_id='', attachment=None):
        return self.resource_registry.create_attachment(resource_id=resource_id, attachment=attachment)

    def read_attachment(self, attachment_id='', include_content=False):
        return self.resource_registry.read_attachment(attachment_id=attachment_id, include_content=include_content)

    def delete_attachment(self, attachment_id=''):
        return self.resource_registry.delete_attachment(attachment_id=attachment_id)

    def find_attachments(self, resource_id='', keyword='', limit=0, descending=False, include_content=False, id_only=True):
        return self.resource_registry.find_attachments(
            resource_id=resource_id, keyword='', limit=limit,
            descending=descending, include_content=include_content,
            id_only=id_only)


    # -------------------------------------------------------------------------
    # Association CRUDs and finds

    def create_association(self, subject=None, predicate=None, object=None, assoc_type=None):
        return self.resource_registry.create_association(subject=subject, predicate=predicate,
                                                         object=object, assoc_type=assoc_type)

    def create_association_mult(self, assoc_list=None):
        return self.resource_registry.create_association_mult(assoc_list=assoc_list)

    def delete_association(self, association=''):
        return self.resource_registry.delete_association(association=association)

    def find_associations(self, subject='', predicate='', object='', assoc_type=None, id_only=False,
                          anyside='', limit=0, skip=0, descending=False):
        return self.resource_registry.find_associations(subject=subject, predicate=predicate, object=object,
                                                        assoc_type=assoc_type, id_only=id_only, anyside=anyside,
                                                        limit=limit, skip=skip, descending=descending)

    def get_association(self, subject="", predicate="", object="", assoc_type=None, id_only=False):
        return self.resource_registry.get_association(subject=subject, predicate=predicate,
            object=object, assoc_type=assoc_type, id_only=id_only)


    # -------------------------------------------------------------------------
    # Resource finds

    def read_object(self, subject="", predicate="", object_type="", assoc="", id_only=False):
        return self.resource_registry.read_object(subject=subject, predicate=predicate,
            object_type=object_type, assoc=assoc, id_only=id_only)

    def find_objects(self, subject="", predicate="", object_type="", id_only=False, limit=0, skip=0, descending=False):
        return self.resource_registry.find_objects(subject=subject, predicate=predicate,
            object_type=object_type, id_only=id_only, limit=limit, skip=skip, descending=descending)

    def find_objects_mult(self, subjects=[], id_only=False, predicate=""):
        return self.resource_registry.find_objects_mult(subjects=subjects, id_only=id_only, predicate=predicate)

    def read_subject(self, subject_type="", predicate="", object="", assoc="", id_only=False):
        return self.resource_registry.read_subject(subject_type=subject_type, predicate=predicate,
            object=object, assoc=assoc, id_only=id_only)

    def find_subjects(self, subject_type="", predicate="", object="", id_only=False, limit=0, skip=0, descending=False):
        return self.resource_registry.find_subjects(subject_type=subject_type, predicate=predicate,
            object=object, id_only=id_only, limit=limit, skip=skip, descending=descending)

    def find_subjects_mult(self, objects=None, id_only=False, predicate=""):
        return self.resource_registry.find_subjects_mult(objects=objects, id_only=id_only, predicate=predicate)

    def find_resources(self, restype="", lcstate="", name="", id_only=False):
        return self.resource_registry.find_resources(restype=restype, lcstate=lcstate, name=name, id_only=id_only)

    def find_resources_ext(self, restype='', lcstate='', name='', keyword='', nested_type='', attr_name='', attr_value='',
                           alt_id='', alt_id_ns='', limit=0, skip=0, descending=False, id_only=False, query=None):
        return self.resource_registry.find_resources_ext(restype=restype, lcstate=lcstate, name=name,
            keyword=keyword, nested_type=nested_type, attr_name=attr_name, attr_value=attr_value,
            alt_id=alt_id, alt_id_ns=alt_id_ns,
            limit=limit, skip=skip, descending=descending,
            id_only=id_only, query=query)


    # -------------------------------------------------------------------------
    # Extended resource framework

    def get_resource_extension(self, resource_id='', resource_extension='', ext_associations=None, ext_exclude=None, optional_args=None):
        """Returns any ExtendedResource object containing additional related information derived from associations

        @param resource_id    str
        @param resource_extension    str
        @param ext_associations    dict
        @param ext_exclude    list
        @param optional_args    dict
        @retval extended_resource    ExtendedResource
        @throws BadRequest    A parameter is missing
        @throws NotFound    An object with the specified resource_id does not exist
        """
        # Ensure that it is not a NoneType
        optional_args = dict() if optional_args is None else optional_args

        return self.resource_registry.get_resource_extension(resource_extension=resource_extension, resource_id=resource_id,
            computed_resource_type=OT.ComputedAttributes, ext_associations=ext_associations, ext_exclude=ext_exclude, **optional_args)

    def prepare_resource_support(self, resource_type='', resource_id=''):
        """
        Returns the object containing the data to create/update a resource
        """

        return self.resource_registry.prepare_resource_support(resource_type=resource_type, resource_id=resource_id)


    # -------------------------------------------------------------------------
    # Governance functions

    def check_attachment_policy(self, process, message, headers):
        try:
            gov_values = GovernanceHeaderValues(headers=headers, process=process, resource_id_required=False)
        except Inconsistent, ex:
            return False, ex.message

        resource_id = message.resource_id

        resource = self.resource_registry.read(resource_id)
        # Allow attachment to an org
        if resource.type_ == 'Org':
            if (has_org_role(gov_values.actor_roles, resource.org_governance_name,
                             [ORG_MANAGER_ROLE, INSTRUMENT_OPERATOR, OBSERVATORY_OPERATOR, DATA_OPERATOR])):
                return True, ''

        # Allow actor to add attachment to his own UserInfo
        elif resource.type_ == 'UserInfo':
            actor_identity,_ = self.resource_registry.find_subjects(subject_type=RT.ActorIdentity,
                                                                    predicate=PRED.hasInfo, object=resource_id, id_only=False)
            if actor_identity[0]._id == headers['ion-actor-id']:
                return True, ''
        # Allow actor to add attachment to any resource in an org where the actor has appropriate role
        else:
            orgs,_ = self.resource_registry.find_subjects(subject_type=RT.Org, predicate=PRED.hasResource,
                                                          object=resource_id, id_only=False)
            for org in orgs:
                if (has_org_role(gov_values.actor_roles, org.org_governance_name,
                                 [ORG_MANAGER_ROLE, INSTRUMENT_OPERATOR, OBSERVATORY_OPERATOR, DATA_OPERATOR])):
                    return True, ''

        return False, '%s(%s) has been denied since the user is not a member in any org to which the resource id %s belongs ' % (process.name, gov_values.op, resource_id)

    def check_edit_policy(self, process, message, headers):
        try:
            gov_values = GovernanceHeaderValues(headers=headers, process=process, resource_id_required=True)
        except Inconsistent, ex:
            return False, ex.message

        resource_id = gov_values.resource_id

        resource = self.resource_registry.read(resource_id)
        # Allow edit to an org
        if resource.type_ == 'Org':
            if (has_org_role(gov_values.actor_roles, resource.org_governance_name, [ORG_MANAGER_ROLE, INSTRUMENT_OPERATOR, OBSERVATORY_OPERATOR, DATA_OPERATOR])):
                return True, ''

        # Allow edit to add attachment to his own UserInfo
        elif resource.type_ == 'UserInfo':
            actor_identity,_ = self.resource_registry.find_subjects(subject_type=RT.ActorIdentity,
                                                                    predicate=PRED.hasInfo, object=resource_id, id_only=False)
            if actor_identity[0]._id == headers['ion-actor-id']:
                return True, ''
        # Allow actor to add attachment to any resource in an org where the actor has appropriate role
        else:
            orgs,_ = self.resource_registry.find_subjects(subject_type=RT.Org, predicate=PRED.hasResource, object=resource_id, id_only=False)
            for org in orgs:
                if (has_org_role(gov_values.actor_roles, org.org_governance_name,
                                 [ORG_MANAGER_ROLE, INSTRUMENT_OPERATOR, OBSERVATORY_OPERATOR, DATA_OPERATOR])):
                    return True, ''

        return False, '%s(%s) has been denied since the user is not a member in any org to which the resource id %s belongs ' % (process.name, gov_values.op, resource_id)

    def check_lifecycle_policy(self, process, message, headers):
        try:
            gov_values = GovernanceHeaderValues(headers=headers, process=process)
            resource_id = gov_values.resource_id
        except Inconsistent, ex:
            log.error("unable to retrieve governance header")
            return False, ex.message

        # Allow actor to start/stop instrument in an org where the actor has the appropriate role
        orgs,_ = self.resource_registry.find_subjects(subject_type=RT.Org, predicate=PRED.hasResource, object=resource_id, id_only=False)
        for org in orgs:
            if (has_org_role(gov_values.actor_roles, org.org_governance_name,
                             [INSTRUMENT_OPERATOR, DATA_OPERATOR, ORG_MANAGER_ROLE, OBSERVATORY_OPERATOR])):
                log.error("returning true: "+str(gov_values.actor_roles))
                return True, ''

        log.error("returning false: "+str(gov_values.actor_roles))

        return False, '%s(%s) denied since user doesn''t have appropriate role in any org with which the resource id %s is shared ' % (process.name, gov_values.op, resource_id)
