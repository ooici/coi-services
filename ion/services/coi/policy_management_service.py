#!/usr/bin/env python



__author__ = 'Stephen P. Henrie'
__license__ = 'Apache 2.0'

from interface.services.coi.ipolicy_management_service import BasePolicyManagementService
from pyon.core.exception import NotFound, BadRequest
from pyon.public import PRED, RT
from pyon.util.containers import is_basic_identifier
from pyon.util.log import log

MANAGER_ROLE = 'Manager'
MEMBER_ROLE = 'Member'


class PolicyManagementService(BasePolicyManagementService):

    """
    Provides the interface to define and manage policy and a repository to store and retrieve policy and templates for
    policy definitions, aka attribute authority.
    """
    def create_policy(self, policy=None):
        """Persists the provided Policy object for the specified Org id. The id string returned
        is the internal id by which Policy will be identified in the data store.

        @param policy    Policy
        @retval policy_id    str
        @throws BadRequest    if object passed has _id or _rev attribute
        """
        if not is_basic_identifier(policy.name):
            raise BadRequest("The policy name '%s' can only contain alphanumeric and underscore characters" % user_role.name)

        policy.rule = policy.rule % (policy.name, policy.description)
        policy_id, version = self.clients.resource_registry.create(policy)
        return policy_id

    def update_policy(self, policy=None):
        """Updates the provided Policy object.  Throws NotFound exception if
        an existing version of Policy is not found.  Throws Conflict if
        the provided Policy object is not based on the latest persisted
        version of the object.

        @param policy    Policy
        @throws NotFound    object with specified id does not exist
        @throws BadRequest    if object does not have _id or _rev attribute
        @throws Conflict    object not based on latest persisted object version
        """
        if not is_basic_identifier(policy.name):
            raise BadRequest("The policy name '%s' can only contain alphanumeric and underscore characters" % user_role.name)

        self.clients.resource_registry.update(policy)

    def read_policy(self, policy_id=''):
        """Returns the Policy object for the specified policy id.
        Throws exception if id does not match any persisted Policy
        objects.

        @param policy_id    str
        @retval policy    Policy
        @throws NotFound    object with specified id does not exist
        """
        if not policy_id:
            raise BadRequest("The policy_id parameter is missing")

        policy = self.clients.resource_registry.read(policy_id)
        if not policy:
            raise NotFound("Policy %s does not exist" % policy_id)
        return policy

    def delete_policy(self, policy_id=''):
        """For now, permanently deletes Policy object with the specified
        id. Throws exception if id does not match any persisted Policy.

        @param policy_id    str
        @throws NotFound    object with specified id does not exist
        """
        if not policy_id:
            raise BadRequest("The policy_id parameter is missing")

        policy = self.clients.resource_registry.read(policy_id)
        if not policy:
            raise NotFound("Policy %s does not exist" % policy_id)
        self.clients.resource_registry.delete(policy_id)


    def enable_policy(self, policy_id=''):
        """Sets a flag to enable the use of the policy rule

        @param policy_id    str
        @throws NotFound    object with specified id does not exist
        """
        policy = self.read_policy(policy_id)
        policy.enabled = True
        self.update_policy(policy)


    def disable_policy(self, policy_id=''):
        """Resets a flag to disable the use of the policy rule

        @param policy_id    str
        @throws NotFound    object with specified id does not exist
        """
        policy = self.read_policy(policy_id)
        policy.enabled = False
        self.update_policy(policy)


    def add_resource_policy(self, resource_id='', policy_id=''):
        """Associates a policy rule to a specific resource

        @param resource_id    str
        @param policy_id    str
        @retval success    bool
        @throws NotFound    object with specified id does not exist
        """

        if not resource_id:
            raise BadRequest("The resource_id parameter is missing")

        resource = self.clients.resource_registry.read(resource_id)
        if not resource:
            raise NotFound("Resource %s does not exist" % resource_id)

        if not policy_id:
            raise BadRequest("The policy_id parameter is missing")

        policy = self.clients.resource_registry.read(policy_id)
        if not policy:
            raise NotFound("Policy %s does not exist" % policy_id)

        aid = self.clients.resource_registry.create_association(resource, PRED.hasPolicy, policy)
        if not aid:
            return False

        return True


    def remove_resource_policy(self, resource_id='', policy_id=''):
        """Removes an association for a policy rule to a specific resource

        @param resource_id    str
        @param policy_id    str
        @retval success    bool
        @throws NotFound    object with specified id does not exist
        """
        if not resource_id:
            raise BadRequest("The resource_id parameter is missing")

        resource = self.clients.resource_registry.read(resource_id)
        if not resource:
            raise NotFound("Resource %s does not exist" % resource_id)

        if not policy_id:
            raise BadRequest("The policy_id parameter is missing")

        policy = self.clients.resource_registry.read(policy_id)
        if not policy:
            raise NotFound("Policy %s does not exist" % policy_id)

        aid = self.clients.resource_registry.get_association(resource, PRED.hasPolicy, policy)
        if not aid:
            raise NotFound("The association between the specified Resource %s and Policy %s was not found" % (resource_id, policy_id))

        self.clients.resource_registry.delete_association(aid)
        return True

    def find_resource_policies(self, resource_id=''):
        """Finds all policies associated with a specific resource

        @param resource_id    str
        @retval policy_list    list
        @throws NotFound    object with specified id does not exist
        """
        if not resource_id:
            raise BadRequest("The resource_id parameter is missing")

        resource = self.clients.resource_registry.read(resource_id)
        if not resource:
            raise NotFound("Resource %s does not exist" % resource_id)

        policy_list,_ = self.clients.resource_registry.find_objects(resource, PRED.hasPolicy, RT.Policy)

        return policy_list

    def _find_service_resource_by_name(self, name):

        if not name:
            raise BadRequest("The name parameter is missing")

        res_list,_  = self.clients.resource_registry.find_resources(restype=RT.ServiceDefinition, name=name)
        if not res_list:
            raise NotFound('The ServiceDefinition with name %s does not exist' % name )
        return res_list[0]


    def _get_policy_template(self):

        policy_template = '''
        <?xml version="1.0" encoding="UTF-8"?>
        <Policy xmlns="urn:oasis:names:tc:xacml:2.0:policy:schema:os"
            xmlns:xacml-context="urn:oasis:names:tc:xacml:2.0:context:schema:os"
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
            xsi:schemaLocation="urn:oasis:names:tc:xacml:2.0:policy:schema:os http://docs.oasis-open.org/xacml/access_control-xacml-2.0-policy-schema-os.xsd"
            xmlns:xf="http://www.w3.org/TR/2002/WD-xquery-operators-20020816/#"
            xmlns:md="http:www.med.example.com/schemas/record.xsd"
            PolicyId="urn:oasis:names:tc:xacml:2.0:example:policyid:%s_%s"
            RuleCombiningAlgId="urn:oasis:names:tc:xacml:1.0:rule-combining-algorithm:permit-overrides">
            <PolicyDefaults>
                <XPathVersion>http://www.w3.org/TR/1999/Rec-xpath-19991116</XPathVersion>
            </PolicyDefaults>

            %s
        </Policy>
        '''
        return policy_template


    def get_active_resource_policy_rules(self, resource_id=''):
        """Generates the set of all enabled policies for the specified resource

        @param resource_id    str
        @retval policy    str
        @throws NotFound    object with specified id does not exist
        """
        if not resource_id:
            raise BadRequest("The resource_id parameter is missing")

        resource = self.clients.resource_registry.read(resource_id)
        if not resource:
            raise NotFound("Resource %s does not exist" % resource_id)

        policy = self._get_policy_template()

        #TODO - investigate better ways to optimize this
        rules = ""
        policy_set = self.find_resource_policies(resource_id)

        for p in policy_set:
            if p.enabled:
                rules += p.rule

        policy_rules = policy % ('', resource_id, rules)

        return policy_rules

    def add_service_policy(self, service_name='', policy_id=''):
        """Associates a policy rule to a specific service

        @param service_name    str
        @param policy_id    str
        @retval success    bool
        @throws NotFound    object with specified id does not exist
        """

        if not service_name:
            raise BadRequest("The name parameter is missing")

        service_resource = self._find_service_resource_by_name(service_name)
        return self.add_resource_policy(service_resource._id,policy_id )


    def remove_service_policy(self, service_name='', policy_id=''):
        """Removes an association for a policy rule to a specific service

        @param service_name    str
        @param policy_id    str
        @retval success    bool
        @throws NotFound    object with specified id does not exist
        """
        if not service_name:
            raise BadRequest("The name parameter is missing")

        service_resource = self._find_service_resource_by_name(service_name)
        return self.remove_resource_policy(service_resource._id,policy_id )

    def find_service_policies(self, service_name=''):
        """Finds all policies associated with a specific service

        @param service_name    str
        @retval policy_list    list
        @throws NotFound    object with specified id does not exist
        """
        if not service_name:
            raise BadRequest("The name parameter is missing")

        service_resource = self._find_service_resource_by_name(service_name)
        return self.find_resource_policies(service_resource._id )

    def get_active_service_policy_rules(self, org_id='', service_name=''):
        """Generates the set of all enabled policies for the specified service

        @param org_id    str
        @param service_name    str
        @retval policy    str
        @throws NotFound    object with specified id does not exist
        """

        if not org_id:
            raise BadRequest("The org_id parameter is missing")

        org = self.clients.resource_registry.read(org_id)
        if not org:
            raise NotFound("Org %s does not exist" % org_id)

        if not service_name:
            raise BadRequest("The name parameter is missing")

        policy = self._get_policy_template()

        #TODO - investigate better ways to optimize this

        rules = ""
        #First get any global Org rules
        policy_set = self.find_resource_policies(org_id)
        for p in policy_set:
            if p.enabled:
                rules += p.rule

        #Next get service specific rules
        policy_set = self.find_service_policies(service_name)
        for p in policy_set:
            if p.enabled:
                rules += p.rule


        policy_rules = policy % (org.name, service_name, rules)

        return policy_rules



    def create_role(self, user_role=None):
        """Persists the provided UserRole object. The name of a role can only contain
        alphanumeric and underscore characters while the description can me human
        readable. The id string returned is the internal id by which a UserRole will
        be indentified in the data store.

        @param user_role    UserRole
        @retval user_role_id    str
        @throws BadRequest    if object passed has _id or _rev attribute
        """

        if not is_basic_identifier(user_role.name):
            raise BadRequest("The role name '%s' can only contain alphanumeric and underscore characters" % user_role.name)

        user_role_id, version = self.clients.resource_registry.create(user_role)
        return user_role_id

    def update_role(self, user_role=None):
        """Updates the provided UserRole object.  The name of a role can only contain
        alphanumeric and underscore characters while the description can me human
        readable.Throws NotFound exception if an existing version of UserRole is
        not found.  Throws Conflict if the provided UserRole object is not based on
        the latest persisted version of the object.

        @param user_role    UserRole
        @retval success    bool
        @throws BadRequest    if object does not have _id or _rev attribute
        @throws NotFound    object with specified id does not exist
        @throws Conflict    object not based on latest persisted object version
        """

        if not is_basic_identifier(user_role.name):
            raise BadRequest("The role name '%s' can only contain alphanumeric and underscore characters" % user_role.name)

        self.clients.resource_registry.update(user_role)

    def read_role(self, user_role_id=''):
        """Returns the UserRole object for the specified role id.
        Throws exception if id does not match any persisted UserRole
        objects.

        @param user_role_id    str
        @retval user_role    UserRole
        @throws NotFound    object with specified id does not exist
        """
        if not user_role_id:
            raise BadRequest("The user_role_id parameter is missing")

        user_role = self.clients.resource_registry.read(user_role_id)
        if not user_role:
            raise NotFound("Role %s does not exist" % user_role_id)
        return user_role

    def delete_role(self, user_role_id=''):
        """For now, permanently deletes UserRole object with the specified
        id. Throws exception if id does not match any persisted UserRole.

        @param user_role_id    str
        @throws NotFound    object with specified id does not exist
        """
        if not user_role_id:
            raise BadRequest("The user_role_id parameter is missing")

        user_role = self.clients.resource_registry.read(user_role_id)
        if not user_role:
            raise NotFound("Role %s does not exist" % user_role_id)
        self.clients.resource_registry.delete(user_role_id)

    def find_roles(self):
        """Returns a list of UserRole objects defined in the resource registry.

        @retval user_role_list    list
        """
        role_list, _ = self.clients.resource_registry.find_resources(RT.UserRole, None, None, False)
        return role_list

    def find_role(self, name=''):
        """Returns UserRole object that matches the specified name.

        @param name    str
        @retval user_role    UserRole
        @throws NotFound    object with specified id does not exist
        """
        role_list, _ = self.clients.resource_registry.find_resources(RT.UserRole, None, name, False)
        if not role_list:
            raise NotFound('The User Role with name %s does not exist' % name )
        return role_list[0]


