#!/usr/bin/env python


__author__ = 'Stephen P. Henrie'
__license__ = 'Apache 2.0'

from interface.services.coi.ipolicy_management_service import BasePolicyManagementService
from pyon.core.exception import NotFound, BadRequest
from pyon.public import PRED, RT
from pyon.util.log import log

MANAGER_ROLE = 'Manager'
MEMBER_ROLE = 'Member'


class PolicyManagementService(BasePolicyManagementService):

    """
    Provides the interface to define and manage policy and a repository to store and retrieve policy and templates for
    policy definitions, aka attribute authority.
    """
    def create_policy(self, policy=None, org_id=''):
        """Persists the provided Policy object for the specified Org id. The id string returned
        is the internal id by which Policy will be indentified in the data store.

        @param policy    Policy
        @param org_id    str
        @retval policy_id    str
        @throws BadRequest    if object passed has _id or _rev attribute
        """
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
        """Advances the lifecycle state of the specified Policy object to be enabled. Only
        enabled policies should be considered by the policy engine.

        @param policy_id    str
        @throws NotFound    object with specified id does not exist
        """
        if not policy_id:
            raise BadRequest("The policy_id parameter is missing")

        raise NotImplementedError()


    def disable_policy(self, policy_id=''):
        """Advances the lifecycle state of the specified Policy object to be disabled. Only
        enabled policies should be considered by the policy engine.

        @param policy_id    str
        @throws NotFound    object with specified id does not exist
        """
        if not policy_id:
            raise BadRequest("The policy_id parameter is missing")

        raise NotImplementedError()

    def create_role(self, user_role=None):
        """Persists the provided UserRole object. The id string returned
        is the internal id by which a UserRole will be indentified in the data store.

        @param user_role    UserRole
        @retval user_role_id    str
        @throws BadRequest    if object passed has _id or _rev attribute
        """
        user_role_id, version = self.clients.resource_registry.create(user_role)
        return user_role_id

    def update_role(self, user_role=None):
        """Updates the provided UserRole object.  Throws NotFound exception if
        an existing version of UserRole is not found.  Throws Conflict if
        the provided UserRole object is not based on the latest persisted
        version of the object.

        @param user_role    UserRole
        @retval success    bool
        @throws NotFound    object with specified id does not exist
        @throws BadRequest    if object does not have _id or _rev attribute
        @throws Conflict    object not based on latest persisted object version
        """
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


