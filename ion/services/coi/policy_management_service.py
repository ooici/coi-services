#!/usr/bin/env python


__author__ = 'Stephen P. Henrie'
__license__ = 'Apache 2.0'

from interface.services.coi.ipolicy_management_service import BasePolicyManagementService
from pyon.core.exception import Conflict, Inconsistent, NotFound
from pyon.public import PRED, RT
from pyon.util.log import log

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
        policy = self.clients.resource_registry.read(policy_id)
        if not policy:
            raise NotFound("Policy %s does not exist" % policy_id)
        self.clients.resource_registry.delete(policy)

    def enable_policy(self, policy_id=''):
        """Advances the lifecycle state of the specified Policy object to be enabled. Only
        enabled policies should be considered by the policy engine.

        @param policy_id    str
        @throws NotFound    object with specified id does not exist
        """
        raise NotImplementedError()


    def disable_policy(self, policy_id=''):
        """Advances the lifecycle state of the specified Policy object to be disabled. Only
        enabled policies should be considered by the policy engine.

        @param policy_id    str
        @throws NotFound    object with specified id does not exist
        """
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
        user_role = self.clients.resource_registry.read(user_role_id)
        if not user_role:
            raise NotFound("Role %s does not exist" % user_role_id)
        self.clients.resource_registry.delete(user_role)


    def grant_role(self, org_id='', user_id='', user_role_id='', scope=None):
        """Grants a defined role within an organization to a specific user. Will throw a not NotFound exception
        if none of the specified ids do not exist.

        @param org_id    str
        @param user_id    str
        @param user_role_id    str
        @param scope    RoleScope
        @throws NotFound    object with specified id does not exist
        """
        raise NotImplementedError()

    def revoke_role(self, org_id='', user_id='', user_role_id=''):
        """Revokes a defined role within an organization to a specific user. Will throw a not NotFound exception
        if none of the specified ids do not exist.

        @param org_id    str
        @param user_id    str
        @param user_role_id    str
        @throws NotFound    object with specified id does not exist
        """
        raise NotImplementedError()

    def find_roles_by_user(self, org_id='', user_id=''):
        """Returns a list of organization roles for a specific user. Will throw a not NotFound exception
        if none of the specified ids do not exist.

        @param org_id    str
        @param user_id    str
        @retval user_role_list    []
        @throws NotFound    object with specified id does not exist
        """
        raise NotImplementedError()


    def has_permission(self, org_id='', user_id='', action_id='', resource_id=''):
        """Returns a boolean of the specified user has permission for the specified action on a specified resource. Will
        throw a NotFound exception if none of the specified ids do not exist.

        @param org_id    str
        @param user_id    str
        @param action_id    str
        @param resource_id    str
        @retval has_permission    bool
        @throws NotFound    object with specified id does not exist
        """
        raise NotImplementedError()

