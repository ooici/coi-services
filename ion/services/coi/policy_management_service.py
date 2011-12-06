#!/usr/bin/env python


__author__ = 'Stephen P. Henrie'
__license__ = 'Apache 2.0'

from interface.services.coi.ipolicy_management_service import BasePolicyManagementService

class PolicyManagementService(BasePolicyManagementService):


    def create_policy(self, policy={}, org_id=''):
        """ Should receive a Policy object
        """
        # Return Value
        # ------------
        # {policy_id: ''}
        #
        pass

    def update_policy(self, policy={}):
        """ Should receive a Policy object
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def read_policy(self, policy_id=''):
        """ Should return a Policy object
        """
        # Return Value
        # ------------
        # policy: {}
        #
        pass

    def delete_policy(self, policy_id=''):
        """method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def enable_policy(self, policy_id=''):
        """method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def disable_policy(self, policy_id=''):
        """method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def create_role(self, role={}):
        """ Should receive a UserRole object
        """
        # Return Value
        # ------------
        # {role_id: ''}
        #
        pass

    def update_role(self, role={}):
        """ Should receive a UserRole object
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def read_role(self, role_id=''):
        """ Should return a UserRole object
        """
        # Return Value
        # ------------
        # role: {}
        #
        pass

    def delete_role(self, role_id=''):
        """method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def grant_role(self, org_id='', user_id='', role_id='', scope={}):
        """  Should receive a RoleScope object
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def revoke_role(self, org_id='', user_id='', role_id=''):
        """method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def find_roles_by_user(self, org_id='', user_id=''):
        """method docstring
        """
        # Return Value
        # ------------
        # role_list: []
        #
        pass

    def has_permission(self, org_id='', user_id='', action_id='', resource_id=''):
        """method docstring
        """
        # Return Value
        # ------------
        # {has_permission: false}
        #
        pass


