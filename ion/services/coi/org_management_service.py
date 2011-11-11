#!/usr/bin/env python

__author__ = 'Michael Meisinger'

from pyon.public import CFG, IonObject
from pyon.util.log import log

from interface.services.coi.iorg_management_service import BaseOrgManagementService

class OrgManagementService(BaseOrgManagementService):

    RT_ORG = "Org"

    def create_org(self, name=''):
        log.debug("create_org" + name)
        org = IonObject(self.RT_ORG, dict(name=name))
        rid,rev = self.clients.resource_registry.create(org)
        return rid

    def affiliate_org(self, org_id='', affiliate_org_id=''):
        # Return Value
        # ------------
        # null
        # ...
        # 
        pass

    def enroll_member(self, org_id='', user_id=''):
        # Return Value
        # ------------
        # null
        # ...
        #
        pass

    def share_resource(self, org_id='', resource_id=''):
        # Return Value
        # ------------
        # null
        # ...
        #
        pass

    def leave(self, org_id='', user_id=''):
        # Return Value
        # ------------
        # null
        # ...
        #
        pass

    def has_permission(self, org_id='', user_id='', action_id='', resource_id=''):
        # Return Value
        # ------------
        # {has_permission: false}
        # 
        pass


