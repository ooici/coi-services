#!/usr/bin/env python

"""Bootstrap process for services"""

__author__ = 'Michael Meisinger'

from ion.core.bootstrap_process import BootstrapPlugin
from ion.services.coi.policy_management_service import MANAGER_ROLE, ION_MANAGER
from pyon.public import IonObject, RT

from interface.objects import Org, UserRole, NegotiationDefinition, ExchangeSpace
from interface.services.coi.iorg_management_service import OrgManagementServiceProcessClient
from interface.services.coi.iexchange_management_service import ExchangeManagementServiceProcessClient


class BootstrapService(BootstrapPlugin):
    """
    Bootstrap process for Org and related resources
    """

    def on_initial_bootstrap(self, process, config, **kwargs):
        # This is where services can be enabled after start before
        # Go through the services and activate them in dependency order
        pass

    def on_restart(self, process, config, **kwargs):
        pass
