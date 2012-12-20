#!/usr/bin/env python

"""Bootstrap process for org related resources"""

__author__ = 'Michael Meisinger, Stephen Henrie'

from ion.core.bootstrap_process import BootstrapPlugin, AbortBootstrap
from pyon.core.governance.governance_controller import ORG_MANAGER_ROLE, ION_MANAGER
from pyon.ion.exchange import ION_ROOT_XS
from pyon.public import IonObject, RT

from interface.objects import Org, UserRole, ExchangeSpace
from interface.services.coi.iorg_management_service import OrgManagementServiceProcessClient
from interface.services.coi.iexchange_management_service import ExchangeManagementServiceProcessClient


class BootstrapOrg(BootstrapPlugin):
    """
    Bootstrap process for Org and related resources
    """

    def on_initial_bootstrap(self, process, config, **kwargs):
        org_ms_client = OrgManagementServiceProcessClient(process=process)
        ex_ms_client = ExchangeManagementServiceProcessClient(process=process)

        system_actor, _ = process.container.resource_registry.find_resources(
            restype=RT.ActorIdentity, name=config.system.system_actor, id_only=True)
        if not system_actor:
            raise AbortBootstrap("Cannot find system actor")
        system_actor_id = system_actor[0]

        # Create root Org: ION
        root_orgname = config.system.root_org
        org = Org(name=root_orgname, description="ION Root Org")
        self.org_id = org_ms_client.create_org(org)

        # Instantiate initial set of User Roles for this Org
        ion_manager = UserRole(name=ION_MANAGER,label='ION Manager', description='ION Manager')
        org_ms_client.add_user_role(self.org_id, ion_manager)
        org_ms_client.grant_role(self.org_id, system_actor_id, ION_MANAGER )

        # Make the ION system agent a manager for the ION Org
        org_ms_client.grant_role(self.org_id, system_actor_id, ORG_MANAGER_ROLE )

        # Create root ExchangeSpace
        xs = ExchangeSpace(name=ION_ROOT_XS, description="ION service XS")
        self.xs_id = ex_ms_client.create_exchange_space(xs, self.org_id)

