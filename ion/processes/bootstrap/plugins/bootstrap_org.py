#!/usr/bin/env python

"""Bootstrap process for org related resources"""

__author__ = 'Michael Meisinger, Stephen Henrie'

from ion.core.bootstrap_process import BootstrapPlugin, AbortBootstrap
from ion.services.coi.policy_management_service import MANAGER_ROLE, ION_MANAGER
from pyon.ion.exchange import ION_ROOT_XS
from pyon.public import IonObject, RT

from interface.objects import Org, UserRole, NegotiationDefinition, ExchangeSpace
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
        self.org_id = org_ms_client.create_org(org, headers={'ion-actor-id': system_actor_id})

        # Instantiate initial set of User Roles for this Org
        ion_manager = UserRole(name=ION_MANAGER,label='ION Manager', description='ION Manager')
        org_ms_client.add_user_role(self.org_id, ion_manager)
        org_ms_client.grant_role(self.org_id, system_actor_id, ION_MANAGER, headers={'ion-actor-id': system_actor_id} )

        # Make the ION system agent a manager for the ION Org
        org_ms_client.grant_role(self.org_id, system_actor_id, MANAGER_ROLE, headers={'ion-actor-id': system_actor_id} )

        # Create root ExchangeSpace
        xs = ExchangeSpace(name=ION_ROOT_XS, description="ION service XS")
        self.xs_id = ex_ms_client.create_exchange_space(xs, self.org_id, headers={'ion-actor-id': system_actor_id})

        # Now load the base set of negotiation definitions used by the request operations (enroll/role/resource, etc)

        neg_def = NegotiationDefinition(name=RT.EnrollmentRequest,
            description='Definition of Enrollment Request Negotiation',
            pre_condition = ['is_registered(user_id)', 'is_not_enrolled(org_id,user_id)', 'enroll_req_not_exist(org_id,user_id)'],
            accept_action = 'enroll_member(org_id,user_id)'
        )
        process.container.resource_registry.create(neg_def)

        neg_def = NegotiationDefinition(name=RT.RoleRequest,
            description='Definition of Role Request Negotiation',
            pre_condition = ['is_enrolled(org_id,user_id)'],
            accept_action = 'grant_role(org_id,user_id,role_name)'
        )
        process.container.resource_registry.create(neg_def)

        neg_def = NegotiationDefinition(name=RT.ResourceRequest,
            description='Definition of Role Request Negotiation',
            pre_condition = ['is_enrolled(org_id,user_id)'],
            accept_action = 'acquire_resource(org_id,user_id,resource_id)'
        )
        process.container.resource_registry.create(neg_def)
