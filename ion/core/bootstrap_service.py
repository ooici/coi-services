#!/usr/bin/env python

"""Process that bootstraps an ION system"""


__author__ = 'Michael Meisinger'

from pyon.public import CFG, IonObject, log, get_sys_name, RT, LCS, PRED, iex
from pyon.ion.exchange import ION_ROOT_XS

from interface.services.ibootstrap_service import BaseBootstrapService
from ion.services.coi.policy_management_service import MANAGER_ROLE


class BootstrapService(BaseBootstrapService):
    """
    Bootstrap service: This service will initialize the ION system environment.
    This service is triggered for each boot level.
    """

    def on_init(self):
        log.info("Bootstrap service INIT: System init")
        self.system_actor_id = None

    def on_start(self):
        level = self.CFG.level
        log.info("Bootstrap service START: service start, level: %s", level)

        self.trigger_level(level, self.CFG)

    def trigger_level(self, level, config):
        #print "Bootstrap level: %s config: %s" % (str(level),str(config))

        if level == "datastore":
            self.post_datastore(config)
        elif level == "directory":
            self.post_directory(config)
        elif level == "resource_registry":
            self.post_resource_registry(config)
        elif level == "identity_management":
            self.post_identity_management(config)
        elif level == "org_management":
            self.post_org_management(config)
        elif level == "exchange_management":
            self.post_exchange_management(config)

            self.post_startup()

        # Create ROOT user identity
        # Create default roles
        # Create default policy


    def post_datastore(self, config):
        # Make sure to detect that system was already bootstrapped.
        # Look in datastore for secret cookie\

        cookie_name = get_sys_name() + ".ION_INIT"
        try:
            res = self.clients.datastore.read_doc(cookie_name)
            log.error("System %s already initialized: %s" % (get_sys_name(), res))
            return
        except iex.NotFound:
            pass

        # Now set the secret cookie
        import time
        cookie = dict(container=self.container.id, time=time.time())
        cid, _ = self.clients.datastore.create_doc(cookie, cookie_name)

    def post_directory(self, config):
        # Load service definitions into directory
        # Load resource types into directory
        # Load object types into directory
        pass

    def post_resource_registry(self, config):
        for res in RT.keys():
            rt = IonObject("ResourceType", name=res)
            #self.clients.datastore.create(rt)

    def post_identity_management(self, config):

        #Create the ION System Agent user which should be passed in subsequent bootstraping calls
        system_actor = CFG.system.system_actor
        user = IonObject(RT.UserIdentity, name=system_actor, description="ION System Agent")
        self.clients.identity_management.create_user_identity(user)

    def post_org_management(self, config):

        system_actor = self.clients.identity_management.find_user_identity_by_name(name=CFG.system.system_actor)

        # Create root Org: ION
        root_orgname = CFG.system.root_org
        org = IonObject(RT.Org, name=root_orgname, description="ION Root Org")
        self.org_id = self.clients.org_management.create_org(org, headers={'ion-actor-id': system_actor._id})
        #TODO - May not want to make the system agent a manager if a real manager user will be seeded for the ION Org
        self.clients.org_management.grant_role(self.org_id,system_actor._id,MANAGER_ROLE, headers={'ion-actor-id': system_actor._id} )

    def post_exchange_management(self, config):

        system_actor = self.clients.identity_management.find_user_identity_by_name(name=CFG.system.system_actor)

        # find root org
        root_orgname = CFG.system.root_org      # @TODO: THIS CAN BE SPECIFIED ON A PER LAUNCH BASIS, HOW TO FIND?
        org = self.clients.org_management.find_org(name=root_orgname)

        # Create root ExchangeSpace
        xs = IonObject(RT.ExchangeSpace, name=ION_ROOT_XS, description="ION service XS")
        self.xs_id = self.clients.exchange_management.create_exchange_space(xs, org._id,headers={'ion-actor-id': system_actor._id})

        #self.clients.resource_registry.find_objects(self.org_id, "HAS-A")

        #self.clients.resource_registry.find_subjects(self.xs_id, "HAS-A")

    def post_startup(self):
        log.info("Cannot sanity check bootstrap yet, need better plan to sync local state (or pull from datastore?)")

#        # Do some sanity tests across the board
#        org_ids, _ = self.clients.resource_registry.find_resources(RT.Org, None, None, True)
#        self.assert_condition(len(org_ids) == 1 and org_ids[0] == self.org_id, "Orgs not properly defined")
#
#        xs_ids, _ = self.clients.resource_registry.find_resources(RT.ExchangeSpace, None, None, True)
#        self.assert_condition(len(xs_ids) == 1 and xs_ids[0] == self.xs_id, "ExchangeSpace not properly defined")
#
#        res_ids, _ = self.clients.resource_registry.find_objects(self.org_id, PRED.hasExchangeSpace, RT.ExchangeSpace, True)
#        self.assert_condition(len(res_ids) == 1 and res_ids[0] == self.xs_id, "ExchangeSpace not associated")
#
#        res_ids, _ = self.clients.resource_registry.find_subjects(RT.Org, PRED.hasExchangeSpace, self.xs_id, True)
#        self.assert_condition(len(res_ids) == 1 and res_ids[0] == self.org_id, "Org not associated")

    def on_quit(self):
        log.info("Bootstrap service QUIT: System quit")

