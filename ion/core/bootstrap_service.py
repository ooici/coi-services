#!/usr/bin/env python

"""Process that bootstraps an ION system"""

__author__ = 'Michael Meisinger'

from pyon.public import CFG, IonObject, log, sys_name, RT, LCS, AT

from interface.services.ibootstrap_service import BaseBootstrapService

bootstrap_instance = None

class BootstrapService(BaseBootstrapService):
    """
    Bootstrap service: This service will initialize the ION system environment.
    This service is triggered for each boot level.
    """

    def on_init(self):
        log.info("Bootstrap service INIT: System init")
        global bootstrap_instance
        bootstrap_instance = self
        self.level_seen = set()

    def on_start(self):
        log.info("Bootstrap service START: System start")

    def trigger_level(self, level, config):
        print "YYYYYYYYYYYYYYYYYYY level: %s config: %s" % (str(level),str(config))
        if level in self.level_seen:
            log.error("Bootstrap level already triggered: %s" % level)
            return

        self.level_seen.add(level)
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
        cookie_name = sys_name + ".ION_INIT"
        res = self.clients.datastore.read_doc(cookie_name)
        if res:
            log.error("System %s already initialized: %s" % (sys_name, res))
            return

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
        # TBD
        pass

    def post_org_management(self, config):
        # Create root Org: ION
        org = IonObject(RT.Org, name="ION", description="ION Root Org")
        self.org_id = self.clients.org_management.create_org(org)

    def post_exchange_management(self, config):
        # Create root ExchangeSpace
        xs = IonObject(RT.ExchangeSpace, name="ioncore", description="ION service XS")
        self.xs_id = self.clients.exchange_management.create_exchange_space(xs, self.org_id)

        #self.clients.resource_registry.find_objects(self.org_id, "HAS-A")

        #self.clients.resource_registry.find_subjects(self.xs_id, "HAS-A")

    def post_startup(self):
        # Do some sanity tests across the board
        org_ids, _ = self.clients.resource_registry.find_resources(RT.Org, None, None, True)
        assert len(org_ids) == 1 and org_ids[0] == self.org_id, "Orgs not properly defined"

        xs_ids, _ = self.clients.resource_registry.find_resources(RT.ExchangeSpace, None, None, True)
        assert len(xs_ids) == 1 and xs_ids[0] == self.xs_id, "ExchangeSpace not properly defined"

        res_ids, _ = self.clients.resource_registry.find_objects(self.org_id, AT.hasExchangeSpace, RT.ExchangeSpace, True)
        assert len(res_ids) == 1 and res_ids[0] == self.xs_id, "ExchangeSpace not associated"

        res_ids, _ = self.clients.resource_registry.find_subjects(RT.Org, AT.hasExchangeSpace, self.xs_id, True)
        assert len(res_ids) == 1 and res_ids[0] == self.org_id, "Org not associated"

    def on_quit(self):
        log.info("Bootstrap service QUIT: System quit")


def start(container, starttype, app_definition, config):
    print "XXXXXXXXXXXXXX starttype: %s app_definition: %s config: %s" % (str(starttype),str(app_definition),str(config))
    level = config.get("level", 0)
    log.debug("Bootstrap Trigger Level: %s" % level)
    bootstrap_instance.trigger_level(level, config)

    return (None, None)

def stop(container, state):
    pass
