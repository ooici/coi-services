#!/usr/bin/env python

"""Process that bootstraps an ION system"""

__author__ = 'Michael Meisinger'

from pyon.ion.resource import RT_LIST
from pyon.public import CFG, IonObject, log, sys_name

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
        elif level == "org_management":
            self.post_org_management(config)
        elif level == "exchange_management":
            self.post_exchange_management(config)

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
        (cid, cv) = self.clients.datastore.create_doc(cookie, cookie_name)

    def post_directory(self, config):
        pass

    def post_resource_registry(self, config):
        for res in RT_LIST:
            rt = IonObject("ResourceType", dict(name=res))
            self.clients.datastore.create(rt)

    def post_org_management(self, config):
        # Create root Org: ION
        self.org_id = self.clients.org_management.create_org("ION")

    def post_exchange_management(self, config):
        # Create root ExchangeSpace
        self.xs_id = self.clients.exchange_management.create_exchange_space("ioncore", self.org_id)

        #self.clients.resource_registry.find_objects(self.org_id, "HAS-A")

        #self.clients.resource_registry.find_subjects(self.xs_id, "HAS-A")
    
    def on_quit(self):
        log.info("Bootstrap service QUIT: System quit")


def start(container, starttype, app_definition, config):
    print "****", config
    level = config.get("level", 0)
    log.debug("Bootstrap Trigger Level: %s" % level)
    bootstrap_instance.trigger_level(level, config)

    return (None, None)

def stop(container, state):
    pass
