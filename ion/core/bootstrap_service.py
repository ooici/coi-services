#!/usr/bin/env python

"""Process that bootstraps an ION system"""

__author__ = 'Michael Meisinger'

from pyon.core.bootstrap import sys_name
from pyon.datastore.datastore import DataStore
from pyon.public import CFG
from pyon.util.log import log

from interface.services.ibootstrap_service import BaseBootstrapService

class BootstrapService(BaseBootstrapService):
    """
    Bootstrap service: This service will initialize the ION system environment.
    This service depends on datastore and directory services started locally before.
    """

    def on_init(self):
        log.info("Bootstrap service INIT: System init")

    def on_start(self):
        log.info("Bootstrap service START: System start")

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

        # Load resource types

        # Fill the directory

    def on_quit(self):
        log.info("Bootstrap service QUIT: System quit")
