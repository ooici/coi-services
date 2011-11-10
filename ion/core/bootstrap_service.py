#!/usr/bin/env python

"""Process that bootstraps an ION system"""

__author__ = 'Michael Meisinger'

from pyon.public import CFG
from pyon.util.log import log

from interface.services.ibootstrap_service import BaseBootstrapService

class BootstrapService(BaseBootstrapService):

    def on_init(self):
        log.info("Bootstrap service INIT: System init")

    def on_start(self):
        log.info("Bootstrap service START: System start")

        # Make sure to detect that system was already bootstrap.
        # Look in datastore for secret cookie - how without running datastore. We are first??

    def on_quit(self):
        log.info("Bootstrap service QUIT: System quit")
