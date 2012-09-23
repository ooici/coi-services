#!/usr/bin/env python

"""Bootstrap process for services"""

__author__ = 'Michael Meisinger'

from ion.core.bootstrap_process import BootstrapPlugin


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
