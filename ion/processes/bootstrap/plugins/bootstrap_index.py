#!/usr/bin/env python

__author__ = 'Luke Campbell <LCampbell@ASAScience.com>'

from ion.core.bootstrap_process import BootstrapPlugin
from pyon.public import log
from pyon.util.containers import DotDict


class BootstrapPolicy(BootstrapPlugin):
    """
    Bootstrap plugin for indexes and search
    """

    def on_initial_bootstrap(self, process, config, **kwargs):
        if config.get_safe('system.elasticsearch'):
            #---------------------------------------------
            # Spawn the index bootstrap
            #---------------------------------------------
            config = DotDict(config)
            config.op                   = 'clean_bootstrap'

            process.container.spawn_process('index_bootstrap','ion.processes.bootstrap.index_bootstrap','IndexBootStrap',config)
            #---------------------------------------------
        else:
            log.info("Not creating the ES indexes.")
