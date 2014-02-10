#!/usr/bin/env python

__author__ = 'Luke Campbell <LCampbell@ASAScience.com>'

from ion.core.bootstrap_process import BootstrapPlugin
from pyon.public import log
from pyon.util.containers import DotDict


class BootstrapIndex(BootstrapPlugin):
    """
    Bootstrap plugin for indexes and search
    """

    def on_initial_bootstrap(self, process, config, **kwargs):
        # There used to be initialization of ElasticSearch, now removed by using Postgres
        pass
