#!/usr/bin/env python

from ion.core.bootstrap_process import BootstrapPlugin
from pyon.public import Container


class BootstrapParameters(BootstrapPlugin):
    """
    Bootstrap process for loading parameters
    """

    def on_initial_bootstrap(self, process, config, **kwargs):
        """
        Just launches the IONLoader bootstrap process to do the bootstrapping
        """
        Container.instance.spawn_process('ion_loader1', 'ion.processes.bootstrap.ion_loader', 'IONLoader', config)

