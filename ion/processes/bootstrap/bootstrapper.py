#!/usr/bin/env python

"""Bootstrap process that can execute bootstrap plugins"""

from ion.core.bootstrap_process import BootstrapProcess, AbortBootstrap
from pyon.util.containers import get_safe, for_name, dict_merge

from pyon.public import log, RT
from pyon.core.governance import get_system_actor_header

class Bootstrapper(BootstrapProcess):
    """
    Extensible bootstrap process.
    """

    def on_initial_bootstrap(self, process, config, **kwargs):
        self._call_plugins("on_initial_bootstrap", process, config, **kwargs)

    def on_restart(self, process, config, **kwargs):
        self._call_plugins("on_restart", process, config, **kwargs)

    def _call_plugins(self, method, process, config, **kwargs):
        bootstrap_plugins = config.get_safe("bootstrap_plugins", None)
        if bootstrap_plugins is None:
            log.warn("Bootstrapper called without bootstrap_plugins config")

        # Finding the system actor ID. If found, construct call context headers.
        # This may be called very early in bootstrap with no system actor yet existing
        system_actor, _ = process.container.resource_registry.find_resources(
            RT.ActorIdentity, name=self.CFG.system.system_actor, id_only=False)

        actor_headers = get_system_actor_header(system_actor[0] if system_actor else None)

        # Set the call context of the current process
        with process.push_context(actor_headers):

            for plugin_info in bootstrap_plugins:
                plugin_mod, plugin_cls = plugin_info.get("plugin", [None,None])
                plugin_cfg = plugin_info.get("config", None)
                plugin_cfg = dict_merge(config, plugin_cfg) if plugin_cfg is not None else config

                try:
                    log.info("Bootstrapping plugin %s.%s ...", plugin_mod, plugin_cls)
                    plugin = for_name(plugin_mod, plugin_cls)
                    plugin_func = getattr(plugin, method)
                    plugin_func(process, plugin_cfg, **kwargs)
                except AbortBootstrap as abort:
                    raise
                except Exception as ex:
                    log.exception("Error bootstrapping plugin %s.%s", plugin_mod, plugin_cls)
