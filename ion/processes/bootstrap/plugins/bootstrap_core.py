#!/usr/bin/env python

__author__ = 'Michael Meisinger'

from ion.core.bootstrap_process import BootstrapPlugin, AbortBootstrap
from pyon.public import IonObject, RT
from pyon.util.containers import get_safe

from interface.objects import ActorIdentity, Org


class BootstrapCore(BootstrapPlugin):
    """
    Bootstrap plugin for core system resources.
    No service dependency
    """

    def on_initial_bootstrap(self, process, config, **kwargs):
        # Detect if system has been started before by the presence of the ION system actor
        system_actor, _ = process.container.resource_registry.find_resources(
            restype=RT.ActorIdentity, id_only=True)
        if system_actor:
            raise AbortBootstrap("System already initialized. Start with bootmode=restart or force_clean!")

        # Possibly start the event persister here


        # Create ION actor
        actor_name = get_safe(config, "system.system_actor", "ionsystem")
        sys_actor = ActorIdentity(name=actor_name, description="ION System Agent")
        process.container.resource_registry.create(sys_actor)

        webauth_actor_name = get_safe(config, "system.web_authentication_actor", "web_authentication")
        web_auth_actor = ActorIdentity(name=webauth_actor_name, description="Web Authentication Actor")
        process.container.resource_registry.create(web_auth_actor)

        # Store all resource types

        # Store all event types

    def on_restart(self, process, config, **kwargs):
        pass
