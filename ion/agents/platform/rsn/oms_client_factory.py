#!/usr/bin/env python

"""
@package ion.agents.platform.rsn.oms_client_factory
@file    ion/agents/platform/rsn/oms_client_factory.py
@author  Carlos Rueda
@brief   CIOMS Client factory.
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'


from pyon.public import log
import logging

from ion.agents.platform.rsn.simulator.oms_simulator import CIOMSSimulator
import xmlrpclib
import os
import yaml

_OMS_URI_ALIASES_FILENAME = 'ion/agents/platform/rsn/oms_uri_aliases.yml'


class CIOMSClientFactory(object):
    """
    Provides a CIOMSClient implementation.
    """

    # counter of created, not-yet-destroyed instances for debugging
    _inst_count = 0

    _uri_aliases = None

    # _sim_process: see launch_simulator and related methods
    _sim_process = None

    @classmethod
    def _load_uri_aliases(cls):
        try:
            cls._uri_aliases = yaml.load(file(_OMS_URI_ALIASES_FILENAME))
            if log.isEnabledFor(logging.DEBUG):
                log.debug("Loaded OMS URI aliases = %s" % cls._uri_aliases)
        except Exception as e:
            log.warn("Cannot loaded %s: %s" % (_OMS_URI_ALIASES_FILENAME, e))
            cls._uri_aliases = {}

    @classmethod
    def create_instance(cls, uri=None):
        """
        Creates an CIOMSClient instance.
        Call destroy_instance with the return object when no longer needed.

        @param uri URI to connect to the RSN OMS server or simulator.
        If None (the default) the value of the OMS environment variable is used
        as argument. If not defined or if the resulting argument is "embsimulator"
        then an CIOMSSimulator instance is created and returned. Otherwise, the
        argument is looked up in the OMS URI aliases file and if found the
        corresponding URI is used for the connection. Otherwise, the given
        argument (or value of the OMS environment variable) is used as given
        to try the connection with corresponding XML/RPC server.
        """

        if cls._uri_aliases is None:
            cls._load_uri_aliases()

        if uri is None:
            uri = os.getenv('OMS', 'embsimulator')

        if "embsimulator" == uri:
            # "embedded" simulator, so instantiate CIOMSSimulator here:
            log.debug("Using embedded CIOMSSimulator instance")
            instance = CIOMSSimulator()
        else:
            # try alias resolution and then create ServerProxy instance:
            uri = cls._uri_aliases.get(uri, uri)
            if log.isEnabledFor(logging.DEBUG):
                log.debug("Creating xmlrpclib.ServerProxy: uri=%s", uri)
            instance = xmlrpclib.ServerProxy(uri, allow_none=True)
            if log.isEnabledFor(logging.DEBUG):
                log.debug("Created xmlrpclib.ServerProxy: uri=%s", uri)

        cls._inst_count += 1
        log.debug("create_instance: _inst_count = %d", cls._inst_count)
        return instance

    @classmethod
    def destroy_instance(cls, instance):
        """
        Destroys an instance created with create_instance.
        This is mainly a convenience method to deactivate the simulator when
        run in embedded form.
        """
        cls._inst_count -= 1
        if isinstance(instance, CIOMSSimulator):
            instance._deactivate_simulator()
            log.debug("Embedded CIOMSSimulator instance destroyed")

        # else: nothing needed to do.
            
        log.debug("destroy_instance: _inst_count = %d", cls._inst_count)

    @classmethod
    def launch_simulator(cls):
        """
        Utility to launch the simulator as a separate process.

        @return the new URI for a regular call to create_instance(uri).
        """
        from ion.agents.platform.rsn.simulator.process_util import ProcessUtil
        cls._sim_process = ProcessUtil()
        cls._sim_process.launch()
        return "localsimulator"

    @classmethod
    def stop_launched_simulator(cls):
        """
        Utility to stop the process launched with launch_simulator.
        """
        if cls._sim_process:
            log.debug("stopping launched simulator...")
            cls._sim_process.stop()
            cls._sim_process = None
