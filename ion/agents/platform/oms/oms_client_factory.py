#!/usr/bin/env python

"""
@package ion.agents.platform.oms.oms_client_factory
@file    ion/agents/platform/oms/oms_client_factory.py
@author  Carlos Rueda
@brief   OMS Client factory.
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'


from pyon.public import log
import logging

from ion.agents.platform.oms.simulator.oms_simulator import OmsSimulator
import xmlrpclib
import os
import yaml

_OMS_URI_ALIASES_FILENAME = 'ion/agents/platform/oms/oms_uri_aliases.yml'


class OmsClientFactory(object):
    """
    Provides an OmsClient implementation.
    """

    _uri_aliases = None

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
        Creates an OmsClient instance.

        @param uri URI to connect to the RSN OMS server or simulator.
        If None (the default) the value of the OMS environment variable is used
        as argument. If not defined or if the resulting argument is "embsimulator"
        then an OmsSimulator instance is created and returned. Otherwise, the
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
            # "embedded" simulator, so instantiate OmsSimulator here:
            log.debug("Using embedded OmsSimulator instance")
            instance = OmsSimulator()
        else:
            # try alias resolution and then create ServerProxy instance:
            uri = cls._uri_aliases.get(uri, uri)
            if log.isEnabledFor(logging.DEBUG):
                log.debug("Creating xmlrpclib.ServerProxy: uri=%s", uri)
            instance = xmlrpclib.ServerProxy(uri, allow_none=True)
            if log.isEnabledFor(logging.DEBUG):
                log.debug("Created xmlrpclib.ServerProxy: uri=%s", uri)

        return instance
