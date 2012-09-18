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

from ion.agents.platform.oms.simulator.oms_simulator import OmsSimulator
import xmlrpclib
import os


class OmsClientFactory(object):
    """
    Provides an OmsClient implementation.
    """

    @classmethod
    def create_instance(cls, uri=None):
        """
        Creates an OmsClient instance.

        @param uri URI to connect to the OMS server. If None (the
          default) the value of the OMS environment variable is used.

        Special values:
        "embsimulator": OmsSimulator instance created and returned
        "localsimulator": connect to OmsSimulator via XMP/RPC on http://localhost:7700/
        "simulator": connect to OmsSimulator via XMP/RPC on http://rsn-oms-simulator.oceanobservatories.org:7700/
        "rsn": connect to real OMS server on http://alice:1234@10.180.80.10:9021/

        Any other value is used as given to try connection with XML/RPC server.
        If the parameter is None and the environment variable OMS is not
        defined, a OmsSimulator instance created and returned.
        """

        if uri is None:
            uri = os.getenv('OMS')

        instance = None
        if uri:
            if "embsimulator" == uri:
                # "embedded" simulator, so instantiate OmsSimulator here:
                log.debug("Will use embedded OmsSimulator instance")
                instance = OmsSimulator()

            elif "localsimulator" == uri:
                # connect with OmsSimulator via XML/RPC on local host
                uri = "http://localhost:7700/"
                log.debug("Will connect to OmsSimulator via XMP/RPC on %s", uri)

            elif "simulator" == uri:
                # connect with OmsSimulator via XML/RPC on oceanobservatories host
                uri = "http://rsn-oms-simulator.oceanobservatories.org:7700/"
                log.debug("Will connect to OmsSimulator via XMP/RPC on %s", uri)

            elif "rsn" == uri:
                # connect to real OMS server on RSN
                uri ="http://alice:1234@10.180.80.10:9021/"
                log.debug("Will connect to real OMS server on %s", uri)

            #else: just use whatever URI was given.

        else:
            # use "embedded" simulator
            log.debug("OMS not defined; will use embedded OmsSimulator instance")
            instance = OmsSimulator()

        if (not instance) and uri:
            log.debug("Creating xmlrpclib.ServerProxy: uri=%s", uri)
            instance = xmlrpclib.ServerProxy(uri, allow_none=True)
            log.debug("Created xmlrpclib.ServerProxy: uri=%s", uri)

        assert instance is not None, "instance must be created here"
        return instance
