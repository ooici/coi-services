#!/usr/bin/env python

"""
@package ion.agents.platform.cgsn.cgsn_client_factory
@file    ion/agents/platform/cgsn/cgsn_client_factory.py
@author  Carlos Rueda
@brief   Cgsn Client factory.
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'


from pyon.public import log
import logging

from ion.agents.platform.cgsn.cgsn_client import CGSNClient
import os
import yaml

_URI_ALIASES_FILENAME = 'ion/agents/platform/cgsn/cgsn_uri_aliases.yml'


class CGSNClientFactory(object):
    """
    Provides a CGSNClient implementation.
    """

    _uri_aliases = None

    @classmethod
    def _load_uri_aliases(cls):
        try:
            cls._uri_aliases = yaml.load(file(_URI_ALIASES_FILENAME))
            if log.isEnabledFor(logging.TRACE):
                log.trace("Loaded CGSN URI aliases = %s" % cls._uri_aliases)
        except Exception as e:
            log.warn("Cannot loaded %s: %s" % (_URI_ALIASES_FILENAME, e))
            cls._uri_aliases = {}

    @classmethod
    def create_instance(cls, uri=None):
        """
        Creates an CGSNClient instance.

        @param uri URI to connect to the CGSN Services endpoint.
        """

        if cls._uri_aliases is None:
            cls._load_uri_aliases()

        uri = uri or os.getenv('CGSN', 'localsimulator')

        # try alias resolution and then create CGSNClient instance:
        uri = cls._uri_aliases.get(uri, uri)
        host, port = tuple(uri.split(':'))
        address = (host, int(port))
        instance = CGSNClient(address)
        if log.isEnabledFor(logging.DEBUG):
            log.debug("Created CGSNClient for endpoint at: %s:%i" % address)

        return instance
