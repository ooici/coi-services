#!/usr/bin/env python

"""
@package ion.agents.platform.cgsn.defs
@file    ion/agents/platform/cgsn/defs.py
@author  Carlos Rueda
@brief   Some definitions and associated utilities.
         This is very preliminary
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'


import os
from pyon.agent.common import BaseEnum

# identifies the CI process
CIPOP = 19

CICGINT = 112


# The default CG CLIENT address to set up listener to receive messages from
# the CG services endpoint in the cgsn_client module.
# This value corresponds to my laptop within the MBARI network, with a port
# that has been opened for this project.
#
# TODO Put this stuff in appropriate CI configuration resource.
#
DEFAULT_CG_CLIENT_ADDRESS = "134.89.13.60:10011"


def get_cg_client_address():
    """
    Gets the address associated with the client (used to set up listener to
    receive messages from the CG services endpoint in the cgsn_client module,
    and in the simulator to where to send responses back).
    By default, this address is DEFAULT_CG_CLIENT_ADDRESS.
    The CG_CLIENT environment variable allows to change this value,
    for example, to facilitate local testing:
       CG_CLIENT=localhost:10011
    """
    cg_client = os.getenv("CG_CLIENT", DEFAULT_CG_CLIENT_ADDRESS)
    host, port = tuple(cg_client.split(":"))
    return host, int(port)


class DclIds(BaseEnum):
    DCL11               = 31


class MessageIds(BaseEnum):
    ACK                 = "ACK"
    NACK                = "NACK"
    CI_INST_STATUS      = "CI_INST_STATUS"
    CI_SYS_STATUS       = "CI_SYS_STATUS"
    PORT_ONOFF          = "PORT_ONOFF"
    PORT_SET_CONF       = "PORT_SET_CONF"
    INFRA_REQ           = "INFRA_REQ"
    PSC_STATUS          = "PSC_STATUS"
    FBB_STATUS          = "FBB_STATUS"
    CPM_STATUS          = "CPM_STATUS"
    DCL_STATUS          = "DCL_STATUS"
    PPS_STATUS          = "PPS_STATUS"
    PORT_CONF_STATUS    = "PORT_CONF_STATUS"
    TELEM_AVAIL         = "TELEM_AVAIL"
    CI_CONTROL          = "CI_CONTROL"
