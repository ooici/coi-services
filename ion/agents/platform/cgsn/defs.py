#!/usr/bin/env python

"""
@package 
@file 
@author Carlos Rueda
@brief 
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'


from ion.agents.instrument.common import BaseEnum

# identifies the CI process
CIPOP = 19

CICGINT = 112

EOL = '\n'


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
