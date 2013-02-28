#!/usr/bin/env python

"""
@package ion.agents.platform.cgsn.cgsn_listener
@file    ion/agents/platform/cgsn/cgsn_listener.py
@author  Carlos Rueda
@brief   CgsnState
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'


from pyon.public import log
import logging
import re

from ion.agents.platform.cgsn.defs import CIPOP, CICGINT


class CgsnState(object):
    """
    Keeps a state of the CGSN system, which is updated upon reception of
    lines from the services endpoint.
    """

    def __init__(self):
        """
        """
        # ACK/NACK reception for each command, eg, the entry
        #    (31, 'DCL_STATUS') : 'ACK'
        # means that a ACK was received for a DCL11's DCL_STATUS request.

        self._an = {}

        if log.isEnabledFor(logging.DEBUG):
            log.debug("CgsnState created.")

    def get_ack_nack(self, dcl_id, cmd):
        """
        Returns the ACK/NACK status for (dcl_id, cmd). This status is cleared
        up right away.
        """
        if (dcl_id, cmd) in self._an:
            res = self._an[(dcl_id, cmd)]
            del self._an[(dcl_id, cmd)]
        else:
            res = None
        return res

    def listener(self, line):
        """
        The line listener
        """
        if log.isEnabledFor(logging.DEBUG):
            log.debug("CgsnState.listener called. line=%r" % line)

        toks = line.split(',')
        (dst, src, nnn, lng) = tuple(int(toks[i]) for i in range(4))
        msg = toks[4]

        assert CIPOP == dst
        assert CICGINT == nnn
        assert lng == len(msg)

        m = re.match(r"(ACK|NACK) (.*)", msg)
        if m:
            an = m.group(1)
            cmd = m.group(2)
            key = (src, cmd)
            self._an[key] = an
            if log.isEnabledFor(logging.DEBUG):
                log.debug("Set %s to %s" % (str(key), an))
        else:
            # TODO handle remaining cases
            pass
