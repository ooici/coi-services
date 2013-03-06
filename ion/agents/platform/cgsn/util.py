#!/usr/bin/env python

"""
@package ion.agents.platform.cgsn.util
@file    ion/agents/platform/cgsn/util.py
@author  Carlos Rueda
@brief   Some common utilities.
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'


#
# TODO perhaps re-use some more appropriate exception class for malformed messages.
#
class MalformedMessage(Exception):
    pass


def basic_message_verification(message):
    """
    Does basic verification and conversion of the given message.

    @param  message A payload received from external endpoint via UDP.
    @return (dst, src, msg_type, msg)
    @raise  MalformedMessage received message is malformed
    """

    toks = message.split(',')

    #
    # 5 tokens expected
    #
    if len(toks) != 5:
        raise MalformedMessage(
            "received message is malformed: 5 tokens expected, "
            "got %d" % len(toks))

    #
    # first 4 tokens must be integers:
    #
    try:
        (dst, src, msg_type, lng) = (int(toks[i]) for i in range(4))
    except ValueError:
        raise MalformedMessage(
            "received message is malformed: "
            "first 4 tokens must be integers")

    msg = toks[4]

    if len(msg) != lng:
        raise MalformedMessage(
            "received message is malformed: length token %d != actual "
            "message length %d" % (lng, len(msg)))

    return dst, src, msg_type, msg
