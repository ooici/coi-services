#!/usr/bin/env python

"""
@package ion.agents.platform.util
@file    ion/agents/platform/util/__init__.py
@author  Carlos Rueda
@brief   Some utilities / placeholders
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'


import ntplib


def ion_ts_2_ntp(ion_ts):
    """
    Converts an ION system timestamp into NTP.
    The converse operation is ntp_2_ion_ts(ntp_time).

    Note: this should probably be a utility provided by pyon.

    @see https://jira.oceanobservatories.org/tasks/browse/OOIION-631

    @param ion_ts
            "a str representing an integer number, the millis in UNIX epoch"
            according to description of pyon's get_ion_ts function as of
            2013-01-08. See:
            https://github.com/ooici/pyon/blob/6a9e4199db1e9/pyon/util/containers.py#L243-248

    @retval float corresponding to NTP calculated as
            ntplib.system_to_ntp_time(float(ion_ts) / 1000)
    """

    # convert to seconds:
    sys_time = float(ion_ts) / 1000

    # convert to NTP
    ntp_time = ntplib.system_to_ntp_time(sys_time)

    return ntp_time


def ntp_2_ion_ts(ntp_time):
    """
    Converts an NTP time into ION system timestamp.
    The converse operation is ion_ts_2_ntp(ion_ts).

    Note: this should probably be a utility provided by pyon.

    @see https://jira.oceanobservatories.org/tasks/browse/OOIION-631

    @param ntp_time
            float representing an NTP time.

    @retval str representing an integer number, the millis in UNIX epoch,
            corresponding to the given NTP time. This is calculated as
            str( int(round(ntplib.ntp_to_system_time(ntp_time) * 1000)) )
    """

    # convert to system time:
    sys_time = ntplib.ntp_to_system_time(ntp_time)

    # convert to milliseconds rounding to the nearest integer:
    sys_time = int(round(sys_time * 1000))

    # return as str
    return str(sys_time)
