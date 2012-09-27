#!/usr/bin/env python

"""
@package ion.agents.platform.oms.simulator.oms_alarms
@file    ion/agents/platform/oms/simulator/oms_alarms.py
@author  Carlos Rueda
@brief   OMS simulator alarm definitions
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'



class AlarmInfo(object):
    ALARM_TYPES = {

        '44.77': {
            'ref_id':        '44.77',
            'name':          'on battery',
            'severity':      3,
            'platform_type': 'UPS',
            'groups':        'power',
            },

        '44.78': {
            'ref_id':        '44.78',
            'name':          'low battery',
            'severity':      3,
            'platform_type': 'UPS',
            'groups':        'power',
            },
    }
