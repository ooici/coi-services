#!/usr/bin/env python

"""
@package ion.services.mi.instrument_agent Instrument resource agent
@file ion/services/mi/instrument_agebt.py
@author Edward Hunter
@brief Resource agent derived class providing an instrument agent as a resource.
This resource fronts instruments and instrument drivers one-to-one in ION.
"""

__author__ = 'Edward Hunter'
__license__ = 'Apache 2.0'

from pyon.core.exception import BadRequest, NotFound
from pyon.public import IonObject, AT, log
from pyon.agent.agent import ResourceAgent


class InstrumentAgent(ResourceAgent):
    pass










