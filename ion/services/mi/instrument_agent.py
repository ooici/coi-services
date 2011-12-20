#!/usr/bin/env python

"""
@package examples.bank.bank_service Implementation of IBankService inteface
@file examples/bank/bank_service.py
@author Thomas R. Lennan
@brief Example service that provides basic banking functionality.
This service tracks customers and their accounts (checking or saving)
"""

__author__ = 'Edward Hunter'
__license__ = 'Apache 2.0'

from pyon.core.exception import BadRequest, NotFound
from pyon.public import IonObject, AT, log
from pyon.agent.agent import ResourceAgent



class InstrumentAgent(ResourceAgent):
    pass










