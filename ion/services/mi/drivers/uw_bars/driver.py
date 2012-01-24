#!/usr/bin/env python

"""
@package ion.services.mi.drivers.uw_bars.driver UW TRHPH BARS driver module
@file ion/services/mi/drivers/uw_bars/driver.py
@author Carlos Rueda
@brief UW TRHPH BARS driver
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'

from ion.services.mi.drivers.uw_bars.protocol import BarsInstrumentProtocol
from ion.services.mi.drivers.uw_bars.common import BarsCapability, \
    BarsChannel, BarsError, BarsMetadataParameter, BarsParameter, \
    BarsStatus, BarsCommand

from ion.services.mi.instrument_driver import InstrumentDriver
from ion.services.mi.instrument_connection import SerialInstrumentConnection
from ion.services.mi.comms_method import AMQPCommsMethod


#import ion.services.mi.mi_logger
import logging
log = logging.getLogger('mi_logger')


class BarsInstrumentDriver(InstrumentDriver):
    """The InstrumentDriver class for the TRHPH BARS sensor"""

    def __init__(self):
        self.instrument_connection = SerialInstrumentConnection()
        self.protocol = BarsInstrumentProtocol(self.instrument_connection)

        self.comms_method = AMQPCommsMethod()
        self.instrument_commands = BarsCommand()
        self.instrument_metadata_parameters = BarsMetadataParameter()
        self.instrument_parameters = BarsParameter()
        self.instrument_channels = BarsChannel()
        self.instrument_errors = BarsError()
        self.instrument_capabilities = BarsCapability()
        self.instrument_status = BarsStatus()
