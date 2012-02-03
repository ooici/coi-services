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

from ion.services.mi.instrument_driver import InstrumentDriver

from ion.services.mi.common import InstErrorCode


#import ion.services.mi.mi_logger
import logging
log = logging.getLogger('mi_logger')


#
# TODO this is not functional yet
#

class BarsInstrumentDriver(InstrumentDriver):
    """The InstrumentDriver class for the TRHPH BARS sensor.

    @see https://confluence.oceanobservatories.org/display/syseng/CIAD+SA+SV+Instrument+Driver+Interface
    """

    def __init__(self):
        InstrumentDriver.__init__(self)

        self.connection = None
        self.protocol = None

    def configure(self, config, timeout=10):
        # TODO harmonize with base class

        success = InstErrorCode.OK

        self._configure_connection(config)
        self._configure_protocol(config)

        return success

    def _configure_connection(self, config):
        # TODO
        self.connection = object()

    def _configure_protocol(self, config):
        # TODO use self.connection
        self.protocol = BarsInstrumentProtocol(config)
