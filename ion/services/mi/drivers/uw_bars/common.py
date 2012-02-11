#!/usr/bin/env python

"""
@package ion.services.mi.drivers.uw_bars.common UW TRHPH BARS common module
@file ion/services/mi/drivers/uw_bars/common.py
@author Carlos Rueda
@brief Some common elements.
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'

from ion.services.mi.common import BaseEnum
from ion.services.mi.instrument_driver import DriverChannel


class BarsCommand(BaseEnum):
    pass


class BarsChannel(BaseEnum):
    RESISTIVITY_5 = "Resistivity/5"
    RESISTIVITY_X1 = "Resistivity X1"
    RESISTIVITY_X5 = "Resistivity X5"
    HYDROGEN_5 = "Hydrogen/5"
    HYDROGEN_X1 = "Hydrogen X1"
    HYDROGEN_X5 = "Hydrogen X5"
    EH_SENSOR = "Eh Sensor"
    REFERENCE_TEMP_VOLTS = "Reference Temp Volts"
    REFERENCE_TEMP_DEG_C = "Reference Temp Deg C"
    RESISTIVITY_TEMP_VOLTS = "Resistivity Temp Volts"
    RESISTIVITY_TEMP_DEG_C = "Resistivity Temp Deg C"
    BATTERY_VOLTAGE = "Battery Voltage"

    ALL = DriverChannel.ALL
    INSTRUMENT = DriverChannel.INSTRUMENT


class BarsStatus(BaseEnum):
    pass


class BarsMetadataParameter(BaseEnum):
    pass


class BarsParameter(BaseEnum):
    TIME_BETWEEN_BURSTS = 'TIME_BETWEEN_BURSTS'
    pass


class BarsError(BaseEnum):
    pass


class BarsCapability(BaseEnum):
    pass
