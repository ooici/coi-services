#!/usr/bin/env python

"""
@package ion.services.mi.drivers.uw_trhph.common
@file ion/services/mi/drivers/uw_trhph/common.py
@author Carlos Rueda
@brief UW TRHPH common elements.
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'

from ion.services.mi.common import BaseEnum
from ion.services.mi.instrument_driver import DriverChannel


class TrhphCommand(BaseEnum):
    # TODO get-metadata operation still to be defined
    #GET_METADATA = DriverCommand.GET_METADATA
    pass


class TrhphChannel(BaseEnum):
    # TODO Concept of "instrument channel" is under revision.

    ALL = DriverChannel.ALL

    # note: the driver only creates a single protocol object,
    # which is associated to the INSTRUMENT special "channel."
    INSTRUMENT = DriverChannel.INSTRUMENT


class TrhphParameter(BaseEnum):
    TIME_BETWEEN_BURSTS = 'TIME_BETWEEN_BURSTS'
    VERBOSE_MODE = 'VERBOSE_MODE'


class TrhphMetadataParameter(BaseEnum):
    SYSTEM_NAME = "System Name"
    SYSTEM_OWNER = "System Owner"
    OWNER_CONTACT_PHONE = "Owner Contact Phone #"
    SYSTEM_SERIAL = "System Serial #"


class TrhphStatus(BaseEnum):
    pass


class TrhphError(BaseEnum):
    pass


class TrhphCapability(BaseEnum):
    pass
