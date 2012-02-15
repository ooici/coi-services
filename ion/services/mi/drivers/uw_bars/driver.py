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
from ion.services.mi.instrument_driver import DriverState

from ion.services.mi.common import InstErrorCode
from ion.services.mi.drivers.uw_bars.common import BarsChannel
from ion.services.mi.drivers.uw_bars.common import BarsParameter
from ion.services.mi.drivers.uw_bars.protocol import BarsProtocolState


#import ion.services.mi.mi_logger
import logging
log = logging.getLogger('mi_logger')


class BarsInstrumentDriver(InstrumentDriver):
    """
    The InstrumentDriver class for the TRHPH BARS sensor.
    """

    # TODO actual handling of the "channel" concept in the design.

    # TODO harmonize with base class.

    # TODO NOTE: Assumes all interaction is for the INSTRUMENT special channel

    def __init__(self, evt_callback=None):
        InstrumentDriver.__init__(self, evt_callback)

        self.connection = None
        self.protocol = None
        self.config = None

        self._state = DriverState.UNCONFIGURED

    def get_current_state(self):
        return self._state

    def _assert_state(self, obj):
        """
        Asserts that the current state is the same as the one given (if not
        a list) or is one of the elements of the given list.
        """
        cs = self.get_current_state()
        if isinstance(obj, list):
            if cs in obj:
                return
            else:
                raise AssertionError("current state=%s, expected one of %s" %
                                 (cs, str(obj)))
        state = obj
        if cs != state:
            raise AssertionError("current state=%s, expected=%s" % (cs, state))

    def initialize(self, channels, *args, **kwargs):
        """
        """

        if log.isEnabledFor(logging.DEBUG):
            log.debug("channels=%s args=%s kwargs=%s" %
                      (str(channels), str(args), str(kwargs)))

        self._assert_state([DriverState.UNCONFIGURED,
                            DriverState.DISCONNECTED])

        assert len(channels) == 1
        assert channels[0] == BarsChannel.INSTRUMENT

        result = None

        self._state = DriverState.UNCONFIGURED

        return result

    def configure(self, configs, *args, **kwargs):
        """
        """

        if log.isEnabledFor(logging.DEBUG):
            log.debug("configs=%s args=%s kwargs=%s" %
                      (str(configs), str(args), str(kwargs)))

        self._assert_state(DriverState.UNCONFIGURED)

        assert isinstance(configs, dict)
        assert len(configs) == 1
        assert BarsChannel.INSTRUMENT in configs

        self.config = configs.get(BarsChannel.INSTRUMENT, None)

        result = None

        self._state = DriverState.DISCONNECTED

        return result

    def connect(self, channels, *args, **kwargs):
        """
        """

        if log.isEnabledFor(logging.DEBUG):
            log.debug("channels=%s args=%s kwargs=%s" %
                      (str(channels), str(args), str(kwargs)))

        self._assert_state(DriverState.DISCONNECTED)

        assert len(channels) == 1
        assert channels[0] == BarsChannel.INSTRUMENT

        result = None

        self._setup_protocol(self.config)

        prot_state = self.protocol.get_current_state()
        if prot_state == BarsProtocolState.COLLECTING_DATA:
            self._state = DriverState.AUTOSAMPLE
        else:
            #
            # TODO proper handling
            raise Exception("Not handled yet. Expecting protocol to be in %s" %
                            BarsProtocolState.COLLECTING_DATA)

        return result

    def _setup_protocol(self, config):
        self.protocol = BarsInstrumentProtocol()
        self.protocol.configure(self.config)
        self.protocol.connect()

    def disconnect(self, channels, *args, **kwargs):
        """
        """

        if log.isEnabledFor(logging.DEBUG):
            log.debug("channels=%s args=%s kwargs=%s" %
                      (str(channels), str(args), str(kwargs)))

        assert len(channels) == 1
        assert channels[0] == BarsChannel.INSTRUMENT

        result = None

        self.protocol.disconnect()
        self.protocol = None

        self._state = DriverState.DISCONNECTED

        return result

    def detach(self, channels, *args, **kwargs):
        """
        """
        pass

    ########################################################################
    # Channel command interface.
    ########################################################################

    def get(self, params, *args, **kwargs):

        if log.isEnabledFor(logging.DEBUG):
            log.debug("params=%s args=%s kwargs=%s" %
                      (str(params), str(args), str(kwargs)))

        result = self.protocol.get(params, *args, **kwargs)

        return result

    def set(self, params, *args, **kwargs):
        """
        """
        if log.isEnabledFor(logging.DEBUG):
            log.debug("params=%s args=%s kwargs=%s" %
                      (str(params), str(args), str(kwargs)))


        pass

    def execute(self, channels, *args, **kwargs):
        """
        """
        if log.isEnabledFor(logging.DEBUG):
            log.debug("channels=%s args=%s kwargs=%s" %
                      (str(channels), str(args), str(kwargs)))

        pass

    def execute_direct(self, channels, *args, **kwargs):
        """
        """
        if log.isEnabledFor(logging.DEBUG):
            log.debug("channels=%s args=%s kwargs=%s" %
                      (str(channels), str(args), str(kwargs)))

        pass

    def get_channels(self):
        """
        """
        return BarsChannel.list()

    ########################################################################
    # TBD.
    ########################################################################

    def get_status(self, params, timeout=10):
        """
        @param timeout Number of seconds before this operation times out
        """

        # TODO for the moment just returning the current driver state
        return self.get_current_state()

    def get_capabilities(self, params, timeout=10):
        """
        @param timeout Number of seconds before this operation times out
        """
        pass
