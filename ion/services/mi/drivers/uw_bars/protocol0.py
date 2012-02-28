#!/usr/bin/env python

"""
@package ion.services.mi.drivers.uw_bars.protocol0
@file ion/services/mi/drivers/uw_bars/protocol0.py
@author Carlos Rueda
@brief UW TRHPH BARS protocol implementation based on bars_client.
BarsInstrumentProtocol extends InstrumentProtocol but does not use the
connection mechanisms there; instead it uses the bars_client utility.
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'

from ion.services.mi.drivers.uw_bars.bars_client import BarsClient
import ion.services.mi.drivers.uw_bars.bars as bars

from ion.services.mi.instrument_protocol import InstrumentProtocol
from ion.services.mi.instrument_driver import DriverState
from ion.services.mi.common import InstErrorCode
from ion.services.mi.common import DriverAnnouncement

from ion.services.mi.drivers.uw_bars.common import BarsParameter

#import ion.services.mi.mi_logger
import logging
log = logging.getLogger('mi_logger')


class BarsInstrumentProtocol(InstrumentProtocol):
    """
    The protocol for the BarsChannel.INSTRUMENT channel.
    """

    def __init__(self, evt_callback=None):
        InstrumentProtocol.__init__(self, evt_callback)

        self.bars_client = None
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

    def initialize(self, *args, **kwargs):
        """
        """

        if log.isEnabledFor(logging.DEBUG):
            log.debug("args=%s kwargs=%s" %
                      (str(args), str(kwargs)))

        self._assert_state([DriverState.UNCONFIGURED,
                            DriverState.DISCONNECTED])

        super(BarsInstrumentProtocol, self).initialize(*args, **kwargs)

        self._state = DriverState.UNCONFIGURED

    def configure(self, config, *args, **kwargs):
        """
        Stores the given config object and set the current state as
        DriverState.DISCONNECTED.
        Note that super.configure is not called.
        """

        if log.isEnabledFor(logging.DEBUG):
            log.debug("config=%s args=%s kwargs=%s" %
                      (str(config), str(args), str(kwargs)))

        self._assert_state(DriverState.UNCONFIGURED)

        self.config = config

        self._state = DriverState.DISCONNECTED

    def connect(self, *args, **kwargs):
        """
        Sets up the bars_client and connects to the instrument.
        Note that super.connect is not called.
        """

        if log.isEnabledFor(logging.DEBUG):
            log.debug("args=%s kwargs=%s" %
                      (str(args), str(kwargs)))

        self._assert_state(DriverState.DISCONNECTED)

        self._setup_bars_client(self.config)

        if self.bars_client.is_collecting_data():
            self._state = DriverState.AUTOSAMPLE
        else:
            #
            # TODO proper handling
            raise Exception("Not handled yet. BARS not collecting data")

    def _setup_bars_client(self, config):
        config = self.config
        host = config['device_addr']
        port = config['device_port']
        outfile = file('driver0.txt', 'w')
        self.bars_client = BarsClient(host, port, outfile)
        self.bars_client.connect()

    def disconnect(self, *args, **kwargs):
        """
        Ends the bars_client.
        Note that super.disconnect is not called.
        """

        if log.isEnabledFor(logging.DEBUG):
            log.debug("args=%s kwargs=%s" %
                      (str(args), str(kwargs)))

        self.bars_client.end()
        self.bars_client = None

        self._state = DriverState.DISCONNECTED

    def detach(self, *args, **kwargs):
        """
        Does nothing.
        Note that super.detach is not called.
        """
        if log.isEnabledFor(logging.DEBUG):
            log.debug("args=%s kwargs=%s" %
                      (str(args), str(kwargs)))

    ########################################################################
    # Channel command interface.
    ########################################################################

    def get(self, params, *args, **kwargs):

        # TODO it only handles BarsParameter.TIME_BETWEEN_BURSTS

        if log.isEnabledFor(logging.DEBUG):
            log.debug("params=%s args=%s kwargs=%s" %
                      (str(params), str(args), str(kwargs)))

        self._assert_state(DriverState.AUTOSAMPLE)

        assert isinstance(params, list)

        params = list(set(params))  # remove any duplicates

        result = {}
        for param in params:
            if param == BarsParameter.TIME_BETWEEN_BURSTS:
                value = self._get_cycle_time()
            else:
                value = InstErrorCode.INVALID_PARAMETER
            result[param] = value

        return result

    def _get_cycle_time(self):

        log.debug("breaking data streaming to enter main menu")
        self.bars_client.enter_main_menu()

        log.debug("select 2 to get system parameter menu")
        self.bars_client.send('2')
        self.bars_client.expect_generic_prompt()

        buffer = self.bars_client.get_last_buffer()
        log.debug("BUFFER='%s'" % repr(buffer))
        string = bars.get_cycle_time(buffer)
        log.debug("VALUE='%s'" % string)
        seconds = bars.get_cycle_time_seconds(string)
        if seconds is None:
            # Unexpected: string could not be matched
            return InstErrorCode.MESSAGING_ERROR

        log.debug("send 3 to return to main menu")
        self.bars_client.send('3')
        self.bars_client.expect_generic_prompt()

        buffer = self.bars_client.get_last_buffer()
        log.debug("BUFFER='%s'" % repr(buffer))

        log.debug("resume data streaming")
        self.bars_client.send('1')

        return seconds

    def set(self, params, *args, **kwargs):
        """
        """
        # TODO it only handles BarsParameter.TIME_BETWEEN_BURSTS

        if log.isEnabledFor(logging.DEBUG):
            log.debug("params=%s args=%s kwargs=%s" %
                      (str(params), str(args), str(kwargs)))

        self._assert_state(DriverState.AUTOSAMPLE)

        assert isinstance(params, dict)

        updated_params = 0
        result = {}
        for (param, value) in params.items():
            if param == BarsParameter.TIME_BETWEEN_BURSTS:
                result[param] = self._set_cycle_time(value)
                if InstErrorCode.is_ok(result[param]):
                    updated_params += 1
            else:
                result[param] = InstErrorCode.INVALID_PARAMETER

        msg = "%s parameter(s) successfully set." % updated_params
        log.debug("announcing to driver: %s" % msg)
        self.announce_to_driver(DriverAnnouncement.CONFIG_CHANGE, msg=msg)
        return result

    def _set_cycle_time(self, seconds):
        if not isinstance(seconds, int):
            return InstErrorCode.INVALID_PARAM_VALUE

        if seconds < 15:
            return InstErrorCode.INVALID_PARAM_VALUE

        log.debug("break data streaming to enter main menu")
        self.bars_client.enter_main_menu()

        log.debug("select 2 to get system parameter menu")
        self.bars_client.send('2')
        self.bars_client.expect_generic_prompt()

        buffer = self.bars_client.get_last_buffer()
        log.debug("BUFFER='%s'" % repr(buffer))
        string = bars.get_cycle_time(buffer)
        log.debug("VALUE='%s'" % string)

        log.debug("send 1 to change cycle time")
        self.bars_client.send('1')
        self.bars_client.expect_generic_prompt()

        if seconds <= 59:
            log.debug("send 0 to change cycle time in seconds")
            self.bars_client.send('0')
            self.bars_client.expect_generic_prompt()
            log.debug("send seconds=%d" % seconds)
            self.bars_client.send(str(seconds))
        else:
            log.debug("send 1 to change cycle time in minutes")
            self.bars_client.send('1')
            self.bars_client.expect_generic_prompt()
            minutes = seconds / 60
            log.debug("send minutes=%d" % minutes)
            self.bars_client.send(str(minutes))

        self.bars_client.expect_generic_prompt()

        log.debug("send 3 to return to main menu")
        self.bars_client.send('3')
        self.bars_client.expect_generic_prompt()

        buffer = self.bars_client.get_last_buffer()
        log.debug("BUFFER='%s'" % repr(buffer))

        log.debug("resume data streaming")
        self.bars_client.send('1')

        return InstErrorCode.OK
