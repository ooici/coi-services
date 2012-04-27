#!/usr/bin/env python

"""
@package ion.services.mi.drivers.uw_trhph.trhph_driver
@file ion/services/mi/drivers/uw_trhph/trhph_driver.py
@author Carlos Rueda
@brief UW TRHPH driver implementation based on trhph_client
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'


from ion.services.mi.common import BaseEnum
from ion.services.mi.instrument_driver import DriverState

from ion.services.mi.drivers.uw_trhph.trhph_client import TrhphClient
from ion.services.mi.drivers.uw_trhph.trhph_client import TrhphClientException
from ion.services.mi.drivers.uw_trhph.trhph_client import TimeoutException

from ion.services.mi.drivers.uw_trhph.common import TrhphParameter

from ion.services.mi.instrument_driver import InstrumentDriver
from ion.services.mi.instrument_driver import DriverParameter

from ion.services.mi.common import InstErrorCode

from ion.services.mi.instrument_driver import DriverConnectionState
from ion.services.mi.exceptions import NotImplementedException
from ion.services.mi.exceptions import InstrumentException
from ion.services.mi.exceptions import InstrumentStateException
from ion.services.mi.exceptions import InstrumentParameterException
from ion.services.mi.exceptions import InstrumentTimeoutException

import time

import logging
from ion.services.mi.mi_logger import mi_logger
log = mi_logger


class TrhphDriverState(BaseEnum):
    """
    TRHPH driver states.
    """

    UNCONFIGURED = DriverState.UNCONFIGURED
    DISCONNECTED = DriverState.DISCONNECTED
    CONNECTED = DriverState.CONNECTED


class TrhphInstrumentDriver(InstrumentDriver):
    """
    TRHPH driver
    """
    def __init__(self, evt_callback):
        """
        Constructor.
        @param evt_callback Driver process event callback.
        """
        InstrumentDriver.__init__(self, evt_callback)

        # _trhph_client created in configure()
        self._trhph_client = None

        self._state = TrhphDriverState.UNCONFIGURED

        # TODO probably promote this convenience to super-class?
        self._timeout = 30
        """Default timeout value for operations accepting an optional timeout
        argument."""


    def _assert_state(self, obj):
        """
        Asserts that the current state is either the same as the one given (if
        not a list) or one of the elements of the given list.
        @raises InstrumentStateException if the assertion fails
        """
        cs = self.get_current_state()
        if isinstance(obj, list):
            if cs in obj:
                return  # OK
            else:
                raise InstrumentStateException(
                        msg="current state=%s not one of %s" % (cs, str(obj)))
        state = obj
        if cs != state:
            raise InstrumentStateException(
                    msg="current state=%s, expected=%s" % (cs, state))

    #############################################################
    # Device connection interface.
    #############################################################

    def initialize(self, *args, **kwargs):
        """
        Initialize driver connection, bringing communications parameters
        into unconfigured state (no connection object).
        @raises InstrumentStateException if command not allowed in current
                 state
        @raises NotImplementedException if not implemented by subclass.
        """

        if self._state == TrhphDriverState.UNCONFIGURED:
            assert self._trhph_client is None
            return

        assert self._trhph_client is not None
        try:
            self._trhph_client.end()
        finally:
            self._trhph_client = None

        self._state = TrhphDriverState.UNCONFIGURED

    def configure(self, *args, **kwargs):
        """
        Configure the driver for communications with the device via
        port agent / logger (valid but unconnected connection object).
        @param arg[0] comms config dict.
        @raises InstrumentStateException if command not allowed in current
                state
        @throws InstrumentParameterException if missing comms or invalid
                config dict.
        @raises NotImplementedException if not implemented by subclass.
        """

        self._assert_state(TrhphDriverState.UNCONFIGURED)

        # Get the required param dict.
        try:
            config = args[0]

        except IndexError:
            raise InstrumentParameterException(
                    'Missing comms config parameter.')

        # Verify dict and construct connection client.
        try:
            addr = config['addr']
            port = config['port']

            if isinstance(addr, str) and \
               isinstance(port, int) and len(addr) > 0:
#                self._connection = LoggerClient(addr, port)

                def _data_listener(sample):
                    log.info("_data_listener: sample = %s" % str(sample))

                host = addr
                outfile = file('trhph_output2.txt', 'w')
                log.info("setting TrhphClient to connect to %s:%s" % (host, port))
                self.trhph_client = TrhphClient(host, port, outfile, True)
                self.trhph_client.set_data_listener(_data_listener)

            else:
                raise InstrumentParameterException('Invalid comms config dict')

        except (TypeError, KeyError):
            raise InstrumentParameterException('Invalid comms config dict.')

        self._state = TrhphDriverState.DISCONNECTED

    def connect(self, *args, **kwargs):
        """
        Establish communications with the device via port agent / logger
        (connected connection object).
        @raises InstrumentStateException if command not allowed in current
                state
        @throws InstrumentConnectionException if the connection failed.
        @raises NotImplementedException if not implemented by subclass.
        """

        self._assert_state(TrhphDriverState.DISCONNECTED)
        self.trhph_client.connect()

        self._state = TrhphDriverState.CONNECTED

    def disconnect(self, *args, **kwargs):
        """
        Disconnect from device via port agent / logger.
        @raises InstrumentStateException if command not allowed in current
                state
        @raises NotImplementedException if not implemented by subclass.
        """

        self._assert_state(TrhphDriverState.CONNECTED)
        self.trhph_client.end()

        self._state = TrhphDriverState.DISCONNECTED


    #############################################################
    # Command and control interface.
    #############################################################

    def discover(self, *args, **kwargs):
        """
        Determine initial state upon establishing communications.
        @param timeout=timeout Optional command timeout.
        @retval Current device state.
        @raises InstrumentTimeoutException if could not wake device.
        @raises InstrumentStateException if command not allowed in current
                state or if device state not recognized.
        @raises NotImplementedException if not implemented by subclass.
        """
        #
        # NOTE: This should not be a "public operation" (that is,
        # intended to be called by client code). Rather it is an internal
        # operation executed upon establishing connection with the instrument.
        #
        raise NotImplementedException('discover() is not implemented.')

    def get(self, *args, **kwargs):
        """
        Retrieve device parameters.
        @param args[0] DriverParameter.ALL or a list of parameters to retrieve.
        @retval parameter : value dict.
        @raises InstrumentParameterException if missing or invalid get parameters.
        @raises InstrumentStateException if command not allowed in current state
        @raises NotImplementedException if not implemented by subclass.
        """
        
        self._assert_state(TrhphDriverState.CONNECTED)

        # Retrieve the required parameter, raise if not present.
        try:
            params = args[0]
        except IndexError:
            raise InstrumentParameterException(
                    msg='Get command requires a parameter list or tuple.')

        if params == DriverParameter.ALL:
            params = TrhphParameter.list()

        timeout = kwargs.get('timeout', self._timeout)

        # gather data collection params if any of them are requested:
        data_collec_params = {}
        if TrhphParameter.TIME_BETWEEN_BURSTS in params or \
           TrhphParameter.VERBOSE_MODE in params:
            seconds, is_data_only = self._get_data_collection_params(timeout)
            data_collec_params[TrhphParameter.TIME_BETWEEN_BURSTS] = seconds
            data_collec_params[TrhphParameter.VERBOSE_MODE] = is_data_only

        params = list(set(params))  # remove any duplicates

        result = {}
        for param in params:
            if param == TrhphParameter.TIME_BETWEEN_BURSTS or \
               param == TrhphParameter.VERBOSE_MODE:
                value = data_collec_params[param]
            else:
                value = InstErrorCode.INVALID_PARAMETER
            result[param] = value

        return result

    def _get_data_collection_params(self, timeout):
        """
        Gets the get_data_collection_params.
        """
        try:
            return self.trhph_client.get_data_collection_params(timeout)
        except TimeoutException, e:
            raise InstrumentTimeoutException(msg=str(e))
        except TrhphClientException, e:
            raise InstrumentException(msg=str(e))

    def set(self, *args, **kwargs):
        """
        Set device parameters.
        @param args[0] parameter : value dict of parameters to set.
        @param timeout=timeout Optional command timeout.
        @raises InstrumentParameterException if missing or invalid set parameters.
        @riases InstrumentTimeoutException if could not wake device or no response.
        @raises InstrumentProtocolException if set command not recognized.
        @raises InstrumentStateException if command not allowed in current state.
        @raises NotImplementedException if not implemented by subclass.
        """

        self._assert_state(TrhphDriverState.CONNECTED)

        # Retrieve required parameter.
        # Raise if no parameter provided, or not a dict.
        try:
            params = args[0]
        except IndexError:
            raise InstrumentParameterException(
                    msg='Set command requires a parameter dict.')

        if not isinstance(params, dict):
            raise InstrumentParameterException(
                    msg='Set parameters not a dict.')

        timeout = kwargs.get('timeout', self._timeout)
        self.trhph_client.go_to_main_menu(timeout)

        updated_params = 0
        result = {}
        for (param, value) in params.items():
            if param == TrhphParameter.TIME_BETWEEN_BURSTS:
                result[param] = self._set_cycle_time(value, timeout)
                if InstErrorCode.is_ok(result[param]):
                    updated_params += 1
            elif param == TrhphParameter.VERBOSE_MODE:
                result[param] = self._set_is_data_only(value, timeout)
                if InstErrorCode.is_ok(result[param]):
                    updated_params += 1
            else:
                result[param] = InstErrorCode.INVALID_PARAMETER

#        msg = "%s parameter(s) successfully set." % updated_params
#        log.debug("announcing to driver: %s" % msg)
#        self.announce_to_driver(DriverAnnouncement.CONFIG_CHANGE, msg=msg)
        return result
        
    def _set_cycle_time(self, seconds, timeout):
        if not isinstance(seconds, int):
            return InstErrorCode.INVALID_PARAM_VALUE

        if seconds < 15:
            return InstErrorCode.INVALID_PARAM_VALUE

        try:
            self.trhph_client.set_cycle_time(seconds, timeout)
            return InstErrorCode.OK
        except TimeoutException, e:
            raise InstrumentTimeoutException(str(e))
        except TrhphClientException, e:
            return InstErrorCode.MESSAGING_ERROR

    def _set_is_data_only(self, data_only, timeout):
        if not isinstance(data_only, bool):
            return InstErrorCode.INVALID_PARAM_VALUE

        try:
            self.trhph_client.set_is_data_only(data_only, timeout)
            return InstErrorCode.OK
        except TimeoutException, e:
            raise InstrumentTimeoutException(str(e))
        except TrhphClientException, e:
            return InstErrorCode.MESSAGING_ERROR

    def execute_acquire_sample(self, *args, **kwargs):
        """
        Poll for a sample.
        @param timeout=timeout Optional command timeout.
        @ retval Device sample dict.
        @riases InstrumentTimeoutException if could not wake device or no response.
        @raises InstrumentProtocolException if acquire command not recognized.
        @raises InstrumentStateException if command not allowed in current state.
        @raises NotImplementedException if not implemented by subclass.
        """
        raise NotImplementedException('execute_acquire_sample() not implemented.')

    def execute_start_autosample(self, *args, **kwargs):
        """
        Switch to autosample mode.
        @param timeout=timeout Optional command timeout.
        @riases InstrumentTimeoutException if could not wake device or no response.
        @raises InstrumentStateException if command not allowed in current state.
        @raises NotImplementedException if not implemented by subclass.
        """
        raise NotImplementedException('execute_start_autosample() not implemented.')

    def execute_stop_autosample(self, *args, **kwargs):
        """
        Leave autosample mode.
        @param timeout=timeout Optional command timeout.
        @riases InstrumentTimeoutException if could not wake device or no response.
        @raises InstrumentProtocolException if stop command not recognized.
        @raises InstrumentStateException if command not allowed in current state.
        @raises NotImplementedException if not implemented by subclass.
        """
        raise NotImplementedException('execute_stop_autosample() not implemented.')

    def execute_test(self, *args, **kwargs):
        """
        Execute device tests.
        @param timeout=timeout Optional command timeout (for wakeup only --
        device specific timeouts for internal test commands).
        @riases InstrumentTimeoutException if could not wake device or no response.
        @raises InstrumentProtocolException if test commands not recognized.
        @raises InstrumentStateException if command not allowed in current state.
        @raises NotImplementedException if not implemented by subclass.
        """
        raise NotImplementedException('execute_test() not implemented.')

    def execute_calibrate(self, *args, **kwargs):
        """
        Execute device calibration.
        @param timeout=timeout Optional command timeout (for wakeup only --
        device specific timeouts for internal calibration commands).
        @riases InstrumentTimeoutException if could not wake device or no response.
        @raises InstrumentProtocolException if test commands not recognized.
        @raises InstrumentStateException if command not allowed in current state.
        @raises NotImplementedException if not implemented by subclass.
        """
        raise NotImplementedException('execute_calibrate() not implemented.')

    def execute_direct(self, *args, **kwargs):
        """
        """
        raise NotImplementedException('execute_direct() not implemented.')

    ########################################################################
    # Resource query interface.
    ########################################################################
    def get_resource_commands(self):
        """
        Retrun list of device execute commands available.
        """
        return [cmd for cmd in dir(self) if cmd.startswith('execute_')]

    def get_resource_params(self):
        """
        Return list of device parameters available.
        """
        return self.get(DriverParameter.ALL)

    def get_current_state(self):
        """
        Return current device state. Implemented in connection specific
        subclasses.
        """
        return self._state

    ########################################################################
    # Event interface.
    ########################################################################

    def _driver_event(self, type, val=None):
        """
        Construct and send an asynchronous driver event.
        @param type a DriverAsyncEvent type specifier.
        @param val event value for sample and test result events.
        """
        event = {
            'type': type,
            'value': None,
            'time': time.time()
        }
        if type == DriverAsyncEvent.STATE_CHANGE:
            state = self.get_current_state()
            event['value'] = state
            self._send_event(event)

        elif type == DriverAsyncEvent.CONFIG_CHANGE:
            config = self.get(DriverParameter.ALL)
            event['value'] = config
            self._send_event(event)

        elif type == DriverAsyncEvent.SAMPLE:
            event['value'] = val
            self._send_event(event)

        elif type == DriverAsyncEvent.ERROR:
            # Error caught at driver process level.
            pass

        elif type == DriverAsyncEvent.TEST_RESULT:
            event['value'] = val
            self._send_event(event)

    ########################################################################
    # Test interface.
    ########################################################################

    def driver_echo(self, msg):
        """
        Echo a message.
        @param msg the message to prepend and echo back to the caller.
        """
        reply = 'driver_echo: %s' % msg
        return reply

    def test_exceptions(self, msg):
        """
        Test exception handling in the driver process.
        @param msg message string to put in a raised exception to be caught in
        a test.
        @raises InstrumentExeption always.
        """
        raise InstrumentException(msg)
