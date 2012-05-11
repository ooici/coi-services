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
#from ion.services.mi.instrument_driver import DriverState

from ion.services.mi.drivers.uw_trhph.trhph_client import TrhphClient
from ion.services.mi.drivers.uw_trhph.trhph_client import TrhphClientException
from ion.services.mi.drivers.uw_trhph.trhph_client import TimeoutException

from ion.services.mi.drivers.uw_trhph.common import TrhphParameter

from ion.services.mi.instrument_driver import InstrumentDriver
from ion.services.mi.instrument_driver import DriverParameter

#from ion.services.mi.common import InstErrorCode

from ion.services.mi.instrument_driver import DriverConnectionState
from ion.services.mi.exceptions import InstrumentException
#from ion.services.mi.exceptions import StateError
from ion.services.mi.exceptions import InstrumentStateException
#from ion.services.mi.exceptions import ParameterError
from ion.services.mi.exceptions import InstrumentParameterException
#from ion.services.mi.exceptions import TimeoutError
from ion.services.mi.exceptions import InstrumentTimeoutException

import time

import logging
from ion.services.mi.mi_logger import mi_logger
log = mi_logger

# TODO define Packet config for TRHPH data granules.
PACKET_CONFIG = {}


class TrhphDriverState(BaseEnum):
    """
    TRHPH driver states.
    """

    UNCONFIGURED = DriverConnectionState.UNCONFIGURED
    DISCONNECTED = DriverConnectionState.DISCONNECTED
    CONNECTED = DriverConnectionState.CONNECTED


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
                raise InstrumentStateException(msg=
                        "current state=%s not one of %s" % (cs, str(obj)))
        state = obj
        if cs != state:
            raise InstrumentStateException(
                    "current state=%s, expected=%s" % (cs, state))

    #############################################################
    # Device connection interface.
    #############################################################

    def initialize(self, *args, **kwargs):
        """
        Initialize driver connection, bringing communications parameters
        into unconfigured state (no connection object).

        @raises InstrumentStateException if command not allowed in current
                 state
        """

#        print
#        print("trhph driver: args=%s kwargs=%s" % (str(args), str(kwargs)))
#        print("trhph driver: state=%s" % str(self._state))
#        print("trhph driver: _trhph_client=%s" % str(self._trhph_client))
#        print

        if log.isEnabledFor(logging.DEBUG):
            log.debug("args=%s kwargs=%s" % (str(args), str(kwargs)))

        if self._state == TrhphDriverState.UNCONFIGURED:
            assert self._trhph_client is None
            return

#        assert self._trhph_client is not None
#        try:
#            self._trhph_client.end()
#        finally:
#            self._trhph_client = None
        if self._trhph_client is not None:
            try:
                self._trhph_client.end()
            finally:
                self._trhph_client = None

        self._state = TrhphDriverState.UNCONFIGURED

    def configure(self, *args, **kwargs):
        """
        Configure the driver for communications with the device via
        port agent / logger (valid but unconnected connection object).

        @param config comms config dict.

        @raises InstrumentStateException if command not allowed in current
                state
        @throws InstrumentParameterException if missing comms or invalid
                config dict.
        """

        if log.isEnabledFor(logging.DEBUG):
            log.debug("args=%s kwargs=%s" % (str(args), str(kwargs)))

        self._assert_state(TrhphDriverState.UNCONFIGURED)

        config = kwargs.get('config', None)
        if config is None:
#            raise InstrumentParameterException(msg="'config' parameter required")
            config = args[0]

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
                outfile = file('trhph_output.txt', 'w')
                log.info("setting TrhphClient to connect to %s:%s" % (host, port))
                self.trhph_client = TrhphClient(host, port, outfile, True)
                self.trhph_client.set_data_listener(_data_listener)

            else:
                raise InstrumentParameterException(msg='Invalid comms config dict')

        except (TypeError, KeyError):
            raise InstrumentParameterException(msg='Invalid comms config dict.')

        self._state = TrhphDriverState.DISCONNECTED

    def connect(self, *args, **kwargs):
        """
        Establish communications with the device via port agent / logger
        (connected connection object).

        @raises InstrumentStateException if command not allowed in current
                state
        @throws InstrumentConnectionException if the connection failed.
        """

        if log.isEnabledFor(logging.DEBUG):
            log.debug("args=%s kwargs=%s" % (str(args), str(kwargs)))

        self._assert_state(TrhphDriverState.DISCONNECTED)

        self.trhph_client.connect()

        self._state = TrhphDriverState.CONNECTED

    def disconnect(self, *args, **kwargs):
        """
        Disconnect from device via port agent / logger.
        @raises InstrumentStateException if command not allowed in current
                state
        """

        if log.isEnabledFor(logging.DEBUG):
            log.debug("args=%s kwargs=%s" % (str(args), str(kwargs)))

        self._assert_state(TrhphDriverState.CONNECTED)

        self.trhph_client.end()

        self._state = TrhphDriverState.DISCONNECTED


    #############################################################
    # Command and control interface.
    #############################################################

    def get(self, *args, **kwargs):
        """
        Retrieve device parameters.

        @param params DriverParameter.ALL or a list of parameters to retrieve.
        @param timeout Timeout for each involved instrument interation,
                self._timeout by default.

        @retval parameter : value dict.
        @raises InstrumentParameterException if missing or invalid get parameters.
        @raises InstrumentStateException if command not allowed in current state
        """
        
        if log.isEnabledFor(logging.DEBUG):
            log.debug("args=%s kwargs=%s" % (str(args), str(kwargs)))

        self._assert_state(TrhphDriverState.CONNECTED)

        params = kwargs.get('params', None)
        if params is None:
#            raise InstrumentParameterException(msg=
#                    "'params' parameter required")
            params = args[0]

        if params == DriverParameter.ALL:
            params = TrhphParameter.list()
        elif not isinstance(params, (list, tuple)):
            raise InstrumentParameterException(msg=
                    'params must be list or tuple.')

        timeout = kwargs.get('timeout', self._timeout)

        # gather data collection params if any of them are requested:
        data_collec_params = {}
        if TrhphParameter.TIME_BETWEEN_BURSTS in params or \
           TrhphParameter.VERBOSE_MODE in params:
            seconds, is_data_only = self._get_data_collection_params(timeout)
            verbose = not is_data_only
            data_collec_params[TrhphParameter.TIME_BETWEEN_BURSTS] = seconds
            data_collec_params[TrhphParameter.VERBOSE_MODE] = verbose

        params = list(set(params))  # remove any duplicates

        result = {}
        for param in params:
            if param == TrhphParameter.TIME_BETWEEN_BURSTS or \
               param == TrhphParameter.VERBOSE_MODE:
                value = data_collec_params[param]
            else:
#                value = InstErrorCode.INVALID_PARAMETER
                raise InstrumentParameterException(msg='invalid parameter %s' % param)
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
            raise InstrumentException(str(e))

    def set(self, *args, **kwargs):
        """
        Set device parameters.

        @param params parameter : value dict of parameters to set.

        @param timeout Timeout for each involved instrument interation,
                self._timeout by default.

        @raises InstrumentParameterException if missing or invalid set parameters.
        @raises InstrumentTimeoutException if could not wake device or no response.
        @raises InstrumentProtocolException if set command not recognized.
        @raises InstrumentStateException if command not allowed in current state.
        """

        if log.isEnabledFor(logging.DEBUG):
            log.debug("args=%s kwargs=%s" % (str(args), str(kwargs)))

        self._assert_state(TrhphDriverState.CONNECTED)

        params = kwargs.get('params', None)
        if params is None:
#            raise InstrumentParameterException(msg=
#                    "'params' parameter required")
            params = args[0]

        if not isinstance(params, dict):
            raise InstrumentParameterException(msg=
                    "'params' parameter not a dict.")

        timeout = kwargs.get('timeout', self._timeout)
        self.trhph_client.go_to_main_menu(timeout)

        updated_params = 0
        result = {}
        for (param, value) in params.items():
            if param == TrhphParameter.TIME_BETWEEN_BURSTS:
                result[param] = self._set_cycle_time(value, timeout)
#                if InstErrorCode.is_ok(result[param]):
#                    updated_params += 1
                updated_params += 1
            elif param == TrhphParameter.VERBOSE_MODE:
                data_only = not value
                result[param] = self._set_is_data_only(data_only, timeout)
#                if InstErrorCode.is_ok(result[param]):
#                    updated_params += 1
                updated_params += 1
            else:
#                result[param] = InstErrorCode.INVALID_PARAMETER
                raise InstrumentParameterException(msg='invalid parameter %s' % param)

#        msg = "%s parameter(s) successfully set." % updated_params
#        log.debug("announcing to driver: %s" % msg)
#        self.announce_to_driver(DriverAnnouncement.CONFIG_CHANGE, msg=msg)
        return result
        
    def _set_cycle_time(self, seconds, timeout):
        if not isinstance(seconds, int):
#            return InstErrorCode.INVALID_PARAM_VALUE
            raise InstrumentParameterException(msg='seconds object is not an int: %s' % seconds)

        if seconds < 15:
#            return InstErrorCode.INVALID_PARAM_VALUE
            raise InstrumentParameterException(msg='seconds must be >= 15: %s' % seconds)

        try:
            self.trhph_client.set_cycle_time(seconds, timeout)
#            return InstErrorCode.OK
            return
        except TimeoutException, e:
            raise InstrumentTimeoutException(msg=str(e))
        except TrhphClientException, e:
#            return InstErrorCode.MESSAGING_ERROR
            raise InstrumentException('TrhphClientException: %s' % str(e))

    def _set_is_data_only(self, data_only, timeout):
        if not isinstance(data_only, bool):
#            return InstErrorCode.INVALID_PARAM_VALUE
            raise InstrumentParameterException(msg='data_only object is not a bool: %s' % data_only)

        try:
            self.trhph_client.set_is_data_only(data_only, timeout)
#            return InstErrorCode.OK
            return
        except TimeoutException, e:
            raise InstrumentTimeoutException(msg=str(e))
        except TrhphClientException, e:
#            return InstErrorCode.MESSAGING_ERROR
            raise InstrumentException('TrhphClientException: %s' % str(e))

    def execute_start_autosample(self, *args, **kwargs):
        """
        Switch to autosample mode.

        @param timeout Timeout for each involved instrument interation,
                self._timeout by default.

        @raises InstrumentTimeoutException if could not wake device or no response.
        @raises InstrumentStateException if command not allowed in current state.
        """
        if log.isEnabledFor(logging.DEBUG):
            log.debug("args=%s kwargs=%s" % (str(args), str(kwargs)))

        self._assert_state(TrhphDriverState.CONNECTED)

        timeout = kwargs.get('timeout', self._timeout)

        try:
            self.trhph_client.resume_data_streaming(timeout)
#            result = InstErrorCode.OK
            result = None
            return result
        except TimeoutException, e:
            raise InstrumentTimeoutException(msg=str(e))
        except TrhphClientException, e:
            log.warn("TrhphClientException while resume_data_streaming: %s" %
                     str(e))
#            return InstErrorCode.MESSAGING_ERROR
            raise InstrumentException('TrhphClientException: %s' % str(e))

    def execute_stop_autosample(self, *args, **kwargs):
        """
        Leave autosample mode.

        @param timeout Timeout for each involved instrument interation,
                self._timeout by default.

        @raises InstrumentTimeoutException if could not wake device or no response.
        @raises InstrumentProtocolException if stop command not recognized.
        @raises InstrumentStateException if command not allowed in current state.
        """
        if log.isEnabledFor(logging.DEBUG):
            log.debug("args=%s kwargs=%s" % (str(args), str(kwargs)))

        self._assert_state(TrhphDriverState.CONNECTED)

        timeout = kwargs.get('timeout', self._timeout)

        try:
            self.trhph_client.go_to_main_menu(timeout)
#            result = InstErrorCode.OK
            result = None
            return result
        except TimeoutException, e:
            raise InstrumentTimeoutException(msg=str(e))
        except TrhphClientException, e:
            log.warn("TrhphClientException while calling go_to_main_menu: %s" %
                     str(e))
#            return InstErrorCode.MESSAGING_ERROR
            raise InstrumentException('TrhphClientException: %s' % str(e))

    def execute_get_metadata(self, *args, **kwargs):
        """
        Returns metadata.

        TODO NOTE: metadata aspect not yet specified in the framework. This
        method is a direct implementation to exercise the functionality
        provided by the underlying TrhphClient. All metadata (from the
        get_system_info call) is reported.

        @param timeout Timeout for each involved instrument interation,
                self._timeout by default.

        @retval name:value dict
        """

        if log.isEnabledFor(logging.DEBUG):
            log.debug("args=%s kwargs=%s" % (str(args), str(kwargs)))

        self._assert_state(TrhphDriverState.CONNECTED)

        timeout = kwargs.get('timeout', self._timeout)
        self.trhph_client.go_to_main_menu(timeout)

        try:
            system_info = self.trhph_client.get_system_info(timeout)
            result = system_info
            return result
        except TimeoutException, e:
            raise InstrumentTimeoutException(msg=str(e))
        except TrhphClientException, e:
            log.warn("TrhphClientException while getting system info: %s" %
                     str(e))
#            return InstErrorCode.MESSAGING_ERROR
            raise InstrumentException('TrhphClientException: %s' % str(e))

    def execute_diagnostics(self, *args, **kwargs):
        """
        Executes the diagnostics operation.

        @param num_scans the number of scans for the operation
        @param timeout Timeout for each involved instrument interation,
                self._timeout by default.

        @retval a list of rows, one with values per scan
        @throws TimeoutException
        """

        if log.isEnabledFor(logging.DEBUG):
            log.debug("args=%s kwargs=%s" % (str(args), str(kwargs)))

        self._assert_state(TrhphDriverState.CONNECTED)

        num_scans = kwargs.get('num_scans', None)
        if num_scans is None:
#            raise InstrumentParameterException(msg=
#                    'num_scans parameter required')
            num_scans = 7
        if not isinstance(num_scans, int) or num_scans < 0:
            raise InstrumentParameterException(msg=
                    'Invalid num_scans parameter value')

        timeout = kwargs.get('timeout', self._timeout)

        try:
            result = self.trhph_client.execute_diagnostics(num_scans, timeout)
            return result
        except TimeoutException, e:
            raise InstrumentTimeoutException(msg=str(e))
        except TrhphClientException, e:
            log.warn("TrhphClientException while executing diagnostics: %s" %
                     str(e))
#            return InstErrorCode.MESSAGING_ERROR
            raise InstrumentException('TrhphClientException: %s' % str(e))

    def execute_get_power_statuses(self, *args, **kwargs):
        """
        Gets the power statuses.

        TODO NOTE: should this be part of the metadata?

        @param timeout Timeout for each involved instrument interation,
                self._timeout by default.

        @retval a dict of power statuses
        @throws TimeoutException
        """

        if log.isEnabledFor(logging.DEBUG):
            log.debug("args=%s kwargs=%s" % (str(args), str(kwargs)))

        self._assert_state(TrhphDriverState.CONNECTED)

        timeout = kwargs.get('timeout', self._timeout)

        try:
            result = self.trhph_client.get_power_statuses(timeout)
            return result
        except TimeoutException, e:
            raise InstrumentTimeoutException(msg=str(e))
        except TrhphClientException, e:
            log.warn("TrhphClientException executing get_power_statuses: %s" %
                     str(e))
#            return InstErrorCode.MESSAGING_ERROR
            raise InstrumentException('TrhphClientException: %s' % str(e))

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
