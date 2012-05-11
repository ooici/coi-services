#!/usr/bin/env python

#########################################################################
# NOTE
# This source is obsolete -- see trhph_driver.py
#########################################################################

"""
@package ion.services.mi.drivers.uw_trhph.protocol
@file ion/services/mi/drivers/uw_trhph/protocol.py
@author Carlos Rueda
@brief UW TRHPH protocol implementation based on trhph_client.
TrhphInstrumentProtocol extends InstrumentProtocol but does not use the
connection mechanisms there; instead it uses the trhph_client utility.
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'

from ion.services.mi.drivers.uw_trhph.trhph_client import TrhphClient
from ion.services.mi.drivers.uw_trhph.trhph_client import TrhphClientException
from ion.services.mi.drivers.uw_trhph.trhph_client import TimeoutException

from ion.services.mi.exceptions import InstrumentConnectionException
from ion.services.mi.exceptions import InstrumentTimeoutException
from ion.services.mi.logger_process import EthernetDeviceLogger

from ion.services.mi.instrument_protocol import InstrumentProtocol
from ion.services.mi.instrument_driver import DriverState
from ion.services.mi.common import InstErrorCode
#from ion.services.mi.common import DriverAnnouncement

from ion.services.mi.drivers.uw_trhph.common import TrhphParameter

import logging
from ion.services.mi.mi_logger import mi_logger
log = mi_logger


class TrhphInstrumentProtocol(InstrumentProtocol):
    """
    The protocol for the TrhphChannel.INSTRUMENT channel.
    """

    def __init__(self, evt_callback=None):
        """
        Creates and initializes a protocol object to the UNCOFIGURED state.
        """
        InstrumentProtocol.__init__(self, evt_callback)

        self.trhph_client = None

        self._host = None
        self._port = None

        self._state = DriverState.UNCONFIGURED

        # TODO probably promote this convenience to super-class?
        self._timeout = 30
        """Default timeout value for operations accepting an optional timeout
        argument."""

    def get_current_state(self):
        """
        Gets the current state of the protocol.
        """
        return self._state

    def _assert_state(self, obj):
        """
        Asserts that the current state is the same as the one given (if not
        a list) or that is one of the elements of the given list.
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
        Resets the protocol to the UNCONFIGURED state.
        """

        if log.isEnabledFor(logging.DEBUG):
            log.debug("args=%s kwargs=%s" %
                      (str(args), str(kwargs)))

        self._assert_state([DriverState.UNCONFIGURED,
                            DriverState.DISCONNECTED])

        super(TrhphInstrumentProtocol, self).initialize(*args, **kwargs)

        self._state = DriverState.UNCONFIGURED

    def configure(self, config, *args, **kwargs):
        """
        Sets _host/_port according to the given config object and sets the
        current state as DriverState.DISCONNECTED.
        Note that super.configure is not called but instead and adjusted
        version of it.
        """

        if log.isEnabledFor(logging.DEBUG):
            log.debug("config=%s args=%s kwargs=%s" %
                      (str(config), str(args), str(kwargs)))

        self._assert_state(DriverState.UNCONFIGURED)

        # Parameters for TrhphClient:
        # with direct access to the instrument:
        #   self._host = config['device_addr']
        #   self._port = config['device_port']
        # but we're now connecting through the EthernetDeviceLogger:
        self._host = config['server_addr']
        self._port = config['server_port']
        self._base_configure(config, *args, **kwargs)

        self._state = DriverState.DISCONNECTED

        return InstErrorCode.OK

    def _base_configure(self, config, *args, **kwargs):
        """
        This is as super.configure but with adjustments so we actually use
        TrhphClient (in attach) instead of LoggerClient.
        The base class is under changes so this all may change as well.
        """
        log.info('Configuring for device comms.')

        method = config['method']

        if method == 'ethernet':
            device_addr = config['device_addr']
            device_port = config['device_port']
            #server_addr = config['server_addr']
            server_port = config['server_port']
            self._logger = EthernetDeviceLogger(device_addr, device_port,
                                            server_port)
            #self._logger_client = LoggerClient(server_addr, server_port)

        elif method == 'serial':
            # The config dict does not have a valid connection method.
            raise InstrumentConnectionException()

        else:
            # The config dict does not have a valid connection method.
            raise InstrumentConnectionException()

        return InstErrorCode.OK

    def connect(self, *args, **kwargs):
        """
        It calls super.connect, which calls attach, where the actual connection
        happens, and sets _state to DriverState.CONNECTED.
        """

        if log.isEnabledFor(logging.DEBUG):
            log.debug("args=%s kwargs=%s" %
                      (str(args), str(kwargs)))

        self._assert_state([DriverState.UNCONFIGURED,
                            DriverState.DISCONNECTED])

        # TODO Note that super.connect calls attach; it would be better that
        # the attach call be done by client code so for example it is
        # possible to separate the intervening states (DISCONNECTED,
        # CONNECTED, ATTACHED, DETACHED).
        super(TrhphInstrumentProtocol, self).connect(*args, **kwargs)

        self._state = DriverState.CONNECTED

    def _data_listener(self, sample):
        """
        The callback used to announce a DATA_RECEIVED event to the driver.
        """
        log.info("_data_listener: sample = %s" % str(sample))

#        log.debug("announcing to driver: %s" % str(sample))
#        self.announce_to_driver(DriverAnnouncement.DATA_RECEIVED,
#                                data_sample=sample)

    def disconnect(self, *args, **kwargs):
        """
        Calls super.disconnect and sets state to DriverState.DISCONNECTED
        """

        if log.isEnabledFor(logging.DEBUG):
            log.debug("args=%s kwargs=%s" %
                      (str(args), str(kwargs)))

        super(TrhphInstrumentProtocol, self).disconnect(*args, **kwargs)

        self._state = DriverState.DISCONNECTED

    def attach(self, *args, **kwargs):
        """
        Sets up the trhph_client and connects to the instrument.
        Note that super.attach is not called.
        """
        if log.isEnabledFor(logging.DEBUG):
            log.debug("args=%s kwargs=%s" %
                      (str(args), str(kwargs)))

        # TODO better handling of the intervening states related with
        # connection and attachment -- see comment above in connect().
#        self._assert_state(DriverState.CONNECTED)

        self._setup_trhph_client()

        return InstErrorCode.OK

    def _setup_trhph_client(self):
        """
        Called by attach() to create the supporting TrhphClient object and do
        the connection. A file trhph_output.txt in the current directory is
        specified for the TrhphClient to log out all received messages from
        the instrument.
        """
        host = self._host
        port = self._port
        outfile = file('trhph_output.txt', 'w')
        log.info("setting TrhphClient to connect to %s:%s" % (host, port))
        self.trhph_client = TrhphClient(host, port, outfile, True)
        self.trhph_client.set_data_listener(self._data_listener)
        self.trhph_client.connect()

    def detach(self, *args, **kwargs):
        """
        Ends the TrhphClient and returns InstErrorCode.OK.
        Note that super.detach is not called.
        """
        if log.isEnabledFor(logging.DEBUG):
            log.debug("args=%s kwargs=%s" %
                      (str(args), str(kwargs)))

        self.trhph_client.end()
        self.trhph_client = None

        return InstErrorCode.OK

    ########################################################################
    # Channel command interface.
    ########################################################################

    def get(self, params, *args, **kwargs):
        """
        Gets the value of the requested parameters.
        It handles TIME_BETWEEN_BURSTS, VERBOSE_MODE.
        """
        if log.isEnabledFor(logging.DEBUG):
            log.debug("params=%s args=%s kwargs=%s" %
                      (str(params), str(args), str(kwargs)))

        assert isinstance(params, list)

        # TODO actually it should be ATTACHED if we want consistency with the
        # states related with connection and attachment.
        self._assert_state(DriverState.CONNECTED)

        timeout = kwargs.get('timeout', self._timeout)

        # gather data collection params if any of them are requested:
        data_coll_params = {}
        if TrhphParameter.TIME_BETWEEN_BURSTS in params or \
           TrhphParameter.VERBOSE_MODE in params:
            seconds, is_data_only = self._get_data_collection_params(timeout)
            data_coll_params[TrhphParameter.TIME_BETWEEN_BURSTS] = seconds
            data_coll_params[TrhphParameter.VERBOSE_MODE] = is_data_only

        params = list(set(params))  # remove any duplicates

        result = {}
        for param in params:
            if param == TrhphParameter.TIME_BETWEEN_BURSTS or \
               param == TrhphParameter.VERBOSE_MODE:
                value = data_coll_params[param]
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
            raise InstrumentTimeoutException(str(e))
        except TrhphClientException, e:
            return InstErrorCode.MESSAGING_ERROR

    def set(self, params, *args, **kwargs):
        """
        Sets the given parameters.
        It handles TIME_BETWEEN_BURSTS, VERBOSE_MODE.

        Once the parameters are set, it does a DriverAnnouncement
        .CONFIG_CHANGE. (removed after refactoring)
        """
        if log.isEnabledFor(logging.DEBUG):
            log.debug("params=%s args=%s kwargs=%s" %
                      (str(params), str(args), str(kwargs)))

        assert isinstance(params, dict)

        # TODO actually it should be ATTACHED if we want consistency with the
        # states related with connection and attachment.
        self._assert_state(DriverState.CONNECTED)

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
                result[param] = self.set_is_data_only(value, timeout)
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

    def execute_start_autosample(self, *args, **kwargs):
        """
        Puts the instrument in the data streaming mode.
        """
        if log.isEnabledFor(logging.DEBUG):
            log.debug("args=%s kwargs=%s" %
                      (str(args), str(kwargs)))

        # TODO actually it should be ATTACHED if we want consistency with the
        # states related with connection and attachment.
        self._assert_state(DriverState.CONNECTED)

        timeout = kwargs.get('timeout', self._timeout)

        try:
            self.trhph_client.resume_data_streaming(timeout)
            result = InstErrorCode.OK
            return result
        except TimeoutException, e:
            raise InstrumentTimeoutException(str(e))
        except TrhphClientException, e:
            log.warn("TrhphClientException while resume_data_streaming: %s" %
                     str(e))
            return InstErrorCode.MESSAGING_ERROR

    def execute_stop_autosample(self, *args, **kwargs):
        """
        Stops the data streaming mode, if such is the current state.
        """
        if log.isEnabledFor(logging.DEBUG):
            log.debug("args=%s kwargs=%s" %
                      (str(args), str(kwargs)))

        # TODO actually it should be ATTACHED if we want consistency with the
        # states related with connection and attachment.
        self._assert_state(DriverState.CONNECTED)

        timeout = kwargs.get('timeout', self._timeout)

        try:
            self.trhph_client.go_to_main_menu(timeout)
            result = InstErrorCode.OK
            return result
        except TimeoutException, e:
            raise InstrumentTimeoutException(str(e))
        except TrhphClientException, e:
            log.warn("TrhphClientException while calling go_to_main_menu: %s" %
                     str(e))
            return InstErrorCode.MESSAGING_ERROR

    def execute_get_metadata(self, params=None, *args, **kwargs):
        """
        Returns metadata.

        TODO NOTE: metadata aspect not yet specified in the framework. This
        method is just a quick implementation to exercise the functionality
        provided by the underlying TrhphClient.

        @param params list of  desired metadata parameter names.
                NOTE: this arg is currently ignored; all metadata attributes
                are reported.
        @retval a dict with corresponding values.
        """
        if log.isEnabledFor(logging.DEBUG):
            log.debug("params=%s args=%s kwargs=%s" %
                      (str(params), str(args), str(kwargs)))

        # TODO actually process requested params with appropriate default
        params = params or []

        assert isinstance(params, list)

        # TODO actually it should be ATTACHED if we want consistency with the
        # states related with connection and attachment.
        self._assert_state(DriverState.CONNECTED)

        timeout = kwargs.get('timeout', self._timeout)
        self.trhph_client.go_to_main_menu(timeout)

        params = list(set(params))  # remove any duplicates

        try:
            system_info = self.trhph_client.get_system_info(timeout)
            # TODO process params appropriately.
            # We currently just report all system info attributes.
            result = system_info
            return result
        except TimeoutException, e:
            raise InstrumentTimeoutException(str(e))
        except TrhphClientException, e:
            log.warn("TrhphClientException while getting system info: %s" %
                     str(e))
            return InstErrorCode.MESSAGING_ERROR

    def execute_diagnostics(self, num_scans, *args, **kwargs):
        """
        Executes the diagnostics operation.

        @param num_scans the number of scans for the operation
        @param timeout Timeout for the wait, self._timeout by default.
        @retval a list of rows, one with values per scan
        @throws TimeoutException
        """

        if log.isEnabledFor(logging.DEBUG):
            log.debug("num_scans=%s args=%s kwargs=%s" %
                      (str(num_scans), str(args), str(kwargs)))

        assert isinstance(num_scans, int)

        # TODO actually it should be ATTACHED if we want consistency with the
        # states related with connection and attachment.
        self._assert_state(DriverState.CONNECTED)

        timeout = kwargs.get('timeout', self._timeout)

        try:
            result = self.trhph_client.execute_diagnostics(num_scans, timeout)
            return result
        except TimeoutException, e:
            raise InstrumentTimeoutException(str(e))
        except TrhphClientException, e:
            log.warn("TrhphClientException while executing diagnostics: %s" %
                     str(e))
            return InstErrorCode.MESSAGING_ERROR

    def execute_get_power_statuses(self, *args, **kwargs):
        """
        Gets the power statuses.

        @param timeout Timeout for the wait, self._timeout by default.
        @retval a dict of power statuses
        @throws TimeoutException
        """

        if log.isEnabledFor(logging.DEBUG):
            log.debug("args=%s kwargs=%s" %
                      (str(args), str(kwargs)))

        # TODO actually it should be ATTACHED if we want consistency with the
        # states related with connection and attachment.
        self._assert_state(DriverState.CONNECTED)

        timeout = kwargs.get('timeout', self._timeout)

        try:
            result = self.trhph_client.get_power_statuses(timeout)
            return result
        except TimeoutException, e:
            raise InstrumentTimeoutException(str(e))
        except TrhphClientException, e:
            log.warn("TrhphClientException executing get_power_statuses: %s" %
                     str(e))
            return InstErrorCode.MESSAGING_ERROR
