#!/usr/bin/env python

"""
@package ion.agents.data.handlers.base_data_handler
@file ion/agents/data/handlers/base_data_handler.py
@author Tim Giguere
@author Christopher Mueller
@brief Base DataHandler class - subclassed by concrete handlers for specific 'classes' of external data

"""
import msgpack

from pyon.public import log, OT
from pyon.util.async import spawn
from pyon.core.exception import NotFound
from pyon.util.containers import get_safe
from pyon.event.event import EventPublisher

from interface.objects import Granule, Attachment, AttachmentType

from ion.core.includes.mi import DriverParameter, DriverEvent
from ion.agents.instrument.exceptions import InstrumentParameterException, InstrumentCommandException, InstrumentDataException, NotImplementedException, InstrumentException

from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient

### For new granule and stream interface
from ion.services.dm.utility.granule.record_dictionary import RecordDictionaryTool

from ion.agents.data.handlers.handler_utils import calculate_iteration_count, list_file_info, get_time_from_filename
from pyon.agent.agent import ResourceAgentState
from pyon.ion.stream import StandaloneStreamPublisher

import gevent
from gevent.coros import Semaphore
from gevent.event import Event
import time
import traceback

class DataHandlerParameter(DriverParameter):
    """
    Base DataHandler parameters.  Inherits from DriverParameter.  Subclassed by specific handlers
    """
    POLLING_INTERVAL = 'POLLING_INTERVAL',
    PATCHABLE_CONFIG_KEYS = 'PATCHABLE_CONFIG_KEYS',


class BaseDataHandler(object):

    def __init__(self, dh_config):
        '''
        Constructor for all data handlers.


        @param dh_config: Dictionary containing configuration parameters for the data handler
        '''
        self._polling = False           # Moved these four variables so they're instance variables, not class variables
        self._polling_glet = None
        self._dh_config = {}
        self._terminate_polling = None
        self._acquiring_data = None
        self._params = {
            'POLLING_INTERVAL': 30,
            'PATCHABLE_CONFIG_KEYS': ['stream_id', 'constraints', 'stream_route']
        }

        self._dh_config = dh_config

        self._semaphore = Semaphore()

    def set_event_callback(self, evt_callback):
        """
        Sets a callback function to be triggered on events

        @param evt_callback:
        """
        self._event_callback = evt_callback

    def _dh_event(self, type, value):
        event = {
            'type': type,
            'value': value,
            'time': time.time()
        }
        self._event_callback(event)

    def _poll(self):
        """
        Internal polling method, run inside a greenlet, that triggers execute_acquire_sample without configuration mods
        The polling interval (in seconds) is retrieved from the POLLING_INTERVAL parameter
        """
        self._polling = True
        self._terminate_polling = Event()
        interval = get_safe(self._params, 'POLLING_INTERVAL', 3600)
        log.debug('Polling interval: {0}'.format(interval))

        while not self._terminate_polling.is_set():
            self.execute_acquire_sample()
            self._terminate_polling.wait(timeout=interval)

    def cmd_dvr(self, cmd, *args, **kwargs):
        """
        Command a DataHandler by request-reply messaging. Package command
        message and send on blocking command socket. Block on same socket
        to receive the reply. Return the driver reply.
        @param cmd The DataHandler command identifier.
        @param args Positional arguments of the command.
        @param kwargs Keyword arguments of the command.
        @throws InstrumentCommandException if the command is not recognized
        @retval Command result.
        """
        # Package command dictionary.

        #need to account for observatory_execute_resource commands
        #connect -> Not used
        #get_current_state -> Not used
        #discover -> Not used
        #disconnect -> Not used

        log.debug('cmd_dvr received command \'{0}\' with: args={1} kwargs={2}'.format(cmd, args, kwargs))

        reply = None
        if cmd == 'initialize':
            # Delegate to BaseDataHandler.initialize()
            reply = self.initialize(*args, **kwargs)
        elif cmd == 'get_resource':
            # Delegate to BaseDataHandler.get()
            reply = self.get(*args, **kwargs)
        elif cmd == 'set_resource':
            # Delegate to BaseDataHandler.set()
            reply = self.set(*args, **kwargs)
        elif cmd == 'get_resource_params':
            # Delegate to BaseDataHandler.get_resource_params()
            reply = self.get_resource_params(*args, **kwargs)
        elif cmd == 'get_resource_commands':
            # Delegate to BaseDataHandler.get_resource_commands()
            reply = self.get_resource_commands(*args, **kwargs)
        elif cmd == 'get_resource_capabilities':
            # Delegate to BaseDataHandler.get_resource_commands()
            reply = self.get_resource_capabilities(*args, **kwargs)
        elif cmd == 'execute_acquire_sample':
            # Delegate to BaseDataHandler.execute_acquire_sample()
            reply = self.execute_acquire_sample(*args, **kwargs)
        elif cmd == 'execute_start_autosample':
            # Delegate to BaseDataHandler.execute_start_autosample()
            reply = self.execute_start_autosample(*args, **kwargs)
        elif cmd == 'execute_stop_autosample':
            # Delegate to BaseDataHandler.execute_stop_autosample()
            reply = self.execute_stop_autosample(*args, **kwargs)
        elif cmd == 'execute_resource':
            reply = self.execute_resource(*args, **kwargs)
        elif cmd in ['configure', 'connect', 'disconnect', 'get_current_state', 'discover', 'execute_acquire_data']:
            # Disregard
            log.info('Command \'{0}\' not used by DataHandler'.format(cmd))
            pass
        else:
            desc = 'Command \'{0}\' unknown by DataHandler'.format(cmd)
            log.info(desc)
            raise InstrumentCommandException(desc)

        return reply

    def initialize(self, *args, **kwargs):
        #TODO: Add documentation
        #TODO: This should put the DataHandler back into an 'unconfigured' state
        """
        Called from:
                      InstrumentAgent._handler_idle_reset
                      InstrumentAgent._handler_idle_go_inactive
                      InstrumentAgent._handler_stopped_reset
                      InstrumentAgent._handler_stopped_go_inactive
                      InstrumentAgent._handler_observatory_reset
                      InstrumentAgent._handler_observatory_go_inactive
                      InstrumentAgent._handler_uninitialized_initialize
                      |--> ExternalDataAgent._start_driver

        @param args Positional arguments of the command.
        @param kwargs Keyword arguments of the command.
        """
        log.debug('Initializing DataHandler...')
        self._glet_queue = []
        return None

    def configure(self, *args, **kwargs):
        #TODO: Add documentation
        #TODO: This should configure the DataHandler for the particular dataset
        """
        Called from:
                      InstrumentAgent._handler_inactive_go_active
        @param args First argument should be a config dictionary
        @param kwargs Keyword arguments of the command.
        @throws InstrumentParameterException if the first argument isn't a config dictionary
        """
        log.debug('Configuring DataHandler: args = {0}'.format(args))
        try:
            self._dh_config = args[0]

        except IndexError:
            raise InstrumentParameterException('\'acquire_sample\' command requires a config dict as the first argument')

        return

    def execute_acquire_sample(self, *args):
        """
        Creates a copy of self._dh_config, creates a publisher, and spawns a greenlet to perform a data acquisition cycle
        If the args[0] is a dict, any entries keyed with one of the 'PATCHABLE_CONFIG_KEYS' are used to patch the config
        Greenlet binds to BaseDataHandler._acquire_sample and passes the publisher and config
        Disallows multiple "new data" (unconstrained) requests using BaseDataHandler._semaphore lock
        Called from:
                      InstrumentAgent._handler_observatory_execute_resource
                       |-->  ExternalDataAgent._handler_streaming_execute_resource

        @parameter args First argument can be a config dictionary
        @throws IndexError if first argument is not a dictionary
        @throws ConfigurationError if required members aren't present
        @retval New ResourceAgentState (COMMAND)
        """
        log.debug('Executing acquire_sample: args = {0}'.format(args))

        # Make a copy of the config to ensure no cross-pollution
        config = self._dh_config.copy()

        # Patch the config if mods are passed in
        try:
            config_mods = args[0]
            if not isinstance(config_mods, dict):
                raise IndexError()

            log.debug('Configuration modifications provided: {0}'.format(config_mods))
            for k in self._params['PATCHABLE_CONFIG_KEYS']:
                p = get_safe(config_mods, k)
                if not p is None:
                    config[k] = p

        except IndexError:
            log.info('No configuration modifications were provided')

        # Verify that there is a stream_id member in the config
        stream_id = get_safe(config, 'stream_id', None)
        if not stream_id:
            raise ConfigurationError('Configuration does not contain required \'stream_id\' member')
        stream_route = get_safe(config, 'stream_route', None)

        if not stream_route:
            raise ConfigurationError('Configuration does not contain required \'stream_route\' member')

        isNew = get_safe(config, 'constraints') is None

        if isNew and not self._semaphore.acquire(blocking=False):
            log.warn('Already acquiring new data - action not duplicated')
            return

        ndc = None
        if isNew:
            # Get the NewDataCheck attachment and add it's content to the config
            ext_ds_id = get_safe(config, 'external_dataset_res_id')
            if ext_ds_id:
                ndc = self._find_new_data_check_attachment(ext_ds_id)

        config['new_data_check'] = ndc

        # Create a publisher to pass into the greenlet
        publisher = StandaloneStreamPublisher(stream_id=stream_id, stream_route=stream_route)

        # Spawn a greenlet to do the data acquisition and publishing
        g = spawn(self._acquire_sample, config, publisher, self._unlock_new_data_callback, self._update_new_data_check_attachment)
        log.debug('** Spawned {0}'.format(g))
        self._glet_queue.append(g)
        return ResourceAgentState.COMMAND, None

    def execute_resource(self, *args, **kwargs):
        """
        Function to acquire data and start/stop autosample

        @param args: first argument should be the command to run, second optional argument is a config dictionary
        @param kwargs: Keyword arguments of the command
        @retval Next ResourceAgentState
        """
        cmd = args[0]
        if cmd == DriverEvent.ACQUIRE_SAMPLE:
            if len(args) == 1:
                return self.execute_acquire_sample()
            else:
                return self.execute_acquire_sample(args[1])
        elif cmd == DriverEvent.START_AUTOSAMPLE:
            if len(args) == 1:
                return self.execute_start_autosample()
            else:
                return self.execute_start_autosample(args[1])
        elif cmd == DriverEvent.STOP_AUTOSAMPLE:
            if len(args) == 1:
                return self.execute_stop_autosample()
            else:
                return self.execute_stop_autosample(args[1])

    def execute_start_autosample(self, *args, **kwargs):
        #TODO: Add documentation
        #TODO: Fix raises statements
        """
        Put the DataHandler into streaming mode and start polling for new data
        Called from:
                      InstrumentAgent._handler_observatory_go_streaming

        @raises InstrumentTimeoutException:
        @raises InstrumentProtocolException:
        @raises NotImplementedException:
        @raises InstrumentParameterException:
        @retval Next ResourceAgentState (STREAMING)
        """
        log.debug('Entered execute_start_autosample with args={0} & kwargs={1}'.format(args, kwargs))
        if not self._polling and self._polling_glet is None:
            self._polling_glet = spawn(self._poll)

        return ResourceAgentState.STREAMING, None

    def execute_stop_autosample(self, *args, **kwargs):
        #TODO: Add documentation
        #TODO: Fix raises statements
        """
        Stop polling for new data and put the DataHandler into observatory mode
        Called from:
                      InstrumentAgent._handler_streaming_go_observatory

        @raises InstrumentTimeoutException:
        @raises InstrumentProtocolException:
        @raises NotImplementedException:
        @raises InstrumentParameterException:
        @retval Next ResourceAgentState (COMMAND)
        """
        log.debug('Entered execute_stop_autosample with args={0} & kwargs={1}'.format(args, kwargs))
        if self._polling and not self._polling_glet is None:
            log.debug("Terminating polling")
            self._terminate_polling.set()
            self._polling_glet.join(timeout=30)
            log.debug("Polling terminated")
            self._polling = False
            self._polling_glet = None

        return ResourceAgentState.COMMAND, None

    def get(self, *args, **kwargs):
        #TODO: Add documentation
        #TODO: Fix raises statements
        """
        Retrieve required parameter

        Called from:
                      InstrumentAgent._handler_get_params

        @throws InstrumentTimeoutException:
        @throws InstrumentProtocolException:
        @throws NotImplementedException:
        @throws InstrumentParameterException: if paramter is not a list or not found
        @retval Parameter value
        """
        try:
            pnames = args[0]
        except IndexError:
            log.warn("No argument provided to get, return all parameters")
            pnames = [DataHandlerParameter.ALL]

        result = None
        if DataHandlerParameter.ALL in pnames:
            result = self._params
        else:
            if not isinstance(pnames, (list, tuple)):
                raise InstrumentParameterException('Get argument not a list or tuple: {0}'.format(pnames))
            result = {}
            for pn in pnames:
                try:
                    log.debug('Get parameter with key: {0}'.format(pn))
                    result[pn] = self._params[pn]
                except KeyError:
                    log.debug('\'{0}\' not found in self._params'.format(pn))
                    raise InstrumentParameterException('{0} is not a valid parameter for this DataHandler.'.format(pn))

        return result

    def set(self, *args, **kwargs):
        #TODO: Add documentation
        #TODO: Fix raises statements
        """
        Set required parameter.
        Called from:
                      InstrumentAgent._handler_observatory_set_params

        @throws InstrumentTimeoutException:
        @throws InstrumentProtocolException:
        @throws NotImplementedException:
        @throws InstrumentParameterException: Invalid parameter or no parameter dictionary provided
        @retval None
        """

        try:
            params = args[0]

        except IndexError:
            raise InstrumentParameterException('Set command requires a parameter dict.')

        to_raise = []

        if not isinstance(params, dict):
            raise InstrumentParameterException('Set parameters not a dict.')
        else:
            for (key, val) in params.iteritems():
                if key in self._params:
                    log.debug('Set parameter \'{0}\' = {1}'.format(key, val))
                    self._params[key] = val
                else:
                    log.debug('Parameter \'{0}\' not in self._params and cannot be set'.format(key))
                    to_raise.append(key)

        if len(to_raise) > 0:
            log.debug('Raise InstrumentParameterException for un-set parameters: {0}'.format(to_raise))
            raise InstrumentParameterException('Invalid parameter(s) could not be set: {0}'.format(to_raise))

    def get_resource_params(self, *args, **kwargs):
        """
        Return list of resource parameters. Implemented in specific DataHandlers
        Called from:
                      InstrumentAgent._handler_get_resource_params

        @retval list of resource parameters
        """
        return self._params.keys()

    def get_resource_capabilities(self, *args, **kwargs):
        """
        Return list of DataHandler execute commands available.
        Called from:
                      InstrumentAgent._handler_get_resource_capabilities
        @retval list of available execute commands and parameters
        """
        res_cmds = [cmd.replace('execute_', '') for cmd in dir(self) if cmd.startswith('execute_')]
        res_params = ['POLLING_INTERVAL', 'PATCHABLE_CONFIG_KEYS']

        result = [res_cmds, res_params]
        return result

    def get_resource_commands(self, *args, **kwargs):
        """
        Return list of DataHandler execute commands available.
        Called from:
                      InstrumentAgent._handler_get_resource_commands
        @retval list of available execute commands
        """
        cmds = [cmd.replace('execute_', '') for cmd in dir(self) if cmd.startswith('execute_')]
        return cmds

    def _unlock_new_data_callback(self, caller):
        """
        Release the polling semaphore

        @retval None
        """
        log.debug('** Release {0}'.format(caller))
        self._semaphore.release()

    def _find_new_data_check_attachment(self, res_id):
        """
        Returns a list of the last data files that were found

        @param res_id The resource ID of the external dataset resource
        @throws InstrumentException if no attachment is found
        @retval list of data file names and sizes
        """
        rr_cli = ResourceRegistryServiceClient()
        try:
            attachment_objs = rr_cli.find_attachments(resource_id=res_id, include_content=True, id_only=False)
            for attachment_obj in attachment_objs:
                kwds = set(attachment_obj.keywords)
                if 'NewDataCheck' in kwds:
                    log.debug('Found NewDataCheck attachment: {0}'.format(attachment_obj._id))
                    return msgpack.unpackb(attachment_obj.content)
                else:
                    log.debug('Found attachment: {0}'.format(attachment_obj))
        except NotFound:
            raise InstrumentException('ExternalDatasetResource \'{0}\' not found'.format(res_id))

    @classmethod
    def _update_new_data_check_attachment(cls, res_id, new_content):
        """
        Update the list of data files for the external dataset resource

        @param res_id the ID of the external dataset resource
        @param new_content list of new files found on data host
        @throws InstrumentException if external dataset resource can't be found
        @retval the attachment ID
        """
        rr_cli = ResourceRegistryServiceClient()
        try:
            # Delete any attachments with the "NewDataCheck" keyword
            attachment_objs = rr_cli.find_attachments(resource_id=res_id, include_content=False, id_only=False)
            for attachment_obj in attachment_objs:
                kwds = set(attachment_obj.keywords)
                if 'NewDataCheck' in kwds:
                    log.debug('Delete NewDataCheck attachment: {0}'.format(attachment_obj._id))
                    rr_cli.delete_attachment(attachment_obj._id)
                else:
                    log.debug('Found attachment: {0}'.format(attachment_obj))

            # Create the new attachment
            att = Attachment(name='new_data_check', attachment_type=AttachmentType.ASCII, keywords=['NewDataCheck', ], content_type='text/plain', content=msgpack.packb(new_content))
            att_id = rr_cli.create_attachment(resource_id=res_id, attachment=att)

        except NotFound:
            raise InstrumentException('ExternalDatasetResource \'{0}\' not found'.format(res_id))

        return att_id

    @classmethod
    def _acquire_sample(cls, config, publisher, unlock_new_data_callback, update_new_data_check_attachment):
        """
        Ensures required keys (such as stream_id) are available from config, configures the publisher and then calls:
             BaseDataHandler._constraints_for_new_request (only if config does not contain 'constraints')
             BaseDataHandler._publish_data passing BaseDataHandler._get_data as a parameter
        @param config Dict containing configuration parameters, may include constraints, formatters, etc
        @param publisher the publisher used to publish data
        @param unlock_new_data_callback BaseDataHandler callback function to allow conditional unlocking of the BaseDataHandler._semaphore
        @param update_new_data_check_attachment classmethod to update the external dataset resources file list attachment
        @throws InstrumentParameterException if the data constraints are not a dictionary
        @retval None
        """
        log.debug('start _acquire_sample: config={0}'.format(config))

        cls._init_acquisition_cycle(config)

        constraints = get_safe(config, 'constraints')
        if not constraints:
            gevent.getcurrent().link(unlock_new_data_callback)
            try:
                constraints = cls._constraints_for_new_request(config)
            except NoNewDataWarning:
                #log.info(nndw.message)
                if get_safe(config, 'TESTING'):
                    #log.debug('Publish TestingFinished event')
                    pub = EventPublisher('DeviceCommonLifecycleEvent')
                    pub.publish_event(origin='BaseDataHandler._acquire_sample', description='TestingFinished')
                return

            if constraints is None:
                raise InstrumentParameterException("Data constraints returned from _constraints_for_new_request cannot be None")
            config['constraints'] = constraints
        elif isinstance(constraints, dict):
            addnl_constr = cls._constraints_for_historical_request(config)
            if not addnl_constr is None and isinstance(addnl_constr, dict):
                constraints.update(addnl_constr)
        else:
            raise InstrumentParameterException('Data constraints must be of type \'dict\':  {0}'.format(constraints))

        cls._publish_data(publisher, cls._get_data(config), config, update_new_data_check_attachment)

        # Publish a 'TestFinished' event
        if get_safe(config, 'TESTING'):
            #log.debug('Publish TestingFinished event')
            pub = EventPublisher(OT.DeviceCommonLifecycleEvent)
            pub.publish_event(origin='BaseDataHandler._acquire_sample', description='TestingFinished')

    @classmethod
    def _constraints_for_historical_request(cls, config):
        """
        Determines any constraints that must be added to the constraints configuration.
        This should present a uniform constraints configuration to be sent to _get_data
        @param config Dict containing configuration parameters, may include constraints, formatters, etc
        @retval dictionary containing the restraints
        """
        raise NotImplementedException('{0}.{1} must implement \'_constraints_for_historical_request\''.format(cls.__module__, cls.__name__))

    @classmethod
    def _init_acquisition_cycle(cls, config):
        """
        Allows the concrete implementation to initialize/prepare objects the data handler
        will use repeatedly (such as a dataset object) in cls._constraints_for_new_request and/or cls._get_data
        Objects should be added to the config so they are available later in the workflow
        @param config Dict containing configuration parameters, may include constraints, formatters, etc
        """
        raise NotImplementedException('{0}.{1} must implement \'_init_acquisition_cycle\''.format(cls.__module__, cls.__name__))

    @classmethod
    def _constraints_for_new_request(cls, config):
        #TODO: Document what "constraints" looks like (yml)!!
        """
        Determines the appropriate constraints for acquiring any "new data" from the external dataset
        Returned value cannot be None and is assigned to config['constraints']
        The format of the constraints are documented:
        @param config dict of configuration parameters - may be used to generate the returned 'constraints' dict
        @retval dict that contains the constraints for retrieval of new data from the external dataset
        """
        raise NotImplementedException('{0}.{1} must implement \'_constraints_for_new_request\''.format(cls.__module__, cls.__name__))

    @classmethod
    def _get_data(cls, config):
        """
        Iterable function that acquires data from a source iteratively based on constraints provided by config
        Passed into BaseDataHandler._publish_data and iterated to publish samples.
        @param config dict containing configuration parameters, may include constraints, formatters, etc
        @retval an iterable that returns well-formed Granule objects on each iteration
        """
        raise NotImplementedException('{0}.{1} must implement \'_get_data\''.format(cls.__module__, cls.__name__))

    @classmethod
    def _publish_data(cls, publisher, data_generator, config=None, update_new_data_check_attachment=None):
        """
        Iterates over the data_generator and publishes granules to the stream indicated in stream_id
        @param publisher to publish the data with
        @param data_generator enumerator to cycle through the data
        @throws InstrumentDataException if data_generator isn't an enumerator
        """
        if data_generator is None or not hasattr(data_generator, '__iter__'):
            raise InstrumentDataException('Invalid object returned from _get_data: returned object cannot be None and must have \'__iter__\' attribute')

        for count, gran in enumerate(data_generator):
            if isinstance(gran, Granule):
                #log.warn('_publish_data: {0}\n{1}'.format(count, gran))
                publisher.publish(gran)
                if config and 'set_new_data_check' in config:
                    update_new_data_check_attachment(config['external_dataset_res_id'], config['set_new_data_check'])
            else:
                log.warn('Could not publish object of {0} returned by _get_data: {1}'.format(type(gran), gran))

        publisher.close()
        #TODO: Persist the 'state' of this operation so that it can be re-established in case of failure

        #TODO: When finished publishing, update (either directly, or via an event callback to the agent) the UpdateDescription


class DataHandlerError(Exception):
    """
    Base DataHandler error
    """
    pass


class NoNewDataWarning(DataHandlerError):
    """
    Raised when there is no new data for a given acquisition cycle
    """
    message = 'There is no new data for the dataset, aborting acquisition cycle'


class ConfigurationError(DataHandlerError):
    """
    Raised when a required configuration parameter is missing
    """
    pass

########################
# Example DataHandlers #
########################

import numpy as np
import numpy.random as npr


class FibonacciDataHandler(BaseDataHandler):
    """
    Sample concrete DataHandler implementation that returns sequential Fibonacci numbers
    """
    @classmethod
    def _init_acquisition_cycle(cls, config):
        """
        Initialize anything the data handler will need to use, such as a dataset
        """

    @classmethod
    def _constraints_for_new_request(cls, config):
        """
        Returns a constraints dictionary with 'count' assigned a random integer
        @param config Dict of configuration parameters - may be used to generate the returned 'constraints' dict
        """
        return {'count': npr.randint(5, 20, 1)[0]}

    @classmethod
    def _constraints_for_historical_request(cls, config):
        pass

    @classmethod
    def _get_data(cls, config):
        """
        A generator that retrieves config['constraints']['count'] number of sequential Fibonacci numbers
        @param config Dict of configuration parameters - must contain ['constraints']['count']
        """
        cnt = get_safe(config, 'constraints.count', 1)

        max_rec = get_safe(config, 'max_records', 1)
        #dprod_id = get_safe(config, 'data_producer_id')

        stream_def = get_safe(config, 'stream_def')

        def fibGenerator():
            """
            A Fibonacci sequence generator
            """
            count = 0
            ret = []
            a, b = 1, 1
            while 1:
                count += 1
                ret.append(a)
                if count == max_rec:
                    yield np.array(ret)
                    ret = []
                    count = 0

                a, b = b, a + b

        gen = fibGenerator()
        cnt = calculate_iteration_count(cnt, max_rec)
        for i in xrange(cnt):
            rdt = RecordDictionaryTool(stream_definition_id=stream_def)
            d = gen.next()
            rdt['data'] = d
            g = rdt.to_granule()
            yield g


class DummyDataHandler(BaseDataHandler):
    @classmethod
    def _init_acquisition_cycle(cls, config):
        # TODO: Can't build a parser here because we won't have a file name!!  Just a directory :)
        # May not be much to do in this method...
        # maybe just ensure access to the dataset_dir and move some of the 'buried' params up to the config dict?
        ext_dset_res = get_safe(config, 'external_dataset_res', None)
        if not ext_dset_res:
            raise SystemError('external_dataset_res not present in configuration, cannot continue')

        config['ds_params'] = ext_dset_res.dataset_description.parameters

    #        base_url = ext_dset_res.dataset_description.parameters['base_url']
    #        pattern = get_safe(ext_dset_res.dataset_description.parameters, 'pattern')
    #        config['base_url'] = base_url
    #        config['pattern'] = pattern

    @classmethod
    def _constraints_for_new_request(cls, config):
        """
        Returns a constraints dictionary with 'array_len' and 'count' assigned random integers
        @param config Dict of configuration parameters - may be used to generate the returned 'constraints' dict
        @retval constraints dictionary
        """
        old_list = get_safe(config, 'new_data_check') or []

        ret = {}
        base_url = get_safe(config, 'ds_params.base_url')
        list_pattern = get_safe(config, 'ds_params.list_pattern')
        date_pattern = get_safe(config, 'ds_params.date_pattern')
        date_extraction_pattern = get_safe(config, 'ds_params.date_extraction_pattern')

        curr_list = list_file_info(base_url, list_pattern)

        # Determine which files are new
        #Not exactly the prettiest method, but here goes:
        #old_list comes in as a list of lists: [[]]
        #curr_list comes in as a list of tuples: [()]
        #each needs to be a set of tuples for set.difference to work properly
        #set.difference returns a list of tuples that appear in curr_list but not old_list, providing the new
        #files that are available

        curr_set = set(tuple(x) for x in curr_list)
        old_set = set(tuple(x) for x in old_list)

        #new_list = [tuple(x) for x in curr_list if list(x) not in old_list] - removed because it wasn't working properly
        new_list = list(curr_set.difference(old_set))

        if len(new_list) is 0:
            raise NoNewDataWarning()

        # The curr_list is the new new_data_check - used for the next "new data" evaluation
        config['set_new_data_check'] = curr_list

        # The new_list is the set of new files - these will be processed
        ret['new_files'] = new_list
        ret['start_time'] = get_time_from_filename(new_list[0][0], date_extraction_pattern, date_pattern)
        ret['end_time'] = get_time_from_filename(new_list[-1][0], date_extraction_pattern, date_pattern)
        ret['bounding_box'] = {}
        ret['vars'] = []

        log.debug('constraints_for_new_request: {0}'.format(ret))

        return ret

    @classmethod
    def _constraints_for_historical_request(cls, config):
        """
        Returns a list of new file names in the given directory
        @param config dictionary of configuration parameters
        @retval list of new file names
        """
        base_url = get_safe(config, 'ds_params.base_url')
        list_pattern = get_safe(config, 'ds_params.list_pattern')
        date_pattern = get_safe(config, 'ds_params.date_pattern')
        date_extraction_pattern = get_safe(config, 'ds_params.date_extraction_pattern')

        start_time = get_safe(config, 'constraints.start_time')
        end_time = get_safe(config, 'constraints.end_time')

        new_list = []
        curr_list = list_file_info(base_url, list_pattern)

        for x in curr_list:
            curr_time = get_time_from_filename(x[0], date_extraction_pattern, date_pattern)
            if start_time <= curr_time <= end_time:
                new_list.append(x)

        return {'new_files': new_list}

    @classmethod
    def _get_data(cls, config):
        """
        Retrieves config['constraints']['count'] number of random samples of length config['constraints']['array_len']
        @param config Dict of configuration parameters - must contain ['constraints']['count'] and ['constraints']['count']
        """
        array_len = get_safe(config, 'constraints.array_len', 1)

        max_rec = get_safe(config, 'max_records', 1)
        #dprod_id = get_safe(config, 'data_producer_id')

        stream_def = get_safe(config, 'stream_def')

        arr = npr.random_sample(array_len)

        #log.debug('Array to send using max_rec={0}: {1}'.format(max_rec, arr))
        cnt = calculate_iteration_count(arr.size, max_rec)
        for x in xrange(cnt):
            rdt = RecordDictionaryTool(stream_definition_id=stream_def)
            d = arr[x * max_rec:(x + 1) * max_rec]
            rdt['dummy'] = d
            g = rdt.to_granule()
            yield g
