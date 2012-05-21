#!/usr/bin/env python

"""
@package ion.agents.data.handlers.base_data_handler
@file ion/agents/data/handlers/base_data_handler.py
@author Tim Giguere
@author Christopher Mueller
@brief Base DataHandler class - subclassed by concrete handlers for specific 'classes' of external data

"""

from pyon.public import log
from pyon.util.async import spawn
from pyon.ion.resource import PRED, RT
from pyon.core.exception import NotFound
from pyon.util.containers import get_safe
from pyon.event.event import EventPublisher
from pyon.ion.granule.taxonomy import TaxyTool

from ion.agents.instrument.instrument_driver import DriverAsyncEvent, DriverParameter
from ion.agents.instrument.exceptions import InstrumentParameterException, InstrumentCommandException

### For new granule and stream interface
from pyon.ion.granule.record_dictionary import RecordDictionaryTool
from pyon.ion.granule.granule import build_granule

import gevent
from gevent.coros import Semaphore
import time

# todo: rethink this
# Stream Packet configuration - originally from import
#from ion.agents.instrument.drivers.sbe37.sbe37_driver import PACKET_CONFIG
PACKET_CONFIG = {
    'data_stream' : ('prototype.sci_data.stream_defs', 'ctd_stream_packet')
}

class DataHandlerParameter(DriverParameter):
    """
    Base DataHandler parameters.  Inherits from DriverParameter.  Subclassed by specific handlers
    """
    POLLING_INTERVAL = 'POLLING_INTERVAL',
    PATCHABLE_CONFIG_KEYS = 'PATCHABLE_CONFIG_KEYS',

class BaseDataHandler(object):
    _params = {
        'POLLING_INTERVAL' : 3600,
        'PATCHABLE_CONFIG_KEYS' : ['stream_id','constraints']
    }
    _polling = False
    _polling_glet = None
    _dh_config = {}
    _rr_cli = None

    def __init__(self, rr_cli, stream_registrar, dh_config):
        self._dh_config=dh_config
        self._stream_registrar = stream_registrar
        self._rr_cli = rr_cli

    def set_event_callback(self, evt_callback):
        self._event_callback = evt_callback

    def _dh_event(self, type, value):
        event = {
            'type' : type,
            'value' : value,
            'time' : time.time()
        }
        self._event_callback(event)

    def _poll(self):
        """
        Internal polling method, run inside a greenlet, that triggers acquire_data for "new" data
        The polling interval (in seconds) is retrieved from the POLLING_INTERVAL parameter
        """
        self._polling = True
        interval = get_safe(self._params, 'POLLING_INTERVAL', 3600)
        while self._polling:
            self.execute_acquire_data()
            time.sleep(interval)

    def cmd_dvr(self, cmd, *args, **kwargs):
        """
        Command a DataHandler by request-reply messaging. Package command
        message and send on blocking command socket. Block on same socket
        to receive the reply. Return the driver reply.
        @param cmd The DataHandler command identifier.
        @param args Positional arguments of the command.
        @param kwargs Keyword arguments of the command.
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
        if cmd == 'configure':
            # Delegate to BaseDataHandler.configure()
#            reply = self.configure(*args, **kwargs)
            pass
        elif cmd == 'initialize':
            # Delegate to BaseDataHandler.initialize()
            reply = self.initialize(*args, **kwargs)
        elif cmd == 'get':
            # Delegate to BaseDataHandler.get()
            reply = self.get(*args, **kwargs)
        elif cmd == 'set':
            # Delegate to BaseDataHandler.set()
            reply = self.set(*args, **kwargs)
        elif cmd == 'get_resource_params':
            # Delegate to BaseDataHandler.get_resource_params()
            reply = self.get_resource_params(*args, **kwargs)
        elif cmd == 'get_resource_commands':
            # Delegate to BaseDataHandler.get_resource_commands()
            reply = self.get_resource_commands(*args, **kwargs)
        elif cmd == 'execute_acquire_data':
            # Delegate to BaseDataHandler.execute_acquire_data()
            reply = self.execute_acquire_data(*args, **kwargs)
        elif cmd == 'execute_acquire_sample':
            #TODO: Can we change these names?  acquire_data would be a better name for EOI...
            # Delegate to BaseDataHandler.execute_acquire_sample()
            reply = self.execute_acquire_sample(*args, **kwargs)
        elif cmd == 'execute_start_autosample':
            #TODO: Can we change these names?  stop_polling would be a better name for EOI...
            # Delegate to BaseDataHandler.execute_start_autosample()
            reply = self.execute_start_autosample(*args, **kwargs)
        elif cmd == 'execute_stop_autosample':
            #TODO: Can we change these names?  stop_polling would be a better name for EOI...
            # Delegate to BaseDataHandler.execute_stop_autosample()
            reply = self.execute_stop_autosample(*args, **kwargs)
        elif cmd in ['connect','disconnect','get_current_state','discover']:
            # Disregard
            pass
        else:
            desc='Command unknown by DataHandler: {0}'.format(cmd)
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
        """
        log.debug('Initializing DataHandler...')
        self._glet_queue = []
        self._semaphore=Semaphore()
        return None

    def configure(self, *args, **kwargs):
        #TODO: Add documentation
        #TODO: This should configure the DataHandler for the particular dataset
        """
        Called from:
                      InstrumentAgent._handler_inactive_go_active
        """
        log.debug('Configuring DataHandler: args = {0}'.format(args))
        try:
            self._dh_config = args[0]

        except IndexError:
            raise InstrumentParameterException('\'acquire_data\' command requires a config dict as the first argument')

        return

    def execute_acquire_data(self, *args):
        """
        Creates a copy of self._dh_config, creates a publisher, and spawns a greenlet to perform a data acquisition cycle
        If the args[0] is a dict, any entries keyed with one of the 'PATCHABLE_CONFIG_KEYS' are used to patch the config
        Greenlet binds to BaseDataHandler._acquire_data and passes the publisher and config
        Disallows multiple "new data" (unconstrained) requests using BaseDataHandler._semaphore lock
        Called from:
                      InstrumentAgent._handler_observatory_execute_resource
                       |-->  ExternalDataAgent._handler_streaming_execute_resource

        @parameter args First argument can be a config dictionary
        """
        log.debug('Executing acquire_data: args = {0}'.format(args))

        # Make a copy of the config to ensure no cross-pollution
        config = self._dh_config.copy()

        # Patch the config if mods are passed in
        try:
            config_mods = args[0]
            if not isinstance(config_mods, dict):
                raise IndexError()

            log.debug('Configuration modifications provided: {0}'.format(config_mods))
            for k in self._params['PATCHABLE_CONFIG_KEYS']:
                if get_safe(config_mods, k):
                   config[k] = config_mods[k]

        except IndexError:
            log.info('No configuration modifications were provided')

        # Verify that there is a stream_id member in the config
        stream_id = get_safe(config, 'stream_id')
        if not stream_id:
            raise ConfigurationError('Configuration does not contain required \'stream_id\' member')

        isNew = get_safe(config, 'constraints') is None

        if isNew and not self._semaphore.acquire(blocking=False):
            log.warn('Already acquiring new data - action not duplicated')
            return

        if isNew:
            # Get any NewDataCheck attachments and add them to the config
            ext_ds_id = get_safe(config,'external_dataset_res_id')
            if ext_ds_id:
                try:
                    attachment_objs, _ = self._rr_cli.find_objects(ext_ds_id, PRED.hasAttachment, RT.Attachment, False)
                    for attachment_obj in attachment_objs:
                        kwds = set(attachment_obj.keywords)
                        if 'NewDataCheck' in kwds:
                            log.debug('Found NewDataCheck attachment: {0}'.format(attachment_obj))
                            config['new_data_check'] = attachment_obj.content
                            break
                        else:
                            log.debug('Found attachment: {0}'.format(attachment_obj))
                except NotFound:
                    log.debug('No attachments found for resource: {0}'.format(ext_ds_id))
                    pass

        if not get_safe(config, 'new_data_check'):
            config['new_data_check'] = None

            # Create a publisher to pass into the greenlet
        publisher = self._stream_registrar.create_publisher(stream_id=stream_id)

        # Spawn a greenlet to do the data acquisition and publishing
        g = spawn(self._acquire_data, config, publisher, self._unlock_new_data_callback)
        log.debug('** Spawned {0}'.format(g))
        self._glet_queue.append(g)

    def execute_acquire_sample(self, *args, **kwargs):
        #TODO: Add documentation
        #TODO: Fix raises statements
        """
        Called from:
                      InstrumentAgent._handler_observatory_execute_resource
                       |-->  ExternalDataAgent._handler_streaming_execute_resource
        """
        # This returns the sample to the agent as an event
        sample = {'stream_name':'data_stream','p': [-6.945], 'c': [0.08707], 't': [20.002], 'time': [1333752198.450622]}
        self._dh_event(DriverAsyncEvent.SAMPLE, sample)
        # This does 'rpc' style event
        return sample

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
        """
        log.debug('Entered execute_start_autosample with args={0} & kwargs={1}'.format(args, kwargs))
        if not self._polling and self._polling_glet is None:
            self._polling_glet = spawn(self._poll)

        return None

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
        """
        log.debug('Entered execute_stop_autosample with args={0} & kwargs={1}'.format(args, kwargs))
        if self._polling and not self._polling_glet is None:
            self._polling_glet.kill()
            self._polling = False
            self._polling_glet = None

        return None

    def get(self, *args, **kwargs):
        #TODO: Add documentation
        #TODO: Fix raises statements
        """
        Called from:
                      InstrumentAgent._handler_get_params

        @raises InstrumentTimeoutException:
        @raises InstrumentProtocolException:
        @raises NotImplementedException:
        @raises InstrumentParameterException:
        """
        try:
            pnames=args[0]
        except IndexError:
            log.warn("No argument provided to get, return all parameters")
            pnames = [DataHandlerParameter.ALL]

        result = None
        if DataHandlerParameter.ALL in pnames:
            result = self._params
        else:
            if not isinstance(pnames, (list,tuple)):
                raise InstrumentParameterException('Get argument not a list or tuple: {0}'.format(pnames))
            result={}
            for pn in pnames:
                try:
                    log.debug('Get parameter with key: {0}'.format(pn))
                    result[pn] = self._params[pn]
                except KeyError:
                    raise InstrumentParameterException('{0} is not a valid parameter for this DataHandler.'.format(pn))

        return result

    def set(self, *args, **kwargs):
        #TODO: Add documentation
        #TODO: Fix raises statements
        """
        Called from:
                      InstrumentAgent._handler_observatory_set_params

        @raises InstrumentTimeoutException:
        @raises InstrumentProtocolException:
        @raises NotImplementedException:
        @raises InstrumentParameterException:
        """
        # Retrieve required parameter.
        # Raise if no parameter provided, or not a dict.
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
            log.debug('Raise ParameterError for un-set parameters: {0}'.format(to_raise))
            raise ParameterError('Invalid parameter(s) could not be set: {0}'.format(to_raise))

    def get_resource_params(self, *args, **kwargs):
        """
        Return list of resource parameters. Implemented in specific DataHandlers
        Called from:
                      InstrumentAgent._handler_get_resource_params
        """
        # TODO: Should raise NotImplementedException here, return temporarily for prototyping
#        raise NotImplementedException('get_resource_params() not implemented in BaseDataHandler')
        return [DataHandlerParameter.POLLING_INTERVAL]

    def get_resource_commands(self, *args, **kwargs):
        """
        Return list of DataHandler execute commands available.
        Called from:
                      InstrumentAgent._handler_get_resource_commands
        """
        cmds = [cmd.replace('execute_','') for cmd in dir(self) if cmd.startswith('execute_')]
        return cmds

    def _unlock_new_data_callback(self, caller):
        log.debug('** Release {0}'.format(caller))
        self._semaphore.release()

    @classmethod
    def _acquire_data(cls, config, publisher, unlock_new_data_callback):
        """
        Ensures required keys (such as stream_id) are available from config, configures the publisher and then calls:
             BaseDataHandler._new_data_constraints (only if config does not contain 'constraints')
             BaseDataHandler._publish_data passing BaseDataHandler._get_data as a parameter
        @param config Dict containing configuration parameters, may include constraints, formatters, etc
        @param unlock_new_data_callback BaseDataHandler callback function to allow conditional unlocking of the BaseDataHandler._semaphore
        """
        log.debug('start _acquire_data: config={0}'.format(config))

        cls._init_acquisition_cycle(config)

        constraints = get_safe(config,'constraints')
        if not constraints:
            gevent.getcurrent().link(unlock_new_data_callback)
            constraints = cls._new_data_constraints(config)
            config['constraints'] = constraints

        if not constraints:
            raise ParameterError("Data constraints not set properly")

        cls._publish_data(publisher, config, cls._get_data(config))

        # Publish a 'TestFinished' event
        if get_safe(config,'TESTING'):
            log.debug('Publish TestingFinished event')
            pub = EventPublisher('DeviceCommonLifecycleEvent')
            pub.publish_event(origin='BaseDataHandler._acquire_data', description='TestingFinished')

    @classmethod
    def _init_acquisition_cycle(cls, config):
        """
        Initialize anything the data handler will need to use, such as a dataset
        """
        raise NotImplementedError("Initialize acquisition cycle not implemented in data handler")

    @classmethod
    def _new_data_constraints(cls, config):
        #TODO: Document what "constraints" looks like (yml)!!
        """
        Determines the appropriate constraints for acquiring any "new data" from the external dataset
        The format of the constraints are documented:
        @param config Dict of configuration parameters - may be used to generate the returned 'constraints' dict
        @retval Dict that constrains retrieval of new data from the external dataset
        """
        raise NotImplementedException

    @classmethod
    def _get_data(cls, config):
        """
        Generator function that acquires data from a source iteratively based on constraints provided by config
        Passed into BaseDataHandler._publish_data and iterated to publish samples.
        Each iteration should return a well-formed Granule
        @param config Dict containing configuration parameters, may include constraints, formatters, etc
        """
        raise NotImplementedException

    @classmethod
    def _publish_data(cls, publisher, config, data_generator):
        """
        Iterates over the data_generator and publishes granules to the stream indicated in stream_id
        """
        stream_id=config['stream_id']
        log.debug('Start publishing to stream_id = {0}, with publisher = {1}'.format(stream_id, publisher))
        for count, gran in enumerate(data_generator):
            #TG: Validate that gran is a Granule object => If Granule, publish, else, just print
#            log.warn('Publish data to stream \'{0}\' [{1}]: {2}'.format(stream_id,count,gran))
            publisher.publish(gran)

            #TODO: Persist the 'state' of this operation so that it can be re-established in case of failure

        #TODO: When finished publishing, update (either directly, or via an event callback to the agent) the UpdateDescription

    @classmethod
    def _calc_iter_cnt(cls, total_recs, max_rec):
        """
        Given the total number of records and the maximum records allowed in a granule,
        calculates the number of iterations required to traverse the entire array in chunks of size max_rec
        @param total_recs The total number of records
        @param max_rec The maximum number of records allowed in a granule
        """
        cnt = total_recs / max_rec
        if total_recs % max_rec > 0:
            cnt += 1

        return cnt

class DataHandlerError(Exception):
    """
    Base DataHandler error
    """
    pass

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
        pass

    @classmethod
    def _new_data_constraints(cls, config):
        """
        Returns a constraints dictionary with 'count' assigned a random integer
        @param config Dict of configuration parameters - may be used to generate the returned 'constraints' dict
        """
        return {'count':npr.randint(5,20,1)[0]}

    @classmethod
    def _get_data(cls, config):
        """
        A generator that retrieves config['constraints']['count'] number of sequential Fibonacci numbers
        @param config Dict of configuration parameters - must contain ['constraints']['count']
        """
        cnt = get_safe(config,'constraints.count',1)

        max_rec = get_safe(config, 'max_records', 1)
        dprod_id = get_safe(config, 'data_producer_id')
        tx_yml = get_safe(config, 'taxonomy')
        ttool = TaxyTool.load(tx_yml)

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
                    ret=[]
                    count = 0

                a, b = b, a + b

        gen=fibGenerator()
        for i in xrange(cnt):
            rdt = RecordDictionaryTool(taxonomy=ttool)
            d = gen.next()
            rdt['data'] = d
            g = build_granule(data_producer_id=dprod_id, taxonomy=ttool, record_dictionary=rdt)
            yield g

class DummyDataHandler(BaseDataHandler):
    @classmethod
    def _init_acquisition_cycle(cls, config):
        """
        Initialize anything the data handler will need to use, such as a dataset
        """
        pass

    @classmethod
    def _new_data_constraints(cls, config):
        """
        Returns a constraints dictionary with 'array_len' and 'count' assigned random integers
        @param config Dict of configuration parameters - may be used to generate the returned 'constraints' dict
        """
        # Make sure the array_len is at least 1 larger than max_rec - so chunking is always seen
        max_rec = get_safe(config, 'max_records', 1)
        return {'array_len':npr.randint(max_rec+1,max_rec+10,1)[0],}

    @classmethod
    def _get_data(cls, config):
        """
        Retrieves config['constraints']['count'] number of random samples of length config['constraints']['array_len']
        @param config Dict of configuration parameters - must contain ['constraints']['count'] and ['constraints']['count']
        """
        array_len = get_safe(config, 'constraints.array_len',1)

        max_rec = get_safe(config, 'max_records', 1)
        dprod_id = get_safe(config, 'data_producer_id')
        tx_yml = get_safe(config, 'taxonomy')
        ttool = TaxyTool.load(tx_yml)

        arr = npr.random_sample(array_len)
        log.debug('Array to send using max_rec={0}: {1}'.format(max_rec, arr))
        cnt = cls._calc_iter_cnt(arr.size, max_rec)
        for x in xrange(cnt):
            rdt = RecordDictionaryTool(taxonomy=ttool)
            d = arr[x*max_rec:(x+1)*max_rec]
            rdt['data'] = d
            g = build_granule(data_producer_id=dprod_id, taxonomy=ttool, record_dictionary=rdt)
            yield g





