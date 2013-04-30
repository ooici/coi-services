"""
simpler implementation of external dataset agent
avoids artificial agent/driver communication
maintain state of agent per-file
"""
from ooi.logging import log
from pyon.util.containers import get_safe
from pyon.core.exception import InstDriverError
from pyon.core.exception import NotFound
from pyon.public import OT
from pyon.core.bootstrap import IonObject
from ion.agents.instrument.exceptions import InstrumentStateException

from pyon.agent.agent import ResourceAgentEvent
from pyon.agent.agent import ResourceAgentState
from ion.agents.instrument.instrument_agent import InstrumentAgent
from ion.core.includes.mi import DriverEvent
from ooi.poller import DirectoryPoller
from pyon.agent.agent import ResourceAgentState
from pyon.ion.stream import StandaloneStreamPublisher
from ion.services.dm.utility.granule.record_dictionary import RecordDictionaryTool
from coverage_model import ParameterDictionary

class BasePollingDatasetAgent(InstrumentAgent):
    """
    override enough of Instrument to make: agent == driver client == driver

    subclass expected to override just set_configuration, start_polling, stop_polling
    """
    def __init__(self):
        log.info("__init__")
        super(BasePollingDatasetAgent,self).__init__()
        self._fsm.add_handler(ResourceAgentState.STREAMING, ResourceAgentEvent.EXECUTE_RESOURCE, self._handler_streaming_execute_resource)
    def _start_driver(self, dvr_config):
        log.info("_start_driver")
        self.set_configuration(dvr_config)
        self._dvr_client = self
    def _stop_driver(self):
        log.info("_stop_driver")
        self._dvr_client = None
    def _handler_streaming_execute_resource(self, command, *args, **kwargs):
        """
        Handler for execute_resource command in streaming state.
        Delegates to InstrumentAgent._handler_observatory_execute_resource
        """
        log.info("_handler_streaming_execute_resource")
        if command == DriverEvent.ACQUIRE_SAMPLE or command == DriverEvent.STOP_AUTOSAMPLE:
            return self._handler_execute_resource(command, *args, **kwargs)
        else:
            raise InstrumentStateException('Command \'{0}\' not allowed in current state {1}'.format(command, self._fsm.get_current_state()))
    def _handler_active_unknown_go_inactive(self, *args, **kwargs):
        log.info("_handler_active_unknown_go_inactive")
        self.stop_polling()
        return (ResourceAgentState.INACTIVE, None)
    def _handler_inactive_go_active(self, *args, **kwargs):
        log.info("_handler_inactive_go_active")
        self.start_polling()
        return (ResourceAgentState.IDLE, None)
    def cmd_dvr(self, cmd, *args, **kwargs):
        log.warn("cmd_dvr: should not be called %s", cmd)
        if cmd == 'execute_start_autosample':
            # Delegate to BaseDataHandler.execute_start_autosample()
            self.start_polling()
            return (None, None)
        elif cmd == 'execute_stop_autosample':
            self.stop_polling()
            return (None, None)
        elif cmd == 'execute_resource':
            log.warn("cmd_dvr.execute_resource %r %r",args,kwargs)
            return (None,None)

    def _validate_driver_config(self):
        # let _start_driver raise more specific exceptions
        log.info("_validate_driver_config")
        return True

    ### subclass will implement agent behavior using these methods
    #
    def set_configuration(self, config):
        raise Exception('subclass must implement')
    def start_polling(self):
        raise Exception('subclass must implement')

    def stop_polling(self):
        raise Exception('subclass must implement')


class FilePollingDatasetAgent(BasePollingDatasetAgent):
    _poller = None
    _polling_exception = None

    def set_configuration(self, config):
        """
        expect configuration to have:
        - parser module/class
        - directory, wildcard to find data files
        - optional timestamp of last granule
        - optional poll rate
        - publish info
        """
        import pprint
        log.info("agent configuration:\n%r", pprint.pformat(config))

        self.directory = config['directory']
        self.wildcard = config['pattern']
        self.interval = get_safe(config, 'frequency', 300)
        self.max_records = get_safe(config, 'max_records', 100)
        stream_id = config['stream_id']
        stream_route_param = config['stream_route']
        stream_route = IonObject(OT.StreamRoute, stream_route_param)
        self.publisher = StandaloneStreamPublisher(stream_id=stream_id, stream_route=stream_route)
        module_name = config['parser.module']
        class_name = config['parser.class']
        parser_module = __import__(module_name, fromlist=[class_name])
        self.parser_class = getattr(parser_module, class_name)
        self.parameter_dictionary = ParameterDictionary.load(config['parameter_dict'])
        log.info('pdict %s: %s', self.parameter_dictionary.__class__.__module__, pprint.pformat(self.parameter_dictionary))
        self.time_field = self.parameter_dictionary.get_temporal_context()
        self.latest_granule_time = get_safe(config, 'last_time', 0)

    def start_polling(self):
        if self._poller:
            raise Exception('already polling')
        self._poller = DirectoryPoller(self.directory, self.wildcard, self.on_data_files, self.on_polling_exception, self.interval)
        self._poller.start()

    def stop_polling(self):
        self._poller.shutdown()

    def on_data_files(self, files):
        # handle synchronously, will delay next polling cycle until these files have been handled
        for file in files:
            self.handle_file(file)

    def on_polling_exception(self, exception):
        log.error('error polling for new files', exc_info=True)
        #self._polling_exception = exception
        # change agent state to idle
        # save state?

    def handle_file(self, file):
        parser = self.parser_class(file, parse_after=self.latest_granule_time)
        records = parser.get_records(max_count=self.max_records)
        while records:
            # secretly uses pubsub client
            rdt = RecordDictionaryTool(param_dictionary=self.parameter_dictionary)
            for key in records[0]: #assume all dict records have same keys
                rdt[key] = [ record[key] for record in records ]
            g = rdt.to_granule()
            self.publisher.publish(g)
            records = parser.get_records(max_count=self.max_records)
        self._set_state('last_file_parsed', file)
