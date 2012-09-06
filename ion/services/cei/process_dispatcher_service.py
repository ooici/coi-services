#!/usr/bin/env python
from pyon.agent.simple_agent import SimpleResourceAgentClient
from pyon.net.endpoint import Subscriber

__author__ = 'Stephen P. Henrie, Michael Meisinger'
__license__ = 'Apache 2.0'

import uuid
import json

import gevent

from pyon.public import log
from pyon.core.exception import NotFound, BadRequest, ServerError
from pyon.util.containers import create_valid_identifier
from pyon.event.event import EventPublisher
from pyon.core import bootstrap
from couchdb.http import ResourceNotFound

try:
    from epu.processdispatcher.core import ProcessDispatcherCore
    from epu.processdispatcher.store import ProcessDispatcherStore
    from epu.processdispatcher.engines import EngineRegistry
    from epu.processdispatcher.matchmaker import PDMatchmaker
    from epu.dashiproc.epumanagement import EPUManagementClient
except ImportError:
    ProcessDispatcherCore = None
    ProcessDispatcherStore = None
    EngineRegistry = None
    PDMatchmaker = None
    EPUManagementClient = None

from ion.agents.cei.execution_engine_agent import ExecutionEngineAgentClient

from interface.services.cei.iprocess_dispatcher_service import BaseProcessDispatcherService
from interface.objects import ProcessStateEnum, Process
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.objects import ProcessStateEnum, ProcessDefinition, ProcessDefinitionType,\
        ProcessQueueingMode, ProcessRestartMode


class ProcessDispatcherService(BaseProcessDispatcherService):

    # Implementation notes:
    #
    # The Process Dispatcher (PD) core functionality lives in a different
    # repository: https://github.com/ooici/epu
    #
    # This PD operates in a few different modes, as implemented in the
    # backend classes below:
    #
    #   local container mode - spawn directly in the local container
    #       without going through any external CEI functionality. This is
    #       the default mode.
    #
    #   native mode - run the full process dispatcher stack natively in the
    #       container. This is the production deployment mode. Note that
    #       because this mode still relies on communication with the external
    #       CEI EPUM Management Service, this mode cannot be directly used
    #       outside of a CEI launch.
    #
    #   bridge mode - this is a deprecated mode where the real Process
    #       Dispatcher runs as an external service and this service "bridges"
    #       requests to it.
    #

    def on_init(self):

        try:
            pd_conf = self.CFG.processdispatcher
        except AttributeError:
            pd_conf = {}

        # temporarily supporting old config format
        try:
            pd_bridge_conf = self.CFG.process_dispatcher_bridge
        except AttributeError:
            pd_bridge_conf = None

        pd_backend_name = pd_conf.get('backend')

        # note: this backend is deprecated. Keeping it around only until the system launches
        # are switched to the Pyon PD implementation.
        if pd_bridge_conf:
            log.debug("Using Process Dispatcher Bridge backend -- requires running CEI services.")
            self.backend = PDBridgeBackend(pd_bridge_conf, self)

        elif not pd_backend_name or pd_backend_name == "container":
            log.debug("Using Process Dispatcher container backend -- spawns processes in local container")
            self.backend = PDLocalBackend(self.container)

        elif pd_backend_name == "native":
            log.debug("Using Process Dispatcher native backend")
            self.backend = PDNativeBackend(pd_conf, self)

        else:
            raise Exception("Unknown Process Dispatcher backend: %s" % pd_backend_name)

    def on_start(self):
        self.backend.initialize()

    def on_quit(self):
        self.backend.shutdown()

    def create_process_definition(self, process_definition=None, process_definition_id=None):
        """Creates a Process Definition based on given object.

        @param process_definition    ProcessDefinition
        @param process_definition_id desired process definition ID
        @retval process_definition_id    str
        @throws BadRequest    if object passed has _id or _rev attribute
        """
        return self.backend.create_definition(process_definition, process_definition_id)

    def read_process_definition(self, process_definition_id=''):
        """Returns a Process Definition as object.

        @param process_definition_id    str
        @retval process_definition    ProcessDefinition
        @throws NotFound    object with specified id does not exist
        """
        return self.backend.read_definition(process_definition_id)

    def delete_process_definition(self, process_definition_id=''):
        """Deletes/retires a Process Definition.

        @param process_definition_id    str
        @throws NotFound    object with specified id does not exist
        """
        self.backend.delete_definition(process_definition_id)

    def associate_execution_engine(self, process_definition_id='', execution_engine_definition_id=''):
        """Declare that the given process definition is compatible with the given execution engine.

        @param process_definition_id    str
        @param execution_engine_definition_id    str
        @throws NotFound    object with specified id does not exist
        """
        #TODO EE Management is not yet supported

    def dissociate_execution_engine(self, process_definition_id='', execution_engine_definition_id=''):
        """Remove the association of the process definition with an execution engine.

        @param process_definition_id    str
        @param execution_engine_definition_id    str
        @throws NotFound    object with specified id does not exist
        """
        #TODO EE Management is not yet supported

    def create_process(self, process_definition_id=''):
        """Create a process resource and process id. Does not yet start the process

        @param process_definition_id    str
        @retval process_id    str
        @throws NotFound    object with specified id does not exist
        """
        if not process_definition_id:
            raise NotFound('No process definition was provided')
        process_definition = self.backend.read_definition(process_definition_id)

        # try to get a unique but still descriptive name
        process_id = str(process_definition.name or "process") + uuid.uuid4().hex
        process_id = create_valid_identifier(process_id, ws_sub='_')

        # TODO: Create a resource object or directory entry here?

        return process_id

    def schedule_process(self, process_definition_id='', schedule=None, configuration=None, process_id=''):
        """Schedule a process definition for execution on an Execution Engine. If no process id is given,
        a new unique ID is generated.

        @param process_definition_id    str
        @param schedule    ProcessSchedule
        @param configuration    IngestionConfiguration
        @param process_id    str
        @retval process_id    str
        @throws BadRequest    if object passed has _id or _rev attribute
        @throws NotFound    object with specified id does not exist
        """
        if not process_definition_id:
            raise NotFound('No process definition was provided')
        process_definition = self.backend.read_definition(process_definition_id)

        # early validation before we pass definition through to backend
        try:
            module = process_definition.executable['module']
            cls = process_definition.executable['class']
        except KeyError, e:
            raise BadRequest("Process definition incomplete. missing: %s", e)

        if configuration is None:
            configuration = {}
        else:
            # push the config through a JSON serializer to ensure that the same
            # config would work with the bridge backend

            try:
                json.dumps(configuration)
            except TypeError, e:
                raise BadRequest("bad configuration: " + str(e))

        # If not provided, create a unique but still descriptive (valid) name
        if not process_id:
            process_id = str(process_definition.name or "process") + uuid.uuid4().hex
            process_id = create_valid_identifier(process_id, ws_sub='_')

        return self.backend.spawn(process_id, process_definition_id, schedule, configuration)

    def cancel_process(self, process_id=''):
        """Cancels the execution of the given process id.

        @param process_id    str
        @retval success    bool
        @throws NotFound    object with specified id does not exist
        """
        if not process_id:
            raise NotFound('No process was provided')

        return self.backend.cancel(process_id)

    def read_process(self, process_id=''):
        """Returns a Process as an object.

        @param process_id    str
        @retval process    Process
        @throws NotFound    object with specified id does not exist
        """
        pass
        if not process_id:
            raise NotFound('No process was provided')

        return self.backend.read_process(process_id)

    def list_processes(self):
        """Lists managed processes

        @retval processes    list
        """
        return self.backend.list()


class PDLocalBackend(object):
    """Scheduling backend to PD that manages processes in the local container

    This implementation is the default and is used in single-container
    deployments where there is no CEI launch to leverage.
    """

    def __init__(self, container):
        self.container = container
        self.event_pub = EventPublisher()
        self._processes = []

        # use the container RR instance -- talks directly to couchdb
        self.rr = container.resource_registry

    def initialize(self):
        pass

    def shutdown(self):
        pass

    def create_definition(self, definition, definition_id=None):
        if definition_id:
            raise BadRequest("specifying process definition IDs is not supported in local backend")
        pd_id, version = self.rr.create(definition)
        return pd_id

    def read_definition(self, definition_id):
        return self.rr.read(definition_id)

    def delete_definition(self, definition_id):
        return self.rr.delete(definition_id)

    def spawn(self, name, definition_id, schedule, configuration):

        definition = self.read_definition(definition_id)

        module = definition.executable['module']
        cls = definition.executable['class']

        # Spawn the process
        pid = self.container.spawn_process(name=name, module=module, cls=cls,
            config=configuration, process_id=name)
        log.debug('PD: Spawned Process (%s)', pid)
        self._add_process(pid, configuration, ProcessStateEnum.SPAWN)

        self.event_pub.publish_event(event_type="ProcessLifecycleEvent",
            origin=name, origin_type="DispatchedProcess",
            state=ProcessStateEnum.SPAWN)

        return pid

    def cancel(self, process_id):
        self.container.proc_manager.terminate_process(process_id)
        log.debug('PD: Terminated Process (%s)', process_id)
        try:
            self._remove_process(process_id)
        except ValueError:
            log.warning("PD: No record of %s to remove?" % process_id)

        self.event_pub.publish_event(event_type="ProcessLifecycleEvent",
            origin=process_id, origin_type="DispatchedProcess",
            state=ProcessStateEnum.TERMINATE)

        return True

    def read_process(self, process_id):
        return self._get_process(process_id)

    def _add_process(self, pid, config, state):
        proc = Process(process_id=pid, process_state=state,
                process_configuration=config)

        self._processes.append(proc)

    def _remove_process(self, pid):
        self._processes = filter(lambda u: u.process_id != pid, self._processes)

    def _get_process(self, pid):
        wanted_procs = filter(lambda u: u.process_id == pid, self._processes)
        if len(wanted_procs) >= 1:
            return wanted_procs[0]
        else:
            return None

    def list(self):
        return self._processes


# map from internal PD states to external ProcessStateEnum values

# some states are known and ignored
_PD_IGNORED_STATE = object()

_PD_PROCESS_STATE_MAP = {
    "400-PENDING": _PD_IGNORED_STATE,
    "500-RUNNING": ProcessStateEnum.SPAWN,
    "600-TERMINATING": ProcessStateEnum.TERMINATE,
    "700-TERMINATED": ProcessStateEnum.TERMINATE,
    "800-EXITED": ProcessStateEnum.TERMINATE,
    "850-FAILED": ProcessStateEnum.ERROR,
    "900-REJECTED": ProcessStateEnum.ERROR
}


class Notifier(object):
    """Sends Process state notifications via ION events

    This object is fed into the internal PD core classes
    """
    def __init__(self):
        self.event_pub = EventPublisher()

    def notify_process(self, process):
        process_id = process.upid
        state = process.state

        ion_process_state = _PD_PROCESS_STATE_MAP.get(state)
        if not ion_process_state:
            log.debug("Received unknown process state from Process Dispatcher." +
                      " process=%s state=%s", process_id, state)
            return
        if ion_process_state is _PD_IGNORED_STATE:
            return

        log.debug("Emitting event for process state. process=%s state=%s", process_id, ion_process_state)
        self.event_pub.publish_event(event_type="ProcessLifecycleEvent",
            origin=process_id, origin_type="DispatchedProcess",
            state=ion_process_state)


# should be configurable to support multiple process dispatchers?
DEFAULT_HEARTBEAT_QUEUE = "heartbeats"


class HeartbeatSubscriber(Subscriber):
    """Custom subscriber to handle incoming EEAgent heartbeats
    """
    def __init__(self, queue_name, callback, **kwargs):
        self.callback = callback

        Subscriber.__init__(self, from_name=queue_name, callback=callback,
            **kwargs)

    def _callback(self, *args, **kwargs):
        self.callback(*args, **kwargs)

    def start(self):
        gl = gevent.spawn(self.listen)
        self._cbthread = gl
        self.get_ready_event().wait(5)
        return gl

    def stop(self):
        self.close()
        self._cbthread.join(timeout=5)
        self._cbthread.kill()
        self._cbthread = None


class AnyEEAgentClient(object):
    """Client abstraction for talking to any EEAgent
    """
    def __init__(self, process):
        self.process = process

    def _get_client_for_eeagent(self, resource_id, attempts=10):
        exception = None
        for i in range(0, attempts):
            try:
                resource_client = SimpleResourceAgentClient(resource_id, process=self.process)
                return ExecutionEngineAgentClient(resource_client)
            except (NotFound, ResourceNotFound, ServerError), e:
                # This exception catches a race condition, where:
                # 1. EEagent spawns and starts heartbeater
                # 2. heartbeat gets sent
                # 3. PD recieves heartbeat and tries to send a message but EEAgent,
                #    hasn't been registered yet
                #
                # So, we try it a few times hoping that it'll come up
                log.exception("Couldn't get eeagent client")
                gevent.sleep(1)
                exception = e
        else:
            raise exception


    def launch_process(self, eeagent, upid, round, run_type, parameters):
        client = self._get_client_for_eeagent(eeagent)
        log.debug("sending launch request to EEAgent")
        return client.launch_process(upid, round, run_type, parameters)

    def restart_process(self, eeagent, upid, round):
        client = self._get_client_for_eeagent(eeagent)
        return client.restart_process(upid, round)

    def terminate_process(self, eeagent, upid, round):
        client = self._get_client_for_eeagent(eeagent)
        return client.terminate_process(upid, round)

    def cleanup_process(self, eeagent, upid, round):
        client = self._get_client_for_eeagent(eeagent)
        return client.cleanup_process(upid, round)

    def dump_state(self, eeagent):
        client = self._get_client_for_eeagent(eeagent)
        return client.dump_state()


class PDNativeBackend(object):
    """Scheduling backend to PD that runs directly in the container
    """

    def __init__(self, conf, service):
        engine_conf = conf.get('engines', {})
        self.store = ProcessDispatcherStore()
        self.registry = EngineRegistry.from_config(engine_conf)

        # The Process Dispatcher communicates with EE Agents over ION messaging
        # but it still uses dashi to talk to the EPU Management Service, until
        # it also is fronted with an ION interface.

        dashi_name = get_pd_dashi_name()

        # grab config parameters used to connect to dashi
        try:
            uri = conf.dashi_uri
            exchange = conf.dashi_exchange
        except AttributeError, e:
            log.warn("Needed Process Dispatcher config not found: %s", e)
            raise

        self.dashi = get_dashi(dashi_name, uri, exchange)

        # "static resources" mode is used in lightweight launch where the PD
        # has a fixed set of Execution Engines and cannot ask for more.
        if conf.get('static_resources'):
            base_domain_config = None
            domain_definition_id = None
            epum_client = None

        else:
            base_domain_config = conf.get('domain_config')
            domain_definition_id = conf.get('definition_id')

            epum_client = EPUManagementClient(self.dashi,
                "epu_management_service")

        self.notifier = Notifier()

        self.eeagent_client = AnyEEAgentClient(service)

        run_type = 'pyon'

        self.core = ProcessDispatcherCore(self.store, self.registry,
            self.eeagent_client, self.notifier)
        self.matchmaker = PDMatchmaker(self.store, self.eeagent_client,
            self.registry, epum_client, self.notifier, dashi_name,
            domain_definition_id, base_domain_config, run_type)

        heartbeat_queue = conf.get('heartbeat_queue', DEFAULT_HEARTBEAT_QUEUE)
        self.beat_subscriber = HeartbeatSubscriber(heartbeat_queue,
            callback=self._heartbeat_callback, node=service.container.node)

        # use the container RR instance -- talks directly to couchdb
        self.rr = service.container.resource_registry

    def initialize(self):

        # start consuming domain subscription messages from the dashi EPUM
        # service if needed.
        if self.dashi:
            self.dashi.handle(self._domain_subscription_callback, "dt_state")
            self.consumer_thread = gevent.spawn(self.dashi.consume)

        self.matchmaker.start_election()
        self.beat_subscriber.start()

    def shutdown(self):
        try:
            self.store.shutdown()
        except Exception:
            log.exception("Error shutting down Process Dispatcher store")

        try:
            if self.dashi:
                if self.consumer_thread:
                    self.dashi.cancel()
                    self.consumer_thread.join()
                self.dashi.disconnect()
        except Exception:
            log.exception("Error shutting down Process Dispatcher dashi consumer")

        self.beat_subscriber.stop()

    def _domain_subscription_callback(self, node_id, deployable_type, state,
                                      properties=None):
        """Callback from Dashi EPUM service when an instance changes state
        """
        self.core.dt_state(node_id, deployable_type, state,
            properties=properties)

    def _heartbeat_callback(self, heartbeat, headers):
        log.debug("Got EEAgent heartbeat. headers=%s msg=%s", headers, heartbeat)

        try:
            resource_id = heartbeat['resource_id']
            beat = heartbeat['beat']
        except KeyError, e:
            log.warn("Invalid EEAgent heartbeat received. Missing: %s -- %s", e, heartbeat)
            return

        try:
            self.core.ee_heartbeart(resource_id, beat)
        except (NotFound, ResourceNotFound, ServerError):
            # This exception catches a race condition, where:
            # 1. EEagent spawns and starts heartbeater
            # 2. heartbeat gets sent
            # 3. PD recieves heartbeat and tries to send a message but EEAgent,
            #    hasn't been registered yet
            log.exception("Problem processing heartbeat from eeagent")
        except:
            log.exception("Unexpected error while processing heartbeat")


    def create_definition(self, definition, definition_id=None):
        """
        @type definition: ProcessDefinition
        """
        definition_id = definition_id or uuid.uuid4().hex
        self.core.create_definition(definition_id, definition.definition_type,
            definition.executable, name=definition.name,
            description=definition.description)

        self.rr.create(definition, object_id=definition_id)

        return definition_id

    def read_definition(self, definition_id):
        definition = self.core.describe_definition(definition_id)
        if not definition:
            raise NotFound("process definition %s unknown" % definition_id)
        return _ion_process_definition_from_core(definition)

    def delete_definition(self, definition_id):

        self.core.remove_definition(definition_id)

        # also delete in RR
        self.rr.delete(definition_id)

    def spawn(self, name, definition_id, schedule, configuration):

        # note: not doing anything with schedule mode yet: the backend PD
        # service doesn't fully support it.

        constraints = None
        node_exclusive = None
        execution_engine_id = None
        if schedule and schedule.target:
            if schedule.target.constraints:
                constraints = schedule.target.constraints
            if schedule.target.node_exclusive:
                node_exclusive = schedule.target.node_exclusive
            if schedule.target.execution_engine_id:
                execution_engine_id = schedule.target.execution_engine_id

        queueing_mode = None
        restart_mode = None
        if schedule:
            if hasattr(schedule, 'queueing_mode') and schedule.queueing_mode:
                queueing_mode = ProcessQueueingMode._str_map.get(schedule.queueing_mode)
            if hasattr(schedule, 'restart_mode') and schedule.restart_mode:
                restart_mode = ProcessRestartMode._str_map.get(schedule.restart_mode)

        self.core.schedule_process(None, upid=name, definition_id=definition_id,
            subscribers=None, constraints=constraints,
            node_exclusive=node_exclusive, queueing_mode=queueing_mode,
            execution_engine_id=execution_engine_id,
            restart_mode=restart_mode, configuration=configuration)

        return name

    def cancel(self, process_id):
        result = self.core.terminate_process(None, upid=process_id)
        return bool(result)

    def list(self):
        d_processes = self.core.describe_processes()
        return [_ion_process_from_core(p) for p in d_processes]

    def read_process(self, process_id):
        d_process = self.core.describe_process(None, process_id)
        process = _ion_process_from_core(d_process)

        return process


class PDBridgeBackend(object):
    """Scheduling backend to PD that bridges to external CEI Process Dispatcher

    This is deprecated but we are leaving it around for the time being.
    """

    def __init__(self, conf, service):
        self.dashi = None
        self.consumer_thread = None

        # grab config parameters used to connect to backend Process Dispatcher
        try:
            self.uri = conf.dashi_uri
            self.topic = conf.topic
            self.exchange = conf.dashi_exchange
        except AttributeError, e:
            log.warn("Needed Process Dispatcher config not found: %s", e)
            raise

        self.dashi_name = self.topic + "_bridge"
        self.pd_process_subscribers = [(self.dashi_name, "process_state")]

        self.event_pub = EventPublisher()

        # use the container RR instance -- talks directly to couchdb
        self.rr = service.container.resource_registry

    def initialize(self):
        self.dashi = self._init_dashi()
        self.dashi.handle(self._process_state, "process_state")
        self.consumer_thread = gevent.spawn(self.dashi.consume)

    def shutdown(self):
        if self.dashi:
            self.dashi.cancel()
        if self.consumer_thread:
            self.consumer_thread.join()

    def _init_dashi(self):
        # we are avoiding directly depending on dashi as this bridging approach
        # is short term and only used from CEI launches. And we have enough
        # deps. Where needed we install dashi specially via a separate
        # buildout config.
        return get_dashi(self.dashi_name, self.uri, self.exchange)

    def _process_state(self, process):
        # handle incoming process state updates from the real PD service.
        # some states map to ION events while others are ignored.

        log.debug("Got process state: %s", process)

        process_id = None
        state = None
        if process:
            process_id = process.get('upid')
            state = process.get('state')

        if not (process and process_id and state):
            log.warn("Invalid process state from CEI process dispatcher: %s",
                process)
            return

        ion_process_state = _PD_PROCESS_STATE_MAP.get(state)
        if not ion_process_state:
            log.debug("Received unknown process state from Process Dispatcher." +
                      " process=%s state=%s", process_id, state)
            return
        if ion_process_state is _PD_IGNORED_STATE:
            return

        log.debug("Sending process state event: %s -> %s", process_id, ion_process_state)
        self.event_pub.publish_event(event_type="ProcessLifecycleEvent",
            origin=process_id, origin_type="DispatchedProcess",
            state=ion_process_state)

    def create_definition(self, definition, definition_id=None):
        """
        @type definition: ProcessDefinition
        """
        definition_id = definition_id or uuid.uuid4().hex
        args = dict(definition_id=definition_id,
            definition_type=definition.definition_type,
            executable=definition.executable, name=definition.name,
            description=definition.description)
        self.dashi.call(self.topic, "create_definition", args=args)

        self.rr.create(definition, object_id=definition_id)

        return definition_id

    def read_definition(self, definition_id):
        definition = self.dashi.call(self.topic, "describe_definition",
            definition_id=definition_id)
        if not definition:
            raise NotFound("process definition %s unknown" % definition_id)
        return _ion_process_definition_from_core(definition)

    def delete_definition(self, definition_id):
        self.dashi.call(self.topic, "remove_definition",
            definition_id=definition_id)

        self.rr.delete(definition_id)

    def spawn(self, name, definition_id, schedule, configuration):

        # note: not doing anything with schedule mode yet: the backend PD
        # service doesn't fully support it.

        constraints = None
        if schedule:
            if schedule.target and schedule.target.constraints:
                constraints = schedule.target.constraints

        # form a pyon process spec
        # warning: this spec will change in the near future.

        config = configuration or {}

        proc = self.dashi.call(self.topic, "schedule_process",
            upid=name, definition_id=definition_id, subscribers=self.pd_process_subscribers,
            constraints=constraints, configuration=config)

        log.debug("Dashi Process Dispatcher returned process: %s", proc)

        # name == upid == process_id
        return name

    def cancel(self, process_id):

        if not process_id:
            raise ValueError("invalid process id")

        proc = self.dashi.call(self.topic, "terminate_process", upid=process_id)
        log.debug("Dashi Process Dispatcher terminating process: %s", proc)
        return True

    def list(self):
        d_processes = self.dashi.call(self.topic, "describe_processes")
        return [_ion_process_from_core(p) for p in d_processes]

    def read_process(self, process_id):
        d_process = self.dashi.call(self.topic, "describe_process", upid=process_id)
        process = _ion_process_from_core(d_process)

        return process


def _ion_process_from_core(core_process):
    try:
        config = core_process['configuration']
    except KeyError:
        config = {}

    state = core_process.get('state')
    process_id = core_process.get('upid')
    ion_process_state = _PD_PROCESS_STATE_MAP.get(state)
    if not ion_process_state:
        log.debug("Process has unknown state: process=%s state=%s",
            process_id, state)
    if ion_process_state is _PD_IGNORED_STATE:
        ion_process_state = None

    process = Process(process_id=process_id,
        process_state=ion_process_state,
        process_configuration=config)

    return process

def _ion_process_definition_from_core(core_process_definition):
    return ProcessDefinition(name=core_process_definition.get('name'),
        description=core_process_definition.get('description'),
        definition_type=core_process_definition.get('definition_type'),
        executable=core_process_definition.get('executable'))

def get_dashi(*args, **kwargs):
    try:
        import dashi
    except ImportError:
        log.warn("Attempted to use Process Dispatcher but the "
                 "dashi library dependency is not available.")
        raise
    return dashi.DashiConnection(*args, **kwargs)


def get_pd_dashi_name():
    return "%s.dashi_process_dispatcher" % bootstrap.get_sys_name()
