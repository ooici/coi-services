#!/usr/bin/env python

import uuid
import json
from time import time

import gevent
from couchdb.http import ResourceNotFound

from pyon.agent.simple_agent import SimpleResourceAgentClient
from pyon.net.endpoint import Subscriber
from pyon.public import log
from pyon.core.exception import NotFound, BadRequest, ServerError
from pyon.util.containers import create_valid_identifier
from pyon.event.event import EventPublisher
from pyon.core import bootstrap
from pyon.event.event import EventSubscriber
from gevent import event as gevent_event

try:
    from epu.processdispatcher.core import ProcessDispatcherCore
    from epu.processdispatcher.store import get_processdispatcher_store
    from epu.processdispatcher.engines import EngineRegistry
    from epu.processdispatcher.matchmaker import PDMatchmaker
    from epu.dashiproc.epumanagement import EPUManagementClient
except ImportError:
    ProcessDispatcherCore = None
    get_processdispatcher_store = None
    EngineRegistry = None
    PDMatchmaker = None
    EPUManagementClient = None


from ion.agents.cei.execution_engine_agent import ExecutionEngineAgentClient

from interface.services.cei.iprocess_dispatcher_service import BaseProcessDispatcherService
from interface.objects import ProcessStateEnum, Process, ProcessDefinition,\
    ProcessQueueingMode, ProcessRestartMode, ProcessTarget, ProcessSchedule


class ProcessStateGate(EventSubscriber):
    """
    Ensure that we get a particular state, now or in the future.

    Usage:
      gate = ProcessStateGate(your_process_dispatcher_client.read_process, process_id, ProcessStateEnum.some_state)
      assert gate.await(timeout_in_seconds)

    This pattern returns True immediately upon reaching the desired state, or False if the timeout is reached.
    This pattern avoids a race condition between read_process and using EventGate.
    """
    def __init__(self, read_process_fn=None, process_id='', desired_state=None, *args, **kwargs):

        EventSubscriber.__init__(self, *args,
                                 callback=self.trigger_cb,
                                 event_type="ProcessLifecycleEvent",
                                 origin=process_id,
                                 origin_type="DispatchedProcess",
                                 **kwargs)

        self.desired_state = desired_state
        self.process_id = process_id
        self.read_process_fn = read_process_fn
        self.last_chance = None
        self.first_chance = None


        _ = ProcessStateEnum._str_map[self.desired_state] # make sure state exists
        log.info("ProcessStateGate is going to wait on process '%s' for state '%s'",
                self.process_id,
                ProcessStateEnum._str_map[self.desired_state])

    def trigger_cb(self, event, x):
        if event.state == self.desired_state:
            self.gate.set()
        else:
            log.info("ProcessStateGate received an event for state %s, wanted %s",
                     ProcessStateEnum._str_map[event.state],
                     ProcessStateEnum._str_map[self.desired_state])
            log.info("ProcessStateGate received (also) variable x = %s", x)

    def in_desired_state(self):
        # check whether the process we are monitoring is in the desired state as of this moment
        # Once pd creates the process, process_obj is never None
        try:
            process_obj = self.read_process_fn(self.process_id)
            return (process_obj and self.desired_state == process_obj.process_state)
        except NotFound:
            return False

    def await(self, timeout=0):
        #set up the event gate so that we don't miss any events
        start_time = time()
        self.gate = gevent_event.Event()
        self.start()

        #if it's in the desired state, return immediately
        if self.in_desired_state():
            self.first_chance = True
            self.stop()
            log.info("ProcessStateGate found process already %s -- NO WAITING",
                     ProcessStateEnum._str_map[self.desired_state])
            return True

        #if the state was not where we want it, wait for the event.
        ret = self.gate.wait(timeout)
        self.stop()

        if ret:
            # timer is already stopped in this case
            log.info("ProcessStateGate received %s event after %0.2f seconds",
                     ProcessStateEnum._str_map[self.desired_state],
                     time() - start_time)
        else:
            log.info("ProcessStateGate timed out waiting to receive %s event",
                     ProcessStateEnum._str_map[self.desired_state])

            # sanity check for this pattern
            self.last_chance = self.in_desired_state()

            if self.last_chance:
                log.warn("ProcessStateGate was successful reading %s on last_chance; " +
                         "should the state change for '%s' have taken %s seconds exactly?",
                         ProcessStateEnum._str_map[self.desired_state],
                         self.process_id,
                         timeout)

        return ret or self.last_chance

    def _get_last_chance(self):
        return self.last_chance

    def _get_first_chance(self):
        return self.first_chance


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

        if pd_conf.get('dashi_messaging', False) == True:

            dashi_name = get_pd_dashi_name()

            # grab config parameters used to connect to dashi
            try:
                uri = pd_conf.dashi_uri
                exchange = pd_conf.dashi_exchange
            except AttributeError, e:
                log.warn("Needed Process Dispatcher config not found: %s", e)
                raise
            self.dashi = get_dashi(dashi_name, uri, exchange)
        else:
            self.dashi = None

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

        if self.dashi is not None:
            self.dashi_handler = PDDashiHandler(self.backend, self.dashi)

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
        # validate executable
        executable = process_definition.executable
        if not executable:
            raise BadRequest("invalid process executable")

        module = executable.get('module')
        cls = executable.get('class')

        if not (module and cls):
            raise BadRequest("process executable must have module and class")
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

        process = Process(process_id=process_id)
        self.container.resource_registry.create(process, object_id=process_id)

        return process_id

    def schedule_process(self, process_definition_id='', schedule=None, configuration=None, process_id='', name=''):
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

        # If not provided, create a unique but still descriptive (valid) id
        if not process_id:
            process_id = str(process_definition.name or "process") + uuid.uuid4().hex
            process_id = create_valid_identifier(process_id, ws_sub='_')

        # If not provided, create a unique but still descriptive (valid) name
        if not name:
            name = self._get_process_name(process_definition, configuration)

        try:
            process = Process(process_id=process_id, name=name)
            self.container.resource_registry.create(process, object_id=process_id)
        except BadRequest:
            log.debug("Tried to create Process %s, but already exists. This is normally ok.", process_id)

        return self.backend.spawn(process_id, process_definition_id, schedule, configuration, name)

    def cancel_process(self, process_id=''):
        """Cancels the execution of the given process id.

        @param process_id    str
        @retval success    bool
        @throws NotFound    object with specified id does not exist
        """
        if not process_id:
            raise NotFound('No process was provided')

        cancel_result = self.backend.cancel(process_id)
        self.container.resource_registry.delete(process_id, del_associations=True)
        return cancel_result

    def read_process(self, process_id=''):
        """Returns a Process as an object.

        @param process_id    str
        @retval process    Process
        @throws NotFound    object with specified id does not exist
        """
        if not process_id:
            raise NotFound('No process was provided')

        return self.backend.read_process(process_id)

    def list_processes(self):
        """Lists managed processes

        @retval processes    list
        """
        return self.backend.list()

    def _get_process_name(self, process_definition, configuration):

        ha_pd_id = configuration.get('highavailability', {}).get('process_definition_id')
        name_suffix = ""
        if ha_pd_id is not None:
            process_definition = self.backend.read_definition(ha_pd_id)
            name_suffix = "ha"

        name_parts = [str(process_definition.name or "process")]
        if name_suffix:
            name_parts.append(name_suffix)
        name_parts.append(uuid.uuid4().hex)
        name = '-'.join(name_parts)

        return name


class PDDashiHandler(object):
    """Dashi messaging handlers for the Process Dispatcher"""

    def __init__(self, backend, dashi):
        self.backend = backend
        self.dashi = dashi

        self.dashi.handle(self.create_definition)
        self.dashi.handle(self.describe_definition)
        self.dashi.handle(self.update_definition)
        self.dashi.handle(self.remove_definition)
        self.dashi.handle(self.list_definitions)
        self.dashi.handle(self.schedule_process)
        self.dashi.handle(self.describe_process)
        self.dashi.handle(self.describe_processes)
        self.dashi.handle(self.restart_process)
        self.dashi.handle(self.terminate_process)

    def create_definition(self, definition_id, definition_type, executable,
                          name=None, description=None):

        definition = ProcessDefinition(name=name, description=description,
                definition_type=definition_type, executable=executable)
        return self.backend.create_definition(definition, definition_id)

    def describe_definition(self, definition_id):
        return _core_process_definition_from_ion(self.backend.read_definition(definition_id))

    def update_definition(self, definition_id, definition_type, executable,
                          name=None, description=None):
        raise BadRequest("The Pyon PD does not support updating process definitions")

    def remove_definition(self, definition_id):
        self.backend.delete_definition(definition_id)

    def list_definitions(self):
        raise BadRequest("The Pyon PD does not support listing process definitions")

    def schedule_process(self, upid, definition_id=None, definition_name=None,
                         configuration=None, subscribers=None, constraints=None,
                         queueing_mode=None, restart_mode=None,
                         execution_engine_id=None, node_exclusive=None, name=None):

        if definition_id:
            process_definition = self.backend.read_definition(definition_id)

        elif definition_name:
            log.info("scheduling process by definition name: '%s'", definition_name)
            process_definition = self.backend.read_definition_by_name(definition_name)
            definition_id = process_definition._id

        else:
            raise NotFound('No process definition id or name was provided')


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

        target = ProcessTarget()
        if execution_engine_id is not None:
            target.execution_engine_id = execution_engine_id
        if node_exclusive is not None:
            target.node_exclusive = node_exclusive
        if constraints is not None:
            target.constraints = constraints

        schedule = ProcessSchedule(target=target)
        if queueing_mode is not None:
            try:
                schedule.queueing_mode = ProcessQueueingMode._value_map[queueing_mode]
            except KeyError:
                msg = "%s is not a known ProcessQueueingMode" % (queueing_mode)
                raise BadRequest(msg)

        if restart_mode is not None:
            try:
                schedule.restart_mode = ProcessRestartMode._value_map[restart_mode]
            except KeyError:
                msg = "%s is not a known ProcessRestartMode" % (restart_mode)
                raise BadRequest(msg)

        # If not provided, create a unique but still descriptive (valid) name
        if not name:
            name = self._get_process_name(process_definition, configuration)

        return self.backend.spawn(upid, definition_id, schedule, configuration, name)

    def describe_process(self, upid):
        if hasattr(self.backend, 'read_core_process'):
            return self.backend.read_core_process(upid)
        else:
            return _core_process_from_ion(self.backend.read_process(upid))

    def describe_processes(self):
        if hasattr(self.backend, 'read_core_process'):
            return [self.backend.read_core_process(proc.process_id) for proc in self.backend.list()]
        else:
            return [_core_process_from_ion(proc) for proc in self.backend.list()]

    def restart_process(self, upid):
        raise BadRequest("The Pyon PD does not support restarting processes")

    def terminate_process(self, upid):
        return self.backend.cancel(upid)

    def _get_process_name(self, process_definition, configuration):

        ha_pd_id = configuration.get('highavailability', {}).get('process_definition_id')
        name_suffix = ""
        if ha_pd_id is not None:
            process_definition = self.backend.read_definition(ha_pd_id)
            name_suffix = "ha"

        name_parts = [str(process_definition.name or "process")]
        if name_suffix:
            name_parts.append(name_suffix)
        name_parts.append(uuid.uuid4().hex)
        name = '-'.join(name_parts)

        return name


class PDLocalBackend(object):
    """Scheduling backend to PD that manages processes in the local container

    This implementation is the default and is used in single-container
    deployments where there is no CEI launch to leverage.
    """

    # We attempt to make the local backend act a bit more like the real thing.
    # Process spawn requests are asynchronous (not completed by the time the
    # operation returns). Therefore, callers need to listen for events to find
    # the success of failure of the process launch. To make races here more
    # detectable, we introduce an artificial delay between when
    # schedule_process() returns and when the process is actually launched.
    SPAWN_DELAY = 0

    def __init__(self, container):
        self.container = container
        self.event_pub = EventPublisher()
        self._processes = []

        self._spawn_greenlets = set()

        # use the container RR instance -- talks directly to couchdb
        self.rr = container.resource_registry

    def initialize(self):
        pass

    def shutdown(self):
        if self._spawn_greenlets:
            try:
                gevent.killall(list(self._spawn_greenlets), block=True)
            except Exception:
                log.warn("Ignoring error while killing spawn greenlets", exc_info=True)
            self._spawn_greenlets.clear()

    def create_definition(self, definition, definition_id=None):
        pd_id, version = self.rr.create(definition, object_id=definition_id)
        return pd_id

    def read_definition(self, definition_id):
        return self.rr.read(definition_id)

    def read_definition_by_name(self, definition_name):
        raise ServerError("reading process definitions by name not supported by this backend")

    def delete_definition(self, definition_id):
        return self.rr.delete(definition_id)

    def spawn(self, process_id, definition_id, schedule, configuration, name):

        definition = self.read_definition(definition_id)

        # in order for this local backend to behave more like the real thing,
        # we introduce an artificial delay in spawn requests. This helps flush
        # out races where callers try to use a process before it is necessarily
        # running.

        if self.SPAWN_DELAY:
            glet = gevent.spawn_later(self.SPAWN_DELAY, self._inner_spawn,
                process_id, definition, schedule, configuration)
            self._spawn_greenlets.add(glet)

            self._add_process(process_id, configuration, None)

        else:
            self._add_process(process_id, configuration, None)
            self._inner_spawn(process_id, name, definition, schedule, configuration)

        return process_id

    def _inner_spawn(self, process_id, process_name, definition, schedule, configuration):

        name = process_name
        module = definition.executable['module']
        cls = definition.executable['class']

        self.event_pub.publish_event(event_type="ProcessLifecycleEvent",
            origin=process_id, origin_type="DispatchedProcess",
            state=ProcessStateEnum.PENDING)

        # Spawn the process
        pid = self.container.spawn_process(name=name, module=module, cls=cls,
            config=configuration, process_id=process_id)
        log.debug('PD: Spawned Process (%s)', pid)

        # update state on the existing process
        process = self._get_process(process_id)
        process.process_state = ProcessStateEnum.RUNNING

        self.event_pub.publish_event(event_type="ProcessLifecycleEvent",
            origin=process_id, origin_type="DispatchedProcess",
            state=ProcessStateEnum.RUNNING)

        if self.SPAWN_DELAY:
            glet = gevent.getcurrent()
            if glet:
                self._spawn_greenlets.discard(glet)

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
            state=ProcessStateEnum.TERMINATED)

        return True

    def read_process(self, process_id):
        process = self._get_process(process_id)
        if process is None:
            raise NotFound("process %s unknown" % process_id)
        return process

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

_PD_PROCESS_STATE_MAP = {
    "300-WAITING": ProcessStateEnum.WAITING,
    "400-PENDING": ProcessStateEnum.PENDING,
    "500-RUNNING": ProcessStateEnum.RUNNING,
    "600-TERMINATING": ProcessStateEnum.TERMINATING,
    "700-TERMINATED": ProcessStateEnum.TERMINATED,
    "800-EXITED": ProcessStateEnum.EXITED,
    "850-FAILED": ProcessStateEnum.FAILED,
    "900-REJECTED": ProcessStateEnum.REJECTED
}

_PD_PYON_PROCESS_STATE_MAP = {
    ProcessStateEnum.WAITING: "300-WAITING",
    ProcessStateEnum.PENDING: "400-PENDING",
    ProcessStateEnum.RUNNING: "500-RUNNING",
    ProcessStateEnum.TERMINATING: "600-TERMINATING",
    ProcessStateEnum.TERMINATED: "700-TERMINATED",
    ProcessStateEnum.EXITED: "800-EXITED",
    ProcessStateEnum.FAILED: "850-FAILED",
    ProcessStateEnum.REJECTED: "900-REJECTED"
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

    def _get_client_for_eeagent(self, eeagent_id):
        eeagent_id = str(eeagent_id)

        resource_client = SimpleResourceAgentClient(eeagent_id, name=eeagent_id, process=self.process)
        return ExecutionEngineAgentClient(resource_client)

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
        default_engine = conf.get('default_engine')
        if default_engine is None and len(engine_conf.keys()) == 1:
            default_engine = engine_conf.keys()[0]
        self.CFG = service.CFG
        self.store = get_processdispatcher_store(self.CFG, use_gevent=True)
        self.store.initialize()
        self.registry = EngineRegistry.from_config(engine_conf, default=default_engine)

        # The Process Dispatcher communicates with EE Agents over ION messaging
        # but it still uses dashi to talk to the EPU Management Service, until
        # it also is fronted with an ION interface.

        if service.dashi is not None:
            self.dashi = service.dashi
        else:
            dashi_name = get_pd_dashi_name()

            # grab config parameters used to connect to dashi
            try:
                uri = conf.dashi_uri
                exchange = conf.dashi_exchange
            except AttributeError, e:
                log.warn("Needed Process Dispatcher config not found: %s", e)
                raise

            self.dashi = get_dashi(dashi_name, uri, exchange)

        dashi_name = self.dashi.name

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
            self.dashi.handle(self._domain_subscription_callback, "node_state")
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

    def _domain_subscription_callback(self, node_id, domain_id, state, properties=None):
        """Callback from Dashi EPUM service when an instance changes state
        """
        self.core.node_state(node_id, domain_id, state, properties=properties)

    def _heartbeat_callback(self, heartbeat, headers):
        log.debug("Got EEAgent heartbeat. headers=%s msg=%s", headers, heartbeat)

        try:
            eeagent_id = heartbeat['eeagent_id']
            beat = heartbeat['beat']
        except KeyError, e:
            log.warn("Invalid EEAgent heartbeat received. Missing: %s -- %s", e, heartbeat)
            return

        try:
            self.core.ee_heartbeat(eeagent_id, beat)
        except (NotFound, ResourceNotFound, ServerError):
            # This exception catches a race condition, where:
            # 1. EEagent spawns and starts heartbeater
            # 2. heartbeat gets sent
            # 3. PD recieves heartbeat and tries to send a message but EEAgent,
            #    hasn't been registered yet
            log.exception("Problem processing heartbeat from eeagent")
        except Exception:
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
        return _ion_process_definition_from_core(definition_id, definition)

    def read_definition_by_name(self, definition_name):

        # this is slow but only used from launch plan so hopefully that is ok
        definition_ids = self.core.list_definitions()

        # pick the first definition that matches
        for definition_id in definition_ids:
            definition = self.core.describe_definition(definition_id)
            if definition and definition.name == definition_name:
                return _ion_process_definition_from_core(definition_id, definition)

        raise NotFound("process definition with name '%s' not found" % definition_name)

    def delete_definition(self, definition_id):

        self.core.remove_definition(definition_id)

        # also delete in RR
        self.rr.delete(definition_id)

    def spawn(self, process_id, definition_id, schedule, configuration, name):

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

        self.core.schedule_process(None, upid=process_id, definition_id=definition_id,
            subscribers=None, constraints=constraints,
            node_exclusive=node_exclusive, queueing_mode=queueing_mode,
            execution_engine_id=execution_engine_id,
            restart_mode=restart_mode, configuration=configuration, name=name)

        return process_id

    def cancel(self, process_id):
        result = self.core.terminate_process(None, upid=process_id)
        return bool(result)

    def list(self):
        d_processes = self.core.describe_processes()
        return [_ion_process_from_core(p) for p in d_processes]

    def read_process(self, process_id):
        d_process = self.core.describe_process(None, process_id)
        if d_process is None:
            raise NotFound("process %s unknown" % process_id)
        process = _ion_process_from_core(d_process)

        return process

    def read_core_process(self, process_id):
        return self.core.describe_process(None, process_id)


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
        return _ion_process_definition_from_core(definition_id, definition)

    def read_definition_by_name(self, definition_name):
        raise ServerError("reading process definitions by name not supported by this backend")

    def delete_definition(self, definition_id):
        self.dashi.call(self.topic, "remove_definition",
            definition_id=definition_id)

        self.rr.delete(definition_id)

    def spawn(self, process_id, definition_id, schedule, configuration, name):

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
            upid=process_id, definition_id=definition_id, subscribers=self.pd_process_subscribers,
            constraints=constraints, configuration=config, name=name)

        log.debug("Dashi Process Dispatcher returned process: %s", proc)

        # upid == process_id
        return process_id

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
        if d_process is None:
            raise NotFound("process %s unknown" % process_id)
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

    process = Process(process_id=process_id,
        process_state=ion_process_state,
        process_configuration=config,
        name=core_process.get('name'))

    return process

def _core_process_from_ion(ion_process):
    process = {
            'state': _PD_PYON_PROCESS_STATE_MAP.get(ion_process.process_state),
            'upid': ion_process.process_id,
            'name': ion_process.name,
            'configuration': ion_process.process_configuration,
    }
    return process

def _ion_process_definition_from_core(definition_id, core_process_definition):
    procdef = ProcessDefinition(name=core_process_definition.get('name'),
        description=core_process_definition.get('description'),
        definition_type=core_process_definition.get('definition_type'),
        executable=core_process_definition.get('executable'))
    procdef._id = definition_id
    return procdef

def _core_process_definition_from_ion(ion_process_definition):
    definition = {
            'name': ion_process_definition.name,
            'description': ion_process_definition.description,
            'definition_type': ion_process_definition.definition_type,
            'executable': ion_process_definition.executable,
            }
    return definition



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
