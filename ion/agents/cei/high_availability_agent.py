from copy import deepcopy

import gevent
from gevent.event import Event

from pyon.agent.simple_agent import SimpleResourceAgent
from pyon.event.event import EventSubscriber
from pyon.public import log, get_sys_name
from pyon.core.exception import BadRequest, Timeout, NotFound
from pyon.core import bootstrap

from interface.objects import AgentCommand, ProcessSchedule, \
        ProcessStateEnum, ProcessQueueingMode, ProcessTarget, \
        ProcessRestartMode, Service, ServiceStateEnum
from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceClient
from ion.services.cei.process_dispatcher_service import ProcessDispatcherService, \
        process_state_to_pd_core

try:
    from epu.highavailability.core import HighAvailabilityCore
    import epu.highavailability.policy as policy
    from epu.states import ProcessState as CoreProcessState
except ImportError:
    HighAvailabilityCore = None
    policy = None
    CoreProcessState = None


"""
@package ion.agents.cei.high_availability_agent
@file ion/agents/cei/exehigh_availability_agent
@author Patrick Armstrong
@brief Pyon port of HAAgent
 """

DEFAULT_INTERVAL = 60


class HighAvailabilityAgent(SimpleResourceAgent):
    """Agent to manage high availability processes

    """

    def __init__(self):
        SimpleResourceAgent.__init__(self)
        self.dashi_handler = None
        self.service_id = None
        self.policy_thread = None
        self.policy_event = None

    def on_init(self):
        if not HighAvailabilityCore:
            msg = "HighAvailabilityCore isn't available. Use autolaunch.cfg buildout"
            log.error(msg)
            return

        policy_name = self.CFG.get_safe("highavailability.policy.name")
        if policy_name is None:
            msg = "HA service requires a policy name at CFG.highavailability.policy.name"
            raise Exception(msg)
        try:
            self.policy = policy.policy_map[policy_name.lower()]
        except KeyError:
            raise Exception("HA Service doesn't support '%s' policy" % policy_name)

        policy_parameters = self.CFG.get_safe("highavailability.policy.parameters")

        self.policy_interval = self.CFG.get_safe("highavailability.policy.interval",
                DEFAULT_INTERVAL)

        cfg = self.CFG.get_safe("highavailability")

        # use default PD name as the sole PD if none are provided in config
        self.pds = self.CFG.get_safe("highavailability.process_dispatchers",
            [ProcessDispatcherService.name])
        if not len(self.pds) == 1:
            raise Exception("HA Service doesn't support multiple Process Dispatchers")

        self.process_definition_id, self.process_definition = self._get_process_definition()

        self.process_configuration = self.CFG.get_safe("highavailability.process_configuration")
        aggregator_config = _get_aggregator_config(self.CFG)

        self.service_id, self.service_name = self._register_service()
        self.policy_event = Event()

        self.logprefix = "HA Agent (%s): " % self.service_name

        self.control = HAProcessControl(self.pds[0],
            self.container.resource_registry, self.service_id,
            self.policy_event.set, logprefix=self.logprefix)

        self.core = HighAvailabilityCore(cfg, self.control,
                self.pds, self.policy, process_definition_id=self.process_definition_id,
                parameters=policy_parameters,
                process_configuration=self.process_configuration,
                aggregator_config=aggregator_config, name=self.service_name)

        dashi_messaging = self.CFG.get_safe("highavailability.dashi_messaging", False)
        if dashi_messaging:

            dashi_name = self.CFG.get_safe("highavailability.dashi_name")
            if not dashi_name:
                raise Exception("dashi_name unknown")
            dashi_uri = self.CFG.get_safe("highavailability.dashi_uri")
            if not dashi_uri:
                rabbit_host = self.CFG.get_safe("server.amqp.host")
                rabbit_user = self.CFG.get_safe("server.amqp.username")
                rabbit_pass = self.CFG.get_safe("server.amqp.password")

                if not (rabbit_host and rabbit_user and rabbit_pass):
                    raise Exception("cannot form dashi URI")

                dashi_uri = "amqp://%s:%s@%s/" % (rabbit_user, rabbit_pass,
                                                  rabbit_host)
            dashi_exchange = self.CFG.get_safe("highavailability.dashi_exchange")
            if not dashi_exchange:
                dashi_exchange = get_sys_name()

            self.dashi_handler = HADashiHandler(self, dashi_name, dashi_uri, dashi_exchange)

        else:
            self.dashi_handler = None

    def _get_process_definition(self):
        process_definition_id = self.CFG.get_safe("highavailability.process_definition_id")
        process_definition_name = self.CFG.get_safe("highavailability.process_definition_name")

        if process_definition_id:
            pd_name = self.pds[0]
            pd = ProcessDispatcherServiceClient(to_name=pd_name)
            definition = pd.read_process_definition(process_definition_id)

        elif process_definition_name:
            definitions, _ = self.container.resource_registry.find_resources(
                restype="ProcessDefinition", name=process_definition_name)
            if len(definitions) == 0:
                raise Exception("Process definition with name '%s' not found" %
                    process_definition_name)
            elif len(definitions) > 1:
                raise Exception("multiple process definitions found with name '%s'" %
                    process_definition_name)
            definition = definitions[0]
            process_definition_id = definition._id

        else:
            raise Exception("HA Agent requires either process definition ID or name")

        return process_definition_id, definition

    def on_start(self):
        if self.dashi_handler:
            self.dashi_handler.start()

        self.control.start()

        # override the core's list of currently managed processes. This is to support
        # restart of an HAAgent.
        self.core.set_managed_upids(self.control.get_managed_upids())

        self.policy_thread = gevent.spawn(self._policy_thread_loop)

        # kickstart the policy once. future invocations will happen via event callbacks.
        self.policy_event.set()

    def on_quit(self):
        self.control.stop()
        self.policy_thread.kill(block=True, timeout=3)
        if self.dashi_handler:
            self.dashi_handler.stop()

        # DL: do we ever want to remove this object?
        #self._unregister_service()

    def _register_service(self):

        definition = self.process_definition
        existing_services, _ = self.container.resource_registry.find_resources(
                restype="Service", name=definition.name)

        if len(existing_services) > 0:
            if len(existing_services) > 1:
                log.warning("There is more than one service object for %s. Using the first one" % definition.name)
            service_id = existing_services[0]._id
        else:
            svc_obj = Service(name=definition.name, exchange_name=definition.name)
            service_id, _ = self.container.resource_registry.create(svc_obj)

        svcdefs, _ = self.container.resource_registry.find_resources(
                restype="ServiceDefinition", name=definition.name)

        if svcdefs:
            try:
                self.container.resource_registry.create_association(
                        service_id, "hasServiceDefinition", svcdefs[0]._id)
            except BadRequest:
                log.warn("Failed to associate %s Service and ServiceDefinition. It probably exists.",
                    definition.name)
        else:
            log.error("Cannot find ServiceDefinition resource for %s",
                    definition.name)

        return service_id, definition.name

    def _unregister_service(self):
        if not self.service_id:
            log.error("No service id. Cannot unregister service")
            return

        self.container.resource_registry.delete(self.service_id, del_associations=True)

    def _policy_thread_loop(self):
        """Single thread runs policy loops, to prevent races
        """
        while True:
            # wait until our event is set, up to policy_interval seconds
            self.policy_event.wait(self.policy_interval)
            if self.policy_event.is_set():
                self.policy_event.clear()
                log.debug("%sapplying policy due to event", self.logprefix)
            else:

                # on a regular basis, we check for the current state of each process.
                # this is essentially a hedge against bugs in the HAAgent, or in the
                # ION events system that could prevent us from seeing state changes
                # of processes.
                log.debug("%sapplying policy due to timer. Reloading process cache first.",
                    self.logprefix)
                try:
                    self.control.reload_processes()
                except (Exception, gevent.Timeout) as e:
                    log.warn("%sFailed to reload processes from PD. Will retry later.",
                        self.logprefix, exc_info=True)

            try:
                self._apply_policy()
            except (Exception, gevent.Timeout) as e:
                log.warn("%sFailed to apply policy. Will retry later.",
                    self.logprefix, exc_info=True)

    def _apply_policy(self):

        self.core.apply_policy()

        try:
            new_service_state = _core_hastate_to_service_state(self.core.status())
            service = self.container.resource_registry.read(self.service_id)
            if service.state != new_service_state:
                service.state = new_service_state
                self.container.resource_registry.update(service)
        except Exception:
            log.warn("%sProblem when updating Service state", self.logprefix, exc_info=True)

    def rcmd_reconfigure_policy(self, new_policy):
        """Service operation: Change the parameters of the policy used for service

        @param new_policy: parameters of policy
        @return:
        """
        self.core.reconfigure_policy(new_policy)
        #trigger policy thread to wake up
        self.policy_event.set()

    def rcmd_status(self):
        """Service operation: Get the status of the HA Service

        @return: {PENDING, READY, STEADY, BROKEN}
        """
        return self.core.status()

    def rcmd_dump(self):
        dump = self.core.dump()
        dump['service_id'] = self.service_id
        return dump


class HADashiHandler(object):
    """Passthrough dashi handlers for agent commands

    Used for messaging from the launch plan.
    """
    def __init__(self, agent, dashi_name, dashi_uri, dashi_exchange):
        self.agent = agent
        self.CFG = agent.CFG
        self.dashi = self._get_dashi(dashi_name, dashi_uri, dashi_exchange, sysname=self.CFG.get_safe('dashi.sysname'))
        self.dashi.handle(self.status)
        self.dashi.handle(self.reconfigure_policy)

        self.consumer_thread = None

    def start(self):
        self.consumer_thread = gevent.spawn(self.dashi.consume)

    def stop(self):
        self.dashi.cancel()
        if self.consumer_thread:
            self.consumer_thread.join()
            self.consumer_thread = None

    def status(self):
        return self.agent.rcmd_status()

    def reconfigure_policy(self, new_policy):
        return self.agent.rcmd_reconfigure_policy(new_policy)

    def _get_dashi(self, *args, **kwargs):

        # broken out to ease testing when dashi is not present
        import dashi
        return dashi.DashiConnection(*args, **kwargs)


class HighAvailabilityAgentClient(object):

    def __init__(self, agent_client):
        self.client = agent_client

    def status(self):

        args = []
        cmd = AgentCommand(command='status', args=args)
        return self.client.execute(cmd)

    def reconfigure_policy(self, new_policy):

        args = [new_policy]
        cmd = AgentCommand(command='reconfigure_policy', args=args)
        return self.client.execute(cmd)

    def dump(self):

        args = []
        cmd = AgentCommand(command='dump', args=args)
        return self.client.execute(cmd)


class HAProcessControl(object):

    def __init__(self, pd_name, resource_registry, service_id, callback=None, logprefix=""):
        self.pd_name = pd_name
        self.resource_registry = resource_registry
        self.service_id = service_id
        self.callback = callback
        if callback and not callable(callback):
            raise ValueError("callback is not callable")
        self.logprefix = logprefix

        self.client = ProcessDispatcherServiceClient(to_name=pd_name)
        self.event_sub = EventSubscriber(event_type="ProcessLifecycleEvent",
            callback=self._event_callback, origin_type="DispatchedProcess",
            auto_delete=True)

        self.processes = {}

    def start(self):
        service = self.resource_registry.read(self.service_id)
        process_assocs = self.resource_registry.find_associations(service, "hasProcess")

        for process_assoc in process_assocs:
            process_id = process_assoc.o
            if process_id:
                try:
                    process = self.client.read_process(process_id)
                except NotFound:
                    log.debug("%sService was associated with process %s, which is unknown to PD. ignoring.",
                        self.logprefix, process_id)
                    continue

                state = process.process_state
                state_str = ProcessStateEnum._str_map.get(state, str(state))

                self.processes[process.process_id] = _process_dict_from_object(process)
                log.info("%srecovered process %s state=%s", self.logprefix, process_id, state_str)
        self.event_sub.start()

    def stop(self):
        self.event_sub.stop()

    def get_managed_upids(self):
        return self.processes.keys()

    def _event_callback(self, event, *args, **kwargs):
        if not event:
            return

        try:
            self._inner_event_callback(event)
        except Exception:
            log.exception("%sException in event handler. This is a bug!", self.logprefix)

    def _inner_event_callback(self, event):
        process_id = event.origin
        state = event.state
        state_str = ProcessStateEnum._str_map.get(state, str(state))
        if not (process_id and process_id in self.processes):
            # we receive events for all processes but ignore most
            return

        process = None
        for _ in range(3):
            try:
                process = self.client.read_process(process_id)
                break
            except Timeout:
                log.warn("Timeout trying to read process from Process Dispatcher!", exc_info=True)
                pass  # retry
            except NotFound:
                break

        if process:
            log.info("%sreceived process %s state=%s", self.logprefix, process_id, state_str)

            # replace the cached data about this process
            self.processes[process_id] = _process_dict_from_object(process)

        else:
            log.warn("%sReceived process %s event but failed to read from Process Dispatcher",
                self.logprefix, process_id)

            #XXX better approach here? we at least have the state from the event,
            # so sticking that on cached process. We could miss other important
            # data like hostname however.
            self.processes[process_id]['state'] = process_state_to_pd_core(state)

        if self.callback:
            try:
                self.callback()
            except Exception, e:
                log.warn("%sError in HAAgent callback: %s", self.logprefix, e, exc_info=True)

    def _associate_process(self, process):
        try:
            self.resource_registry.create_association(self.service_id,
                "hasProcess", process.process_id)
        except Exception:
            log.exception("Couldn't associate service %s to process %s" % (self.service_id, process.process_id))

    def schedule_process(self, pd_name, process_definition_id, **kwargs):

        if pd_name != self.pd_name:
            raise Exception("schedule_process request received for unknown PD: %s" % pd_name)

        # figure out if there is an existing PID which can be reused
        found_upid = None
        for process in self.processes.values():
            upid = process.get('upid')
            state = process.get('state')
            if not (upid and state):
                continue

            if state in CoreProcessState.TERMINAL_STATES:
                found_upid = upid

        if found_upid:
            upid = found_upid
            proc = self.client.read_process(upid)

        else:
            # otherwise create a new process and associate
            upid = self.client.create_process(process_definition_id)

            # note: if the HAAgent fails between the create call above and the
            # associate call below, there may be orphaned Process objects. These
            # processes will not however be running, so are largely harmless.
            proc = self.client.read_process(upid)
            self._associate_process(proc)

        process_schedule = _get_process_schedule(**kwargs)
        configuration = kwargs.get('configuration')

        # cheat and roll the process state to REQUESTED before we actually
        # schedule it. this is in-memory only, so should be harmless. This
        # avoids a race between this scheduling process and the event
        # subscriber.
        proc.process_state = ProcessStateEnum.REQUESTED
        self.processes[upid] = _process_dict_from_object(proc)

        self.client.schedule_process(process_definition_id, process_schedule,
            configuration=configuration, process_id=upid)
        return upid

    def terminate_process(self, pid):
        return self.client.cancel_process(pid)

    def get_all_processes(self):
        processes = deepcopy(self.processes.values())
        return {self.pd_name: processes}

    def reload_processes(self):
        for process_id, process_dict in self.processes.items():
            try:
                process = self.client.read_process(process_id)
            except Exception:
                log.warn("%sFailed to read process %s from PD. Will retry later.",
                    self.logprefix, process_id, exc_info=True)
                continue
            new_process_dict = _process_dict_from_object(process)
            if new_process_dict['state'] != process_dict['state']:
                log.warn("%sUpdating process %s record manually. we may have missed an event?",
                    self.logprefix, process_id)
                self.processes[process_id] = new_process_dict


def _process_dict_from_object(process):
    state = process_state_to_pd_core(process.process_state)
    dict_proc = {'upid': process.process_id, 'state': state}
    return dict_proc


def _core_hastate_to_service_state(core):
    return ServiceStateEnum._value_map.get(core)


def _get_aggregator_config(config):
    trafficsentinel = config.get_safe("server.trafficsentinel")
    if trafficsentinel:
        host = trafficsentinel.get('host')
        username = trafficsentinel.get('username')
        password = trafficsentinel.get('password')
        protocol = trafficsentinel.get('protocol')
        port = trafficsentinel.get('port')

        if host and username and password:
            aggregator_config = dict(type='trafficsentinel', host=host,
                username=username, password=password)
            if protocol:
                aggregator_config['protocol'] = protocol
            if port:
                aggregator_config['port'] = port
            return aggregator_config

    # return None if no config. policy will error out if it needs aggregator.
    return None


def _get_process_schedule(**kwargs):
    queueing_mode = kwargs.get('queueing_mode')
    restart_mode = kwargs.get('restart_mode')
    execution_engine_id = kwargs.get('execution_engine_id')
    node_exclusive = kwargs.get('node_exclusive')
    constraints = kwargs.get('constraints')

    process_schedule = ProcessSchedule()
    if queueing_mode is not None:
        try:
            process_schedule.queueing_mode = ProcessQueueingMode._value_map[queueing_mode]
        except KeyError:
            msg = "%s is not a known ProcessQueueingMode" % (queueing_mode)
            raise BadRequest(msg)

    if restart_mode is not None:
        try:
            process_schedule.restart_mode = ProcessRestartMode._value_map[restart_mode]
        except KeyError:
            msg = "%s is not a known ProcessRestartMode" % (restart_mode)
            raise BadRequest(msg)
    else:
        # if restart mode isn't specified, use NEVER. HA Agent itself will reschedule failures.
        process_schedule.restart_mode = ProcessRestartMode.NEVER

    target = ProcessTarget()
    if execution_engine_id is not None:
        target.execution_engine_id = execution_engine_id
    if node_exclusive is not None:
        target.node_exclusive = node_exclusive
    if constraints is not None:
        target.constraints = constraints

    process_schedule.target = target
    return process_schedule
