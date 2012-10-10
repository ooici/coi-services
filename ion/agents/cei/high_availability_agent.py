import gevent

from pyon.agent.simple_agent import SimpleResourceAgent
from pyon.event.event import EventPublisher
from pyon.public import log, get_sys_name

from interface.objects import AgentCommand, ProcessDefinition, ProcessSchedule, ProcessStateEnum
from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceClient
from ion.agents.cei.util import looping_call
from ion.services.cei.process_dispatcher_service import _core_process_definition_from_ion, \
    ProcessDispatcherService

try:
    from epu.highavailability.core import HighAvailabilityCore
    import epu.highavailability.policy as policy
except ImportError:
    HighAvailabilityCore = None
    #raise


"""
@package ion.agents.cei.high_availability_agent
@file ion/agents/cei/exehigh_availability_agent
@author Patrick Armstrong
@brief Pyon port of HAAgent
 """

DEFAULT_INTERVAL = 5


class HighAvailabilityAgent(SimpleResourceAgent):
    """Agent to manage high availability processes

    """

    def __init__(self):
        log.debug("HighAvailabilityAgent init")
        SimpleResourceAgent.__init__(self)
        self.dashi_handler = None

    def on_init(self):
        if not HighAvailabilityCore:
            msg = "HighAvailabilityCore isn't available. Use autolaunch.cfg buildout"
            log.error(msg)
            return
        log.debug("HighAvailabilityCore Pyon on_init")

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
        pds = self.CFG.get_safe("highavailability.process_dispatchers",
            [ProcessDispatcherService.name])

        process_definition_id = self.CFG.get_safe("highavailability.process_definition_id")
        process_configuration = self.CFG.get_safe("highavailability.process_configuration")
        aggregator_config = self.CFG.get_safe("highavailability.aggregator")
        # TODO: Allow other core class?
        self.core = HighAvailabilityCore(cfg, ProcessDispatcherSimpleAPIClient,
                pds, self.policy, process_definition_id=process_definition_id,
                parameters=policy_parameters,
                process_configuration=process_configuration,
                aggregator_config=aggregator_config)

        self.policy_thread = looping_call(self.policy_interval, self.core.apply_policy)

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

    def on_start(self):
        if self.dashi_handler:
            self.dashi_handler.start()

    def on_quit(self):
        self.policy_thread.kill(block=True, timeout=3)
        if self.dashi_handler:
            self.dashi_handler.stop()

    def rcmd_reconfigure_policy(self, new_policy):
        """Service operation: Change the parameters of the policy used for service

        @param new_policy: parameters of policy
        @return:
        """
        self.core.reconfigure_policy(new_policy)

    def rcmd_status(self):
        """Service operation: Get the status of the HA Service

        @return: {PENDING, READY, STEADY, BROKEN}
        """
        return self.core.status()

    def rcmd_dump(self):
        return self.core.dump()


class HADashiHandler(object):
    """Passthrough dashi handlers for agent commands

    Used for messaging from the launch plan.
    """
    def __init__(self, agent, dashi_name, dashi_uri, dashi_exchange):
        self.agent = agent

        self.dashi = self._get_dashi(dashi_name, dashi_uri, dashi_exchange)
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


class ProcessDispatcherSimpleAPIClient(object):

    # State to use when state returned from PD is None
    unknown_state = "400-PENDING"

    state_map = {
        ProcessStateEnum.RUNNING: '500-RUNNING',
        ProcessStateEnum.TERMINATED: '700-TERMINATED',
        ProcessStateEnum.FAILED: '850-FAILED'
    }

    def __init__(self, name, real_client=None, **kwargs):
        if real_client is not None:
            self.real_client = real_client
        else:
            self.real_client = ProcessDispatcherServiceClient(to_name=name, **kwargs)
        self.event_pub = EventPublisher()

    def create_definition(self, definition_id, definition_type, executable,
                          name=None, description=None):

        # note: we lose the description
        definition = ProcessDefinition(name=name)
        definition.executable = {'module': executable.get('module'),
                'class': executable.get('class')}
        definition.definition_type = definition_type
        return self.real_client.create_process_definition(definition, definition_id)

    def describe_definition(self, definition_id):

        definition = self.real_client.read_process_definition(definition_id)
        core_defintion = _core_process_definition_from_ion(definition)
        return core_defintion

    def schedule_process(self, upid, definition_id, configuration=None,
            subscribers=None, constraints=None, queueing_mode=None,
            restart_mode=None, execution_engine_id=None, node_exclusive=None):

        definition = self.real_client.read_process_definition(definition_id)
        self.event_pub.publish_event(event_type="ProcessLifecycleEvent",
            origin=definition.name, origin_type="DispatchedHAProcess",
            state=ProcessStateEnum.RUNNING)

        pid = self.real_client.create_process(definition_id)

        process_schedule = ProcessSchedule()

        sched_pid = self.real_client.schedule_process(definition_id,
                process_schedule, configuration=configuration, process_id=upid)

        proc = self.real_client.read_process(sched_pid)
        dict_proc = {'upid': proc.process_id,
                'state': self.state_map.get(proc.process_state, self.unknown_state),
                }
        return dict_proc

    def terminate_process(self, pid):
        return self.real_client.cancel_process(pid)

    def describe_processes(self):
        procs = self.real_client.list_processes()
        dict_procs = []
        for proc in procs:
            dict_proc = {'upid': proc.process_id,
                    'state': self.state_map.get(proc.process_state, self.unknown_state),
                    }
            dict_procs.append(dict_proc)
        return dict_procs
