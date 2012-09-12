import datetime
import logging

from pyon.agent.simple_agent import SimpleResourceAgent
from pyon.public import IonObject, log
from pyon.util.containers import get_safe
from pyon.net.endpoint import Publisher
from pyon.event.event import EventPublisher

from interface.objects import AgentCommand, ProcessDefinition, ProcessSchedule, ProcessStateEnum
from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceClient
from ion.agents.cei.util import looping_call

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
        pds = self.CFG.get_safe("highavailability.process_dispatchers", [])
        process_spec = self.CFG.get_safe("highavailability.process_spec")
        # TODO: Allow other core class?
        self.core = HighAvailabilityCore(cfg, ProcessDispatcherSimpleAPIClient,
                pds, process_spec, self.policy)

        self.policy_thread = looping_call(self.policy_interval, self.core.apply_policy)

    def on_quit(self):
        self.policy_thread.kill(block=True, timeout=3)

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
        ProcessStateEnum.SPAWN: '500-RUNNING',
        ProcessStateEnum.TERMINATE: '700-TERMINATED',
        ProcessStateEnum.ERROR: '850-FAILED'
    }

    def __init__(self, name, **kwargs):
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

    def schedule_process(self, upid, definition_id, configuration=None,
            subscribers=None, constraints=None, queueing_mode=None,
            restart_mode=None, execution_engine_id=None, node_exclusive=None):

        definition = self.real_client.read_process_definition(definition_id)
        self.event_pub.publish_event(event_type="ProcessLifecycleEvent",
            origin=definition.name, origin_type="DispatchedHAProcess",
            state=ProcessStateEnum.SPAWN)

        pid = self.real_client.create_process(definition_id)

        process_schedule = ProcessSchedule()

        sched_pid = self.real_client.schedule_process(definition_id,
                process_schedule, configuration={}, process_id=pid)

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
