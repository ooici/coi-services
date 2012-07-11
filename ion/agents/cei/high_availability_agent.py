import datetime
import logging

from pyon.agent.agent import ResourceAgent
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


class HighAvailabilityAgent(ResourceAgent):
    """Agent to manage high availability processes

    """

    def __init__(self):
        log.debug("HighAvailabilityAgent init")
        ResourceAgent.__init__(self)

    def on_init(self):
        if not HighAvailabilityCore:
            msg = "HighAvailabilityCore isn't available. Use production.cfg buildout"
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

    state_map = {
        ProcessStateEnum.SPAWN: '500-RUNNING',
        ProcessStateEnum.TERMINATE: '700-TERMINATED',
        ProcessStateEnum.ERROR: '850-FAILED'
    }

    def __init__(self, name, **kwargs):
        self.real_client = ProcessDispatcherServiceClient(to_name=name, **kwargs)
        self.event_pub = EventPublisher()

    def dispatch_process(self, upid, spec, subscribers, constraints=None,
                         immediate=False):

        name = spec.get('name')
        self.event_pub.publish_event(event_type="ProcessLifecycleEvent",
            origin=name, origin_type="DispatchedHAProcess",
            state=ProcessStateEnum.SPAWN)
        process_def = ProcessDefinition(name=name)
        process_def.executable = {'module': spec.get('module'),
                'class': spec.get('class')}

        process_def_id = self.real_client.create_process_definition(process_def)

        pid = self.real_client.create_process(process_def_id)

        process_schedule = ProcessSchedule()

        sched_pid = self.real_client.schedule_process(process_def_id,
                process_schedule, configuration={}, process_id=pid)

        return sched_pid

    def terminate_process(self, pid):
        return self.real_client.cancel_process(pid)

    def describe_processes(self):
        procs = self.real_client.list_processes()
        dict_procs = []
        for proc in procs:
            dict_proc = {'upid': proc.process_id,
                    'state': self.state_map[proc.process_state],
                    }
            dict_procs.append(dict_proc)

        return dict_procs
