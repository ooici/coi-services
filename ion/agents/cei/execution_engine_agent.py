import datetime
import logging

from pyon.agent.agent import ResourceAgent
from pyon.public import IonObject, log
from pyon.util.containers import get_safe

from interface.objects import AgentCommand
from ion.agents.cei.util import looping_call


from eeagent.core import EEAgentCore
from eeagent.beatit import make_beat_msg
from eeagent.execute import get_exe_factory


"""
@package ion.agents.cei.execution_engine_agent
@file ion/agents/cei/execution_engine_agent.py
@author Patrick Armstrong
@brief Pyon port of EEAgent
 """

class ExecutionEngineAgent(ResourceAgent):
    """Agent to manage processes on a worker

    """

    def __init__(self):
        log.debug("ExecutionEngineAgent init")
        ResourceAgent.__init__(self)


    def on_init(self):
        log.debug("ExecutionEngineAgent Pyon on_init")
        launch_type_name = get_safe(self.CFG, "eeagent.launch_type.name")

        if not launch_type_name:
            # TODO: Fail fast here?
            log.error("No launch_type.name specified")

        self._factory = get_exe_factory(launch_type_name, self.CFG,
            pyon_container=self.container, log=log)

        # TODO: Allow other core class?
        self.core = EEAgentCore(self.CFG, self._factory, log)

        #self.heartbeater = HeartBeater(self.CFG, self._factory, log=log)
        #interval = 0.5 #TODO get from CFG
        #looping_call(interval, self.heartbeater.poll)

    def on_quit(self):
        self._factory.terminate()

    def acmd_launch_process(self, u_pid, round, run_type, parameters):
        self.core.launch_process(u_pid, round, run_type, parameters)

    def acmd_terminate_process(self, u_pid, round):
        self.core.terminate_process(u_pid, round)

    def acmd_restart_process(self, u_pid, round):
        self.core.restart_process(u_pid, round)

    def acmd_cleanup(self, u_pid, round):
        self.core.cleanup(u_pid, round)

    def acmd_dump_state(self):
        return make_beat_msg(self.core._process_managers_map)

class HeartBeater(object):
    def __init__(self, CFG, factory, log=logging):

        self._log = log
        self._log.log(logging.DEBUG, "Starting the heartbeat thread")
        self._CFG = CFG
        self._res = None
        self._interval = int(CFG.eeagent.heartbeat)
        self._res = None
        self._done = False
        self._factory = factory
        self._next_beat(datetime.datetime.now())

        self._factory.set_state_change_callback(self._state_change_callback, None)

    def _next_beat(self, now):
        self._beat_time = now + datetime.timedelta(seconds=self._interval)

    def _state_change_callback(self, user_arg):
        # on state change set the beat time to now
        self._beat_time = datetime.datetime.now()

    def poll(self):

        now = datetime.datetime.now()
        if now > self._beat_time:
            self._next_beat(now)
            self.beat()

    def beat(self):
        try:
            message =  make_beat_msg(self._factory)
            self._log.debug("Send heartbeat: %s" % message)
        except:
            self._log.exception("beat failed")


class ExecutionEngineAgentClient(object):

    def __init__(self, agent_client):
        self.client = agent_client

    def launch_process(self, u_pid, round, run_type, parameters):

        args = [u_pid, round, run_type, parameters]
        cmd = AgentCommand(command='launch_process', args=args)
        return self.client.execute_agent(cmd)

    def terminate_process(self, u_pid, round):

        args = [u_pid, round]
        cmd = AgentCommand(command='terminate_process', args=args)
        return self.client.execute_agent(cmd)

    def restart_process(self, u_pid, round):

        args = [u_pid, round]
        cmd = AgentCommand(command='restart_process', args=args)
        return self.client.execute_agent(cmd)

    def dump_state(self):
        cmd = AgentCommand(command='dump_state', args=[])
        return self.client.execute_agent(cmd)
