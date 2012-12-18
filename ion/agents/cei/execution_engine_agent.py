import datetime
import logging

from pyon.agent.simple_agent import SimpleResourceAgent
from pyon.core.exception import Unauthorized, NotFound
from pyon.core import bootstrap
from pyon.public import IonObject, log
from pyon.util.containers import get_safe
from pyon.net.endpoint import Publisher

from interface.objects import AgentCommand
from ion.agents.cei.util import looping_call

try:
    from eeagent.core import EEAgentCore
    from eeagent.beatit import make_beat_msg
    from eeagent.execute import get_exe_factory
    from eeagent.eeagent_exceptions import EEAgentUnauthorizedException
    from pidantic.pidantic_exceptions import PIDanticExecutionException
except ImportError:
    EEAgentCore = None

"""
@package ion.agents.cei.execution_engine_agent
@file ion/agents/cei/execution_engine_agent.py
@author Patrick Armstrong
@brief Pyon port of EEAgent
 """

DEFAULT_HEARTBEAT = 5


class ExecutionEngineAgent(SimpleResourceAgent):
    """Agent to manage processes on a worker

    """

    def __init__(self):
        log.debug("ExecutionEngineAgent init")
        SimpleResourceAgent.__init__(self)

    def on_init(self):
        if not EEAgentCore:
            msg = "EEAgentCore isn't available. Use autolaunch.cfg buildout"
            log.error(msg)
            self.heartbeat_thread = None
            return
        log.debug("ExecutionEngineAgent Pyon on_init")
        launch_type_name = self.CFG.eeagent.launch_type.name

        if not launch_type_name:
            # TODO: Fail fast here?
            log.error("No launch_type.name specified")

        self._factory = get_exe_factory(launch_type_name, self.CFG,
            pyon_container=self.container, log=log)

        # TODO: Allow other core class?
        self.core = EEAgentCore(self.CFG, self._factory, log)

        interval = self.CFG.eeagent.get('heartbeat', DEFAULT_HEARTBEAT)
        if interval > 0:
            self.heartbeater = HeartBeater(self.CFG, self._factory, self.resource_id, log=log)
            self.heartbeater.poll()
            self.heartbeat_thread = looping_call(0.1, self.heartbeater.poll)
        else:
            self.heartbeat_thread = None

    def on_quit(self):
        if self.heartbeat_thread is not None:
            self.heartbeat_thread.kill()
        self._factory.terminate()

    def rcmd_launch_process(self, u_pid, round, run_type, parameters):
        try:
            self.core.launch_process(u_pid, round, run_type, parameters)
        except EEAgentUnauthorizedException, e:
            raise Unauthorized(e.message)
        except PIDanticExecutionException, e:
            raise NotFound(e.message)

    def rcmd_terminate_process(self, u_pid, round):
        self.core.terminate_process(u_pid, round)

    def rcmd_restart_process(self, u_pid, round):
        self.core.restart_process(u_pid, round)

    def rcmd_cleanup_process(self, u_pid, round):
        self.core.cleanup(u_pid, round)

    def rcmd_dump_state(self):
        return make_beat_msg(self.core._process_managers_map, self.CFG)


class HeartBeater(object):
    def __init__(self, CFG, factory, process_id, log=logging):

        self._log = log
        self._log.log(logging.DEBUG, "Starting the heartbeat thread")
        self._CFG = CFG
        self._res = None
        self._interval = int(CFG.eeagent.heartbeat)
        self._res = None
        self._done = False
        self._factory = factory
        self.process_id = process_id
        self._publisher = Publisher()
        self._pd_name = CFG.eeagent.get('heartbeat_queue', 'heartbeat_queue')

        self._factory.set_state_change_callback(self._state_change_callback, None)
        self._first_beat()

    def _first_beat(self):
        self._beat_time = datetime.datetime.now()

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
            beat = make_beat_msg(self._factory, self._CFG)
            message = dict(beat=beat, eeagent_id=self.process_id, resource_id=self._CFG.agent.resource_id)
            to_name = self._pd_name
            self._log.debug("Send heartbeat: %s to %s", message, self._pd_name)
            self._publisher.publish(message, to_name=to_name)
        except Exception:
            self._log.exception("beat failed")


class ExecutionEngineAgentClient(object):

    def __init__(self, agent_client):
        self.client = agent_client

    def launch_process(self, u_pid, round, run_type, parameters):

        args = [u_pid, round, run_type, parameters]
        cmd = AgentCommand(command='launch_process', args=args)
        return self.client.execute(cmd)

    def terminate_process(self, u_pid, round):

        args = [u_pid, round]
        cmd = AgentCommand(command='terminate_process', args=args)
        return self.client.execute(cmd)

    def restart_process(self, u_pid, round):

        args = [u_pid, round]
        cmd = AgentCommand(command='restart_process', args=args)
        return self.client.execute(cmd)

    def cleanup_process(self, u_pid, round):
        args = [u_pid, round]
        cmd = AgentCommand(command='cleanup_process', args=args)
        return self.client.execute(cmd)

    def dump_state(self):
        cmd = AgentCommand(command='dump_state', args=[])
        return self.client.execute(cmd)
