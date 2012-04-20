#!/usr/bin/env python

"""Example agents"""

__author__ = 'Michael Meisinger'
__license__ = 'Apache 2.0'

import threading
import time

from pyon.agent.agent import ResourceAgent, UserAgent, ResourceAgentClient
from pyon.ion.process import StandaloneProcess
from pyon.public import log

from interface.objects import AgentCommand, Org, InstrumentDevice, UserIdentity

# For this example only. Proc_name -> proc id
agent_instances = {}
# For this example only. Proc_id -> resource id
agent_resources = {}

class OrgAgent(ResourceAgent):
    def on_init(self):
        res_obj = Org(name="org1")
        rid,_ = self.clients.resource_registry.create(res_obj)
        agent_resources[self.id] = rid
        self.resource_id = rid
        agent_instances[self._proc_name] = self.id

class ResAgentOne(ResourceAgent):
    apar_debug = True
    rpar_tone = "friendly"

    def on_init(self):
        res_obj = InstrumentDevice(name="resource1")
        rid,_ = self.clients.resource_registry.create(res_obj)
        agent_resources[self.id] = rid
        self.resource_id = rid
        agent_instances[self._proc_name] = self.id

    def rcmd_say(self, what, *args, **kwargs):
        log.info("We say: " + str(what))
        return ('hey', 1)

    def acmd_log(self, *args, **kwargs):
        pass

class ResAgentTwo(ResourceAgent):
    def on_init(self):
        res_obj = InstrumentDevice(name="resource2")
        rid,_ = self.clients.resource_registry.create(res_obj)
        agent_resources[self.id] = rid
        self.resource_id = rid
        agent_instances[self._proc_name] = self.id

    def rcmd_shout(self, *args, **kwargs):
        pass

class UserAgentOne(UserAgent):
    def on_init(self):
        res_obj = UserIdentity(name="user1")
        rid,_ = self.clients.resource_registry.create(res_obj)
        agent_resources[self.id] = rid
        self.resource_id = rid
        agent_instances[self._proc_name] = self.id

    def on_start(self):
        pass

    def rcmd_makesay(self, who, what, *args, **kwargs):
        log.info("Makesay: Relaying command %s to %s" % (what, who))
        target_name = agent_instances.get(str(who), None)
        if target_name:
            self.rac = ResourceAgentClient(resource_id='R', name=target_name, process=self)

            cmd = AgentCommand(command='say', args=[what])
            res = self.rac.execute(cmd)
            return "OK"
        else:
            return "UNKNOWN AGENT"

class UserAgentTwo(UserAgent):
    def on_init(self):
        res_obj = UserIdentity(name="user1")
        rid,_ = self.clients.resource_registry.create(res_obj)
        agent_resources[self.id] = rid
        self.resource_id = rid
        agent_instances[self._proc_name] = self.id

class TriggerProcess(StandaloneProcess):

    def on_start(self):
        log.info("Known agents: "+ str(agent_instances))
        target_name = agent_instances['user_agent_1']
        resource_id = agent_resources[target_name]

        self.rac = ResourceAgentClient(resource_id=resource_id, process=self)

        self.trigger_func = threading.Thread(target=self._trigger_func)
        self.trigger_func.start()

    def _trigger_func(self):
        time.sleep(1)

        cmd = AgentCommand(command='makesay', args=['res_agent_1', 'HI'])
        res = self.rac.execute(cmd)
