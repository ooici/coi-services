#!/usr/bin/env python

"""Example agents"""

__author__ = 'Michael Meisinger'
__license__ = 'Apache 2.0'

from pyon.agent.agent import ResourceAgent, UserAgent

class OrgAgent(ResourceAgent):
    pass

class ResAgentOne(ResourceAgent):
    apar_debug = True
    rpar_tone = "friendly"

    def rcmd_say(self, what, *args, **kwargs):
        print "We say:", what
        return ('hey', 1)

    def acmd_log(self, *args, **kwargs):
        pass

class ResAgentTwo(ResourceAgent):
    def rcmd_shout(self, *args, **kwargs):
        pass

class UserAgentOne(UserAgent):

    def on_start(self):
        pass

class UserAgentTwo(UserAgent):
    pass
