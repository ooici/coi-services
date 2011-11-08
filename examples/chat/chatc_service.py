#!/usr/bin/env python

__author__ = 'Michael Meisinger'

from pyon.net.endpoint import ProcessRPCClient
from pyon.public import CFG
from pyon.util.log import log
from pyon.util.containers import DotDict

from interface.services.ichatc_service import BaseChatcService

def get_username():
    import os
    import pwd
    return pwd.getpwuid( os.getuid() )[ 0 ]

class ChatClientService(BaseChatcService):
    def on_init(self):

        self.username = get_username()
        try:
            self.username = CFG.chat.user_name
        except Exception, ex:
            pass
        print "INIT CHAT CLIENT: ", self.clients.chats.register(self.username, self.id)

    def ping(self, from_name=''):
        print "PING from %s" % (from_name)
        return "OK"

    def message(self, from_name='', text=''):
        print "MESSAGE from %s: %s" % (from_name, text)
        try:
            import os, string
            text = string.replace(text,'"','')
            os.popen('say "%s"' % text)
        except Exception, ex:
            pass
        return "OK"
