#!/usr/bin/env python

__author__ = 'Michael Meisinger'

from pyon.util.log import log
from pyon.net.endpoint import ProcessRPCClient
from pyon.util.containers import DotDict

from interface.services.ichats_service import BaseChatsService
from interface.services.ichatc_service import IChatcService

class ChatServerService(BaseChatsService):
    def on_init(self):
        print "INIT CHAT SERVER"
        self.clients = DotDict()

    def register(self, user_name='', proc_id=''):
        print "Registering user %s, client %s" % (user_name, proc_id)
        client = ProcessRPCClient(node=self.container.node, name=proc_id, iface=IChatcService, process=self)
        self.clients[user_name] = DotDict(procid=proc_id, user_name=user_name, client=client)
        return "OK"

    def unregister(self, user_name=''):
        log.debug("Unregistering client %s" % proc_id)
        del self.clients[user_name]
        return "OK"

    def message(self, from_name, to_name, text=''):
        if to_name == "all":
            for cl in self.clients.values():
                cl['client'].message(from_name, text)
        else:
            client = self.clients.get(to_name, None)
            if client:
                client.client.message(from_name, text)
            else:
                return "USER NOT FOUND"
        return "OK"
