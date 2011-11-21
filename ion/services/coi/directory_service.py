#!/usr/bin/env python

__author__ = 'Thomas R. Lennan'
__license__ = 'Apache 2.0'

from pyon.directory.directory import Directory
from interface.services.coi.idirectory_service import BaseDirectoryService

class DirectoryService(BaseDirectoryService):

    def on_init(self):
        self.directory = Directory()

    def register(self, parent='/', key='', attributes={}):
        return self.directory.register(parent, key, **attributes)

    def unregister(self, parent='/', key=''):
        return self.directory.unregister(parent, key)

    def read(self, qualified_key='/'):
        return self.directory.read(qualified_key)

