#!/usr/bin/env python

__author__ = 'Thomas R. Lennan'
__license__ = 'Apache 2.0'

from pyon.directory.directory import Directory
from interface.services.coi.idirectory_service import BaseDirectoryService

class DirectoryService(BaseDirectoryService):

    """
    Provides a directory of services and other resources specific to an Org.
    """

    def on_init(self):
        self.directory = Directory()
        # For easier interactive debugging
        self.dss = None
        self.ds = self.directory.datastore
        try:
            self.dss = self.directory.datastore.server[self.directory.datastore.datastore_name]
        except Exception:
            pass

    def register(self, parent='/', key='', attributes={}):
        return self.directory.register(parent, key, **attributes)

    def unregister(self, parent='/', key=''):
        return self.directory.unregister(parent, key)

    def lookup(self, qualified_key=''):
        return self.directory.lookup(qualified_key)

    def find(self, parent='/', pattern=''):
        raise NotImplementedError()