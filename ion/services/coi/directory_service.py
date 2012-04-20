#!/usr/bin/env python

__author__ = 'Thomas R. Lennan, Michael Meisinger'
__license__ = 'Apache 2.0'

from pyon.core.exception import BadRequest
from pyon.ion.directory import Directory

from interface.services.coi.idirectory_service import BaseDirectoryService


class DirectoryService(BaseDirectoryService):
    """
    Provides a directory of services and other resources specific to an Org.
    The directory is backed by a persistent datastore and is system/Org wide.
    """

    def on_init(self):
        self.directory = self.container.directory

        # For easier interactive debugging
        self.dss = None
        self.ds = self.directory.dir_store
        try:
            self.dss = self.directory.dir_store.server[self.directory.dir_store.datastore_name]
        except Exception:
            pass

    def register(self, parent='/', key='', attributes={}):
        return self.directory.register(parent, key, **attributes)

    def unregister(self, parent='/', key=''):
        return self.directory.unregister(parent, key)

    def lookup(self, qualified_key=''):
        return self.directory.lookup(qualified_key)

    def find(self, parent='/', pattern=''):
        raise BadRequest("Not Implemented")
