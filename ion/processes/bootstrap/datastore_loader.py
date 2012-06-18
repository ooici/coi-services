#!/usr/bin/env python

"""Process that loads the datastore"""

__author__ = 'Michael Meisinger, Thomas Lennan'

"""
Possible Features
- load objects into different datastores
- load from a directory of YML files in ion-definitions
- load from a ZIP of YMLs
- load an additional directory (not under GIT control)
- change timestamp for resources
- load a subset of objects by type, etc
"""

from pyon.public import CFG, log, ImmediateProcess, iex
from pyon.datastore import datastore_admin
from pyon.core import bootstrap
from pyon.core.bootstrap import get_sys_name

class DatastoreAdmin(ImmediateProcess):
    """
    bin/pycc -x ion.processes.bootstrap.datastore_loader.DatastoreLoader op=clear prefix=ion
    """
    def on_init(self):
        pass

    def on_start(self):
        # print env temporarily to debug cei
        import os
        log.info('ENV vars: %s' % str(os.environ))
        op = self.CFG.get("op", None)
        datastore = self.CFG.get("datastore", None)
        path = self.CFG.get("path", None)
        prefix = self.CFG.get("prefix", get_sys_name()).lower()
        log.info("DatastoreLoader: {op=%s, datastore=%s, path=%s, prefix=%s}" % (op, datastore, path, prefix))

        self.da = datastore_admin.DatastoreAdmin()

        if op:
            if op == "load":
                self.da.load_datastore(path, datastore, ignore_errors=False)
            elif op == "dump":
                self.da.dump_datastore(path, datastore)
            elif op == "blame":
                # TODO make generic
                self.da.get_blame_objects()
            elif op == "clear":
                self.da.clear_datastore(datastore, prefix)
            else:
                raise iex.BadRequest("Operation unknown")
        else:
            raise iex.BadRequest("No operation specified")

    def on_quit(self):
        pass

DatastoreLoader = DatastoreAdmin
