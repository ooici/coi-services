#!/usr/bin/env python

"""Process that loads the datastore"""

__author__ = 'Michael Meisinger'

"""
Features
- load objects into different datastores
- load from a directory of YML files in ion-definitions
- load from a ZIP of YMLs
- load an additional directory (not under GIT control)
- change timestamp for resources
- load a subset of objects by type, etc
"""

from pyon.public import CFG, IonObject, log, sys_name, RT, LCS, PRED, StreamProcess

class DatastoreLoader(StreamProcess):
    def on_init(self, *args, **kwargs):
        pass

    def on_start(self, *args, **kwargs):
        pass

    def on_quit(self, *args, **kwargs):
        pass
