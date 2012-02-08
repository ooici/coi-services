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

import yaml
import os
import os.path

from pyon.datastore.datastore import DataStore
from pyon.public import CFG, IonObject, log, sys_name, RT, LCS, PRED, StreamProcess, Container


class DatastoreLoader(StreamProcess):
    def on_init(self, *args, **kwargs):
        pass

    def on_start(self, *args, **kwargs):
        pass

    def on_quit(self, *args, **kwargs):
        pass

def dump_datastore(ds_name, path=None, clear_dir=True):
    if CFG.system.mockdb:
        log.warn("Cannot dump from MockDB")
        return
    ds = Container.instance.datastore_manager.get_datastore(ds_name)
    if not ds:
        return
    outpath_base = "res/preload/local/dump"
    outpath = "%s/%s" % (outpath_base, ds_name)
    if not os.path.exists(outpath):
        os.makedirs(outpath)
    [os.remove(os.path.join(outpath, f)) for f in os.listdir(outpath)]

    objs = ds.find_by_view("_all_docs", None, id_only=False, convert_doc=False)
    numwrites = 0
    for obj_id, obj_key, obj in objs:
        if obj_id.startswith("_design"):
            continue
        with open("%s/%s.yml" % (outpath, obj_id), 'w') as f:
            yaml.dump(obj, f, default_flow_style=False)
            numwrites += 1
    log.info("Wrote %s objects to %s" % (numwrites, outpath))
