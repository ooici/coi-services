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
import datetime
import os
import os.path

from pyon.datastore.datastore import DataStore, DatastoreManager
from pyon.public import CFG, IonObject, log, sys_name, RT, LCS, PRED, StreamProcess


class DatastoreLoader(StreamProcess):
    def on_init(self, *args, **kwargs):
        pass

    def on_start(self, *args, **kwargs):
        pass

    def on_quit(self, *args, **kwargs):
        pass

    @classmethod
    def load_datastore(cls, ds_name, path=None):
        if CFG.system.mockdb:
            log.warn("Cannot load into MockDB")
            return
        ds = DatastoreManager.get_datastore(ds_name)
        if not ds:
            return
        path = path or "res/preload/default"


    @classmethod
    def dump_datastore(cls, ds_name=None, path=None, clear_dir=True):
        """
        Dumps CouchDB datastores into a directory as YML files.
        @param ds_name Logical name (such as "resources") of an ION datastore
        @param path Directory to put dumped datastores into (defaults to
                    "res/preload/local/dump_[timestamp]")
        @param clear_dir if True, delete contents of datastore dump dirs
        """
        if CFG.system.mockdb:
            log.warn("Cannot dump from MockDB")
            return
        if not path:
            dtstr = datetime.datetime.today().strftime('%Y%m%d_%H%M%S')
            path = "res/preload/local/dump_%s" % dtstr
        if ds_name:
            # 1 Check ds_name exists
            ds = DatastoreManager.get_datastore("objects")
            if ds.datastore_exists(ds_name):

            cls._dump_datastore(ds, path, clear_dir)
        else:
            ds_list = [ds_name] if ds_name else ['resources','objects','state','events',]
            for ds in ds_list:
                cls._dump_datastore(ds, path, clear_dir)

    @classmethod
    def _dump_datastore(cls, ds_name, outpath_base, clear_dir=True):
        ds = DatastoreManager.get_datastore(ds_name)
        if not ds:
            return
        if not os.path.exists(outpath):
            os.makedirs(outpath)
        [os.remove(os.path.join(outpath, f)) for f in os.listdir(outpath)]

        outpath = "%s/%s" % (outpath_base, ds_name)

        objs = ds.find_by_view("_all_docs", None, id_only=False, convert_doc=False)
        numwrites = 0
        for obj_id, obj_key, obj in objs:
            if obj_id.startswith("_design"):
                continue
            with open("%s/%s.yml" % (outpath, obj_id), 'w') as f:
                yaml.dump(obj, f, default_flow_style=False)
                numwrites += 1
        log.info("Wrote %s objects to %s" % (numwrites, outpath))
