#!/usr/bin/env python

"""Process that loads the datastore"""

__author__ = 'Michael Meisinger, Thomas Lennan'

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

from pyon.public import CFG, log, sys_name, StreamProcess, Container


class DatastoreLoader(StreamProcess):
    def on_init(self, *args, **kwargs):
        pass

    def on_start(self, *args, **kwargs):
        datastore = self.CFG.get("datastore", "")
        path = self.CFG.get("path", None)
        op = self.CFG.get("op", None)
        if op:
            if op == "load":
                self.load_datastore(datastore, path)
            elif op == "dump":
                self.dump_datastore(datastore, path)
            else:
                log.warn("No operation specified")

    def on_quit(self, *args, **kwargs):
        pass

    @classmethod
    def _read_and_create_obj(cls, ds, fp):
        with open(fp, 'r') as f:
            yaml_text = f.read()
        obj = yaml.load(yaml_text)
        ds._preload_create_doc(obj)

    @classmethod
    def load_datastore(cls, ds_name, path=None):
        if CFG.system.mockdb:
            log.warn("Cannot load into MockDB")
            return
        if not ds_name:
            log.warn("Datastore name not provided")
        if not Container.instance.datastore_manager.exists(ds_name):
            log.warn("Datastore does not exist")

        ds = Container.instance.datastore_manager.get_datastore(ds_name)

        path = path or "res/preload/default"
        if not os.path.exists(path):
            log.warn("Load file or dir not found")
            return

        if os.path.isdir(path) == True:
            for fn in os.listdir(path):
                fp = os.path.join(path, fn)
                cls._read_and_create_obj(ds, fp)
        else:
            cls._read_and_create_obj(ds, path)


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
            if Container.instance.datastore_manager.exists(ds_name):
                cls._dump_datastore(ds_name, path, clear_dir)
            else:
                log.warn("Datastore does not exist")
        else:
            ds_list = [ds_name] if ds_name else ['resources','objects','state','events',]
            for ds in ds_list:
                cls._dump_datastore(ds, path, clear_dir)

    @classmethod
    def _dump_datastore(cls, ds_name, outpath_base, clear_dir=True):
        ds = Container.instance.datastore_manager.get_datastore(ds_name)
        if not ds:
            return
        if not os.path.exists(outpath_base):
            os.makedirs(outpath_base)
        if clear_dir:
            [os.remove(os.path.join(outpath_base, f)) for f in os.listdir(outpath_base)]

        outpath = "%s/%s" % (outpath_base, ds_name)
        if not os.path.exists(outpath):
            os.makedirs(outpath)

        objs = ds.find_by_view("_all_docs", None, id_only=False, convert_doc=False)
        numwrites = 0
        for obj_id, obj_key, obj in objs:
            if obj_id.startswith("_design"):
                continue
            fn = obj_id
            # Some object ids start with slash
            if obj_id.startswith("/"):
                fn = "_" + obj_id
            with open("%s/%s.yml" % (outpath, fn), 'w') as f:
                yaml.dump(obj, f, default_flow_style=False)
                numwrites += 1
        log.info("Wrote %s objects to %s" % (numwrites, outpath))
