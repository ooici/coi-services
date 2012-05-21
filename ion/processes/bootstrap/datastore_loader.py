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

import yaml
import datetime
import os
import os.path

from pyon.public import CFG, log, ImmediateProcess, iex
from pyon.datastore.datastore import DatastoreManager
from pyon.core.bootstrap import get_sys_name
from pyon.core.exception import BadRequest

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
        if op:
            if op == "load":
                self.load_datastore(path, datastore, ignore_errors=False)
            elif op == "dump":
                self.dump_datastore(path, datastore)
            elif op == "blame":
                # TODO make generic
                self.get_blame_objects()
            elif op == "clear":
                self.clear_datastore(datastore, prefix)
            else:
                raise iex.BadRequest("Operation unknown")
        else:
            raise iex.BadRequest("No operation specified")

    def on_quit(self):
        pass

    @classmethod
    def load_datastore(cls, path=None, ds_name=None, ignore_errors=True):
        if CFG.system.mockdb:
            log.warn("Cannot load into MockDB")
            return

        path = path or "res/preload/default"
        if not os.path.exists(path):
            log.warn("Load path not found: %s" % path)
            return
        if not os.path.isdir(path):
            log.error("Path is not a directory: %s" % path)

        if ds_name:
            # Here we expect path to contain YML files for given datastore
            log.info("DatastoreLoader: LOAD datastore=%s" % ds_name)
            cls._load_datastore(path, ds_name, ignore_errors)
        else:
            # Here we expect path to have subdirs that are named according to logical
            # datastores, e.g. "resources"
            log.info("DatastoreLoader: LOAD ALL DATASTORES")
            for fn in os.listdir(path):
                fp = os.path.join(path, fn)
                if not os.path.exists(path):
                    log.warn("Item %s is not a directory" % fp)
                    continue
                cls._load_datastore(fp, fn, ignore_errors)

    @classmethod
    def _load_datastore(cls, path=None, ds_name=None, ignore_errors=True):
        if not DatastoreManager.exists(ds_name):
            log.warn("Datastore does not exist: %s" % ds_name)
        ds = DatastoreManager.get_datastore_instance(ds_name)
        objects = []
        for fn in os.listdir(path):
            fp = os.path.join(path, fn)
            try:
                with open(fp, 'r') as f:
                    yaml_text = f.read()
                obj = yaml.load(yaml_text)
                if "_rev" in obj:
                    del obj["_rev"]
                objects.append(obj)
            except Exception as ex:
                if ignore_errors:
                    log.warn("load error id=%s err=%s" % (fn, str(ex)))
                else:
                    raise ex

        if objects:
            try:
                res = ds.create_doc_mult(objects, allow_ids=True)
                log.info("DatastoreLoader: Loaded %s objects into %s" % (len(res), ds_name))
            except Exception as ex:
                if ignore_errors:
                    log.warn("load error id=%s err=%s" % (fn, str(ex)))
                else:
                    raise ex

    @classmethod
    def dump_datastore(cls, path=None, ds_name=None, clear_dir=True):
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
            if DatastoreManager.exists(ds_name):
                cls._dump_datastore(ds_name, path, clear_dir)
            else:
                log.warn("Datastore does not exist")
        else:
            ds_list = ['resources', 'objects', 'state', 'events',
                    'directory', 'scidata']
            for ds in ds_list:
                cls._dump_datastore(path, ds, clear_dir)

    @classmethod
    def _dump_datastore(cls, outpath_base, ds_name, clear_dir=True):
        if not DatastoreManager.exists(ds_name):
            log.warn("Datastore does not exist: %s" % ds_name)
            return
        ds = DatastoreManager.get_datastore_instance(ds_name)

        if not os.path.exists(outpath_base):
            os.makedirs(outpath_base)

        outpath = "%s/%s" % (outpath_base, ds_name)
        if not os.path.exists(outpath):
            os.makedirs(outpath)
        if clear_dir:
            [os.remove(os.path.join(outpath, f)) for f in os.listdir(outpath)]

        objs = ds.find_by_view("_all_docs", None, id_only=False, convert_doc=False)
        numwrites = 0
        for obj_id, obj_key, obj in objs:
            fn = obj_id
            # Some object ids have slashes
            fn = obj_id.replace("/","_")
            with open("%s/%s.yml" % (outpath, fn), 'w') as f:
                yaml.dump(obj, f, default_flow_style=False)
                numwrites += 1
        log.info("Wrote %s objects to %s" % (numwrites, outpath))

    @classmethod
    def _get_datastore_names(cls, prefix=None):
        return []

    @classmethod
    def clear_datastore(cls, ds_name=None, prefix=None):
        if CFG.system.mockdb:
            log.warn("Cannot clear MockDB")
            return

        generic_ds = DatastoreManager.get_datastore_instance("")

        if ds_name:
            # First interpret ds_name as unqualified name
            if DatastoreManager.exists(ds_name, scoped=False):
                generic_ds.delete_datastore(ds_name)
                return
            # New interpret as logical name
            if DatastoreManager.exists(ds_name, scoped=True):
                generic_ds.delete_datastore(ds_name)
            else:
                log.warn("Datastore does not exist: %s" % ds_name)
        elif prefix:
            db_list = generic_ds.list_datastores()
            cleared, ignored = 0, 0
            for db_name in db_list:
                if db_name.startswith(prefix):
                    generic_ds.delete_datastore(db_name)
                    log.debug("Cleared couch datastore '%s'" % db_name)
                    cleared += 1
                else:
                    ignored += 1
            log.info("Cleared %d couch datastores, ignored %d" % (cleared, ignored))
        else:
            log.warn("Cannot clear datastore without prefix or datastore name")

    @classmethod
    def get_blame_objects(cls):
        ds_list = ['resources', 'objects', 'state', 'events', 'directory', 'scidata']
        blame_objs = {}
        for ds_name in ds_list:
            try:
                ds = DatastoreManager.get_datastore_instance(ds_name)
            except BadRequest:
                continue
            ret_objs = ds.find_by_view("_all_docs", None, id_only=False, convert_doc=False)
            objs = []
            for obj_id, obj_key, obj in ret_objs:
                if "blame_" in obj:
                    objs.append(obj)
            blame_objs[ds_name] = objs
        return blame_objs

    @classmethod
    def bulk_delete(cls, objs):
        for ds_name in objs:
            ds = DatastoreManager.get_datastore_instance(ds_name)
            for obj in objs[ds_name]:
                ds.delete(obj["_id"])
        
DatastoreLoader = DatastoreAdmin
