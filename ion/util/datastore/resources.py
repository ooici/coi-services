#!/usr/bin/env python

"""Helpers to work with the resource registry"""

__author__ = 'Michael Meisinger'


import datetime
try:
    import xlrd
    import xlwt
except ImportError:
    print "Imports failed"

from pyon.core import bootstrap
from pyon.core.bootstrap import CFG, get_sys_name

from pyon.datastore.datastore import DataStore

#from pyon.datastore.datastore_common import DatastoreFactory, DataStore
from pyon.core.exception import BadRequest, NotFound, Inconsistent
from pyon.datastore.couchdb.couchdb_standalone import CouchDataStore
from pyon.public import log


class ResourceRegistryHelper(object):
    def __init__(self, container = None):
        self.container = container or bootstrap.container_instance
        self.sysname = get_sys_name()
        self._resources = {}
        self._associations = {}
        self._directory = {}
        self._res_by_type = {}
        self._attr_by_type = {}

    def dump_resources_as_xlsx(self, filename=None):

        ds = CouchDataStore(DataStore.DS_RESOURCES, profile=DataStore.DS_PROFILE.RESOURCES, config=CFG, scope=self.sysname)
        all_objs = ds.find_docs_by_view("_all_docs", None, id_only=False)

        log.info("Found %s objects in datastore resources", len(all_objs))

        self._analyze_objects(all_objs)

        self._wb = xlwt.Workbook()
        self._worksheets = {}

        for restype in sorted(self._res_by_type.keys()):
            self._dump_resource_type(restype)

        dtstr = datetime.datetime.today().strftime('%Y%m%d_%H%M%S')
        path = "interface/resources_%s.xls" % dtstr
        self._wb.save(path)

    def _analyze_objects(self, resources_objs):
        for obj_id, key, obj in resources_objs:
            if obj_id.startswith("_design"):
                continue
            if not isinstance(obj, dict):
                raise Inconsistent("Object of bad type found: %s" % type(obj))
            obj_type = obj.get("type_", None)
            if obj_type == "Association":
                self._associations[obj_id] = obj
            elif obj_type == "DirEntry":
                self._directory[obj_id] = obj
            elif obj_type:
                self._resources[obj_id] = obj
                if obj_type not in self._res_by_type:
                    self._res_by_type[obj_type] = []
                self._res_by_type[obj_type].append(obj_id)
                for attr, value in obj.iteritems():
                    if obj_type not in self._attr_by_type:
                        self._attr_by_type[obj_type] = set()
                    self._attr_by_type[obj_type].add(attr)
            else:
                raise Inconsistent("Object with no type_ found: %s" % obj)

    def _dump_resource_type(self, restype):
        ws = self._wb.add_sheet(restype)
        self._worksheets[restype] = ws
        for j, attr in enumerate(sorted(list(self._attr_by_type[restype]))):
            ws.write(0, j, attr)

        res_objs = [self._resources[res_id] for res_id in self._res_by_type[restype]]
        res_objs.sort(key=lambda doc: doc['name'])
        for i, res_obj in enumerate(res_objs):
            for j, attr in enumerate(sorted(list(self._attr_by_type[restype]))):
                value = res_obj.get(attr, "")
                if type(value) in (str, bool, int, None, float):
                    ws.write(i+1, j, value)
                elif isinstance(value, dict):
                    if value.get("type_", None):
                        ws.write(i+1, j, value["type_"])
                    else:
                        ws.write(i+1, j, "Dict of length %s" % len(value))
                elif isinstance(value, list):
                    ws.write(i+1, j, "List of length %s" % len(value))
                else:
                    ws.write(i+1, j, "Type:%s" % type(value))

