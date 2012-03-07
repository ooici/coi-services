#!/usr/bin/env python

"""Process that loads ION resources via service calls based on given definitions"""

__author__ = 'Michael Meisinger, Ian Katz, Thomas Lennan'

import csv
import datetime
import os
import os.path

from pyon.core.bootstrap import service_registry
from pyon.public import CFG, log, ImmediateProcess, iex, IonObject
from pyon.util.containers import named_any

class IONLoader(ImmediateProcess):
    """
    bin/pycc -x ion.processes.bootstrap.ion_loader.IONLoader op=load path=res/preload/lca_demo scenario=LCA_DEMO_PRE
    """

    COL_SCENARIO = "Scenario"
    COL_ID = "ID"

    def on_start(self):
        op = self.CFG.get("op", None)
        path = self.CFG.get("path", None)
        scenario = self.CFG.get("scenario", None)

        log.info("IONLoader: {op=%s, path=%s, scenario=%s}" % (op, path, scenario))
        if op:
            if op == "load":
                self.load_ion(path, scenario)
            else:
                raise iex.BadRequest("Operation unknown")
        else:
            raise iex.BadRequest("No operation specified")

    def on_quit(self):
        pass

    def load_ion(self, path, scenario):
        if not path:
            raise iex.BadRequest("Must provide path")

        log.info("Start preloading from path=%s" % path)
        categories = ['User', 'MarineFacility', 'Site', 'LogicalPlatform', 'LogicalInstrument', 'PlatformModel', 'InstrumentModel']

        self.obj_classes = {}
        self.resource_ids = {}

        for category in categories:
            row_do, row_skip = 0, 0

            filename = "%s/%s.csv" % (path, category)
            log.info("Loading category %s from file %s" % (category, filename))
            with open(filename, "rb") as csvfile:
                reader = self._get_csv_reader(csvfile)
                for row in reader:
                    # Check if scenario applies
                    rowsc = row[self.COL_SCENARIO]
                    if not scenario in rowsc:
                        row_skip += 1
                        continue
                    row_do += 1

                    if category == "User":
                        self._load_user(row)
                    elif category == "MarineFacility":
                        self._load_marine_facility(row)
                    elif category == "Site":
                        self._load_site(row)
                    elif category == "LogicalPlatform":
                        self._load_logical_platform(row)
                    elif category == "LogicalInstrument":
                        self._load_logical_instrument(row)
                    elif category == "PlatformModel":
                        self._load_platform_model(row)
                    elif category == "InstrumentModel":
                        self._load_instrument_model(row)
                    else:
                        raise iex.BadRequest("Unknown category: %s" % category)

            log.info("Loaded category %s: %d rows imported, %d rows skipped" % (category, row_do, row_skip))

    def _get_csv_reader(self, csvfile):
        #determine type of csv
        #dialect = csv.Sniffer().sniff(csvfile.read(1024))
        #csvfile.seek(0)
        return csv.DictReader(csvfile, delimiter=',')

    def _create_object_from_row(self, objtype, row, prefix=''):
        log.info("Create object type=%s, prefix=%s" % (objtype, prefix))
        schema = self._get_object_class(objtype)._schema
        obj_fields = {}
        exclude_prefix = set()
        for col,value in row.iteritems():
            if col.startswith(prefix):
                fieldname = col[len(prefix):]
                if '/' in fieldname:
                    slidx = fieldname.find('/')
                    nested_obj_field = fieldname[:slidx]
                    if not nested_obj_field in exclude_prefix:
                        nested_obj_type = schema[nested_obj_field]['type']
                        nested_prefix = prefix + fieldname[:slidx+1]
                        log.info("Get nested object field=%s type=%s, prefix=%s" % (nested_obj_field, nested_obj_type, nested_prefix))
                        nested_obj = self._create_object_from_row(nested_obj_type, row, nested_prefix)
                        obj_fields[nested_obj_field] = nested_obj
                        exclude_prefix.add(nested_obj_field)
                elif fieldname in schema:
                    try:
                        if value:
                            fieldvalue = self._get_typed_value(value, schema[fieldname]['type'])
                            obj_fields[fieldname] = fieldvalue
                    except Exception:
                        log.warn("Object type=%s, prefix=%s, field=%s cannot be converted to type=%s. Value=%s" % (objtype, prefix, fieldname, schema[fieldname]['type'], value))
                        #fieldvalue = str(fieldvalue)
                else:
                    log.warn("Unknown fieldname: %s" % fieldname)
        log.info("Create object type %s from field names %s" % (objtype, obj_fields.keys()))
        obj = IonObject(objtype, **obj_fields)
        return obj

    def _get_object_class(self, objtype):
        if objtype in self.obj_classes:
            return self.obj_classes[objtype]

        obj_class = named_any("interface.objects.%s" % objtype)
        self.obj_classes[objtype] = obj_class
        return obj_class

    def _get_typed_value(self, value, targettype):
        if targettype is 'str':
            return str(value)
        elif targettype is 'bool':
            return bool(value)
        elif targettype is 'int':
            return int(value)
        elif targettype is 'list':
            return list(value.split('[,]'))
        else:
            raise Exception("Unknown type: %s" % targettype)

    def _get_service_client(self, service):
        return service_registry.services[service].client(process=self)

    def _register_id(self, alias, resid):
        if alias in self.resource_ids:
            raise iex.BadRequest("ID alias %s used twice" % alias)
        self.resource_ids[alias] = resid
        log.info("Added resource alias=%s to id=%s" % (alias, resid))

    # --------------------------------------------------------------------------------------------------
    # Add specific types of resources below

    def _load_user(self, row):
        log.info("Loading user")
        #user_obj = self._create_object_from_row("UserInfo", row)

    def _load_marine_facility(self, row):
        log.info("Loading MarineFacility")
        mf = self._create_object_from_row("MarineFacility", row, "mf/")
        log.info("MarineFacility: %s" % mf)

        mfms = self._get_service_client("marine_facility_management")
        mf_id = mfms.create_marine_facility(mf)
        self._register_id(row[self.COL_ID], mf_id)

    def _load_site(self, row):
        log.info("Loading Site")
        site = self._create_object_from_row("Site", row, "site/")
        log.info("Site: %s" % site)

        mfms = self._get_service_client("marine_facility_management")
        site_id = mfms.create_site(site)
        self._register_id(row[self.COL_ID], site_id)

        mf_id = row["marine_facility_id"]
        mfms.assign_site_to_marine_facility(site_id, self.resource_ids[mf_id])

    def _load_logical_platform(self, row):
        log.info("Loading LogicalPlatform")
        lp = self._create_object_from_row("LogicalPlatform", row, "lp/")
        log.info("LogicalPlatform: %s" % lp)

        mfms = self._get_service_client("marine_facility_management")
        lp_id = mfms.create_logical_platform(lp)
        self._register_id(row[self.COL_ID], lp_id)

        site_id = row["site_id"]
        mfms.assign_logical_platform_to_site(lp_id, self.resource_ids[site_id])

    def _load_logical_instrument(self, row):
        log.info("Loading LogicalInstrument")
        li = self._create_object_from_row("LogicalInstrument", row, "li/")
        log.info("LogicalInstrument: %s" % li)

        mfms = self._get_service_client("marine_facility_management")
        li_id = mfms.create_logical_instrument(li)
        self._register_id(row[self.COL_ID], li_id)

        lp_id = row["logical_platform_id"]
        mfms.assign_logical_instrument_to_logical_platform(li_id, self.resource_ids[lp_id])

    def _load_platform_model(self, row):
        log.info("Loading PlatformModel")
        pm = self._create_object_from_row("PlatformModel", row, "pm/")
        log.info("PlatformModel: %s" % pm)

        ims = self._get_service_client("instrument_management")
        pm_id = ims.create_platform_model(pm)
        self._register_id(row[self.COL_ID], pm_id)

    def _load_instrument_model(self, row):
        log.info("Loading InstrumentModel")
        im = self._create_object_from_row("InstrumentModel", row, "im/")
        log.info("InstrumentModel: %s" % im)

        ims = self._get_service_client("instrument_management")
        im_id = ims.create_instrument_model(im)
        self._register_id(row[self.COL_ID], im_id)
