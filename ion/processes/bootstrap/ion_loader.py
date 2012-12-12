#!/usr/bin/env python

"""Process that loads ION resources via service calls based on definitions in spreadsheets using loader functions.

    @see https://confluence.oceanobservatories.org/display/CIDev/R2+System+Preload
    bin/pycc -x ion.processes.bootstrap.ion_loader.IONLoader op=load path=master scenario=R2_DEMO
    bin/pycc -x ion.processes.bootstrap.ion_loader.IONLoader op=load path=res/preload/r2_ioc/R2PreloadedResources.xlsx scenario=R2_DEMO

    bin/pycc -x ion.processes.bootstrap.ion_loader.IONLoader op=load path="https://docs.google.com/spreadsheet/pub?key=0AttCeOvLP6XMdG82NHZfSEJJOGdQTkgzb05aRjkzMEE&output=xls" scenario=R2_DEMO
    bin/pycc -x ion.processes.bootstrap.ion_loader.IONLoader op=load path=res/preload/r2_ioc scenario=R2_DEMO

    bin/pycc -x ion.processes.bootstrap.ion_loader.IONLoader op=loadui path=res/preload/r2_ioc
    bin/pycc -x ion.processes.bootstrap.ion_loader.IONLoader op=loadui path=https://userexperience.oceanobservatories.org/database-exports/

    bin/pycc -x ion.processes.bootstrap.ion_loader.IONLoader op=load path=master assets=res/preload/r2_ioc/ooi_assets scenario=R2_DEMO loadooi=True
    bin/pycc -x ion.processes.bootstrap.ion_loader.IONLoader op=load path=res/preload/r2_ioc scenario=R2_DEMO loadooi=True assets=res/preload/r2_ioc/ooi_assets
    bin/pycc -x ion.processes.bootstrap.ion_loader.IONLoader op=load path=res/preload/r2_ioc scenario=R2_DEMO loadui=True

    bin/pycc -x ion.processes.bootstrap.ion_loader.IONLoader op=parseooi assets=res/preload/r2_ioc/ooi_assets

    bin/pycc -x ion.processes.bootstrap.ion_loader.IONLoader op=deleteooi

    ui_path= override location to get UI preload files (default is path + '/ui_assets')
    assets= override location to get OOI asset file (default is path + '/ooi_assets')
    attachments= override location to get file attachments (default is path)
    ooifilter= one or comma separated list of CE,CP,GA,GI,GP,GS,ES to limit ooi resource import
    ooiexclude= one or more categories to NOT import in the OOI import
    bulk= if True, uses RR bulk insert operations to load, not service calls

    TODO: constraints defined in multiple tables as list of IDs, but not used
    TODO: support attachments using HTTP URL
    TODO: Owner, Events with bulk

    Note: For quick debugging without restarting the services container:
    - Once after starting r2deploy:
    bin/pycc -x ion.processes.bootstrap.datastore_loader.DatastoreLoader op=dump path=res/preload/local/my_dump
    - Before each test of the ion_loader:
    bin/pycc -fc -x ion.processes.bootstrap.datastore_loader.DatastoreLoader op=load path=res/preload/local/my_dump

"""

__author__ = 'Michael Meisinger, Ian Katz, Thomas Lennan'

import ast
import calendar
import csv
import numpy as np
import re
import requests
import time

from pyon.core.bootstrap import get_service_registry
from pyon.core.exception import NotFound
from pyon.datastore.datastore import DatastoreManager
from pyon.ion.identifier import create_unique_resource_id, create_unique_association_id
from pyon.ion.resource import get_restype_lcsm
from pyon.public import log, ImmediateProcess, iex, IonObject, RT, PRED, OT
from pyon.util.containers import get_ion_ts, named_any
from ion.core.ooiref import OOIReferenceDesignator
from ion.processes.bootstrap.ooi_loader import OOILoader
from ion.processes.bootstrap.ui_loader import UILoader
from ion.services.dm.utility.granule_utils import time_series_domain
from ion.agents.port.port_agent_process import PortAgentProcessType, PortAgentType
from ion.util.parameter_loader import ParameterPlugin
from ion.util.xlsparser import XLSParser
from coverage_model.parameter import ParameterContext
from coverage_model.parameter_types import QuantityType, ArrayType, RecordType
from coverage_model.basic_types import AxisTypeEnum
from ion.util.parameter_loader import ParameterPlugin
from ion.agents.platform.oms.oms_client_factory import OmsClientFactory


from interface import objects
import logging

DEFAULT_TIME_FORMAT = "%Y-%m-%dT%H:%M:%S"

### this master URL has the latest changes, but if columns have changed, it may no longer work with this commit of the loader code
MASTER_DOC = "https://docs.google.com/spreadsheet/pub?key=0AttCeOvLP6XMdG82NHZfSEJJOGdQTkgzb05aRjkzMEE&output=xls"

### the URL below should point to a COPY of the master google spreadsheet that works with this version of the loader
#TESTED_DOC = "https://docs.google.com/spreadsheet/pub?key=0AgkUKqO5m-ZidE01OXVvMnhraVZtM05rNkthQnVjU1E&output=xls"
TESTED_DOC = "https://docs.google.com/spreadsheet/pub?key=0AgjFgozf2vG6dDlHWDhhZUgxYzlLZ2VtdjRPQXNYR0E&output=xls"
#
### while working on changes to the google doc, use this to run test_loader.py against the master spreadsheet
#TESTED_DOC=MASTER_DOC

# The preload spreadsheets (tabs) in the order they should be loaded
DEFAULT_CATEGORIES = [
    'Constraint',
    'Contact',
    'User',
    'Org',
    'UserRole',
    'CoordinateSystem',
    'ParameterDefs',
    'ParameterDictionary',
    'StreamConfiguration',
    'SensorModel',
    'PlatformModel',
    'InstrumentModel',
    'Observatory',
    'Subsite',
    'PlatformSite',
    'InstrumentSite',
    'StreamDefinition',
    'PlatformDevice',
    'PlatformAgent',
    'PlatformAgentInstance',
    'InstrumentDevice',
    'SensorDevice',
    'InstrumentAgent',
    'InstrumentAgentInstance',
    'DataProduct',
    'DataProcessDefinition',
    'DataProcess',
    'EventProcessDefinition',
    'EventProcess',
    'DataProductLink',
    'Attachment',
    'WorkflowDefinition',
    'Workflow',
    'Deployment',
    ]

class IONLoader(ImmediateProcess):
    COL_SCENARIO = "Scenario"
    COL_ID = "ID"
    COL_OWNER = "owner_id"
    COL_LCSTATE = "lcstate"
    COL_ORGS = "org_ids"

    ID_ORG_ION = "ORG_ION"
    ID_SYSTEM_ACTOR = "USER_SYSTEM"

    def on_start(self):
        # Main operation to perform
        op = self.CFG.get("op", None)

        # Additional parameters
        self.path = self.CFG.get("path", TESTED_DOC)
        if self.path=='master':
            self.path = MASTER_DOC
        self.attachment_path = self.CFG.get("attachments", self.path + '/attachments')
        self.asset_path = self.CFG.get("assets", self.path + "/ooi_assets")
        default_ui_path = self.path if self.path.startswith('http') else self.path + "/ui_assets"
        self.ui_path = self.CFG.get("ui_path", default_ui_path)
        scenarios = self.CFG.get("scenario", None)
        category_csv = self.CFG.get("categories", None)
        self.categories = category_csv.split(",") if category_csv else DEFAULT_CATEGORIES

        self.debug = self.CFG.get("debug", False)        # Debug mode with certain shorthands
        self.loadooi = self.CFG.get("loadooi", False)    # Import OOI asset data
        self.loadui = self.CFG.get("loadui", False)      # Import UI asset data
        self.exportui = self.CFG.get("exportui", False)  # Save UI JSON file
        self.update = self.CFG.get("update", False)      # Support update to existing resources
        self.bulk = self.CFG.get("bulk", False)          # Use bulk insert where available
        self.ooifilter = self.CFG.get("ooifilter", None) # Filter OOI import to RD prefixes (e.g. array "CE,GP")
        self.ooiexclude = self.CFG.get("ooiexclude", '') # Don't import the listed categories
        if self.ooiexclude:
            self.ooiexclude = self.ooiexclude.split(',')

        # External loader tools
        self.ui_loader = UILoader(self)
        self.ooi_loader = OOILoader(self, asset_path=self.asset_path)
        self.resource_ds = DatastoreManager.get_datastore_instance("resources")

        # Initialize variables used during subsequent load
        self.obj_classes = {}     # Cache of class for object types
        self.resource_ids = {}    # Holds a mapping of preload IDs to internal resource ids
        self.resource_objs = {}   # Holds a mapping of preload IDs to the actual resource objects
        self.existing_resources = None
        self.unknown_fields = {} # track unknown fields so we only warn once
        self.constraint_defs = {} # alias -> value for refs, since not stored in DB
        self.contact_defs = {} # alias -> value for refs, since not stored in DB
        self.stream_config = {} # name -> obj for StreamConfiguration objects, used by *AgentInstance
        # Loads internal bootstrapped resource ids that will be referenced during preload
        self._load_system_ids()

        log.info("IONLoader: {op=%s, path=%s, scenario=%s}" % (op, self.path, scenarios))
        if not op:
            raise iex.BadRequest("No operation specified")

        # Perform operations
        if op == "load":
            if not scenarios:
                raise iex.BadRequest("Must provide scenarios to load: scenario=sc1,sc2,...")

            if self.loadooi:
                self.ooi_loader.extract_ooi_assets()
            if self.loadui:
                specs_path = 'interface/ui_specs.json' if self.exportui else None
                self.ui_loader.load_ui(self.ui_path, specs_path=specs_path)

            # Load existing resources by preload ID
            self._prepare_incremental()

            scenarios = scenarios.split(',')
            self.load_ion(scenarios)

        elif op == "parseooi":
            self.ooi_loader.extract_ooi_assets()
        elif op == "loadui":
            specs_path = 'interface/ui_specs.json' if self.exportui else None
            self.ui_loader.load_ui(self.ui_path, specs_path=specs_path)
        elif op == "deleteooi":
            self.delete_ooi_assets()
        elif op == "deleteui":
            self.ui_loader.delete_ui()
        else:
            raise iex.BadRequest("Operation unknown")

    def on_quit(self):
        pass

    def load_ion(self, scenarios):
        """
        Loads resources for one scenario, by parsing input spreadsheets for all resource categories
        in defined order, executing service calls for all entries in the scenario.
        Can load the spreadsheets from http or file location.
        Optionally imports OOI assets at the beginning of each category.
        """
        log.info("Loading scenario from path: %s", self.path)
        if self.bulk:
            log.warn("WARNING: Bulk load is ENABLED. Making bulk RR calls to create resources/associations. No policy checks!")

        # Fetch the spreadsheet directly from a URL (from a GoogleDocs published spreadsheet)
        if self.path.startswith('http'):
            preload_doc_str = requests.get(self.path).content
            log.debug("Fetched URL contents, size=%s", len(preload_doc_str))
            xls_parser = XLSParser()
            self.csv_files = xls_parser.extract_csvs(preload_doc_str)
        elif self.path.endswith(".xlsx"):
            with open(self.path, "rb") as f:
                preload_doc_str = f.read()
                log.debug("Loaded xlsx file, size=%s", len(preload_doc_str))
                xls_parser = XLSParser()
                self.csv_files = xls_parser.extract_csvs(preload_doc_str)
        else:
            self.csv_files = None

        for category in self.categories:
            row_do, row_skip, num_bulk = 0, 0, 0
            self.bulk_objects = {}      # This keeps objects to be bulk inserted/updated at the end of a category

            # First load all OOI assets for this category
            if self.loadooi and category not in self.ooiexclude:
                catfunc_ooi = getattr(self, "_load_%s_OOI" % category, None)
                if catfunc_ooi:
                    log.debug('Loading OOI assets for %s', category)
                    catfunc_ooi()

            # Now load entries from preload spreadsheet top to bottom where scenario matches
            catfunc = getattr(self, "_load_%s" % category)
            filename = "%s/%s.csv" % (self.path, category)
            log.debug("Loading category %s", category)
            try:
                csvfile = None
                if self.csv_files is not None:
                    csv_doc = self.csv_files[category]
                    # This is a hack to be able to read from string
                    csv_doc = csv_doc.splitlines()
                    reader = csv.DictReader(csv_doc, delimiter=',')
                else:
                    csvfile = open(filename, "rb")
                    reader = csv.DictReader(csvfile, delimiter=',')

                for row in reader:
                    # Check if scenario applies
                    if row[self.COL_SCENARIO] not in scenarios:
                        row_skip += 1
                        continue
                    row_do += 1

                    log.debug('handling %s row: %r', category, row)
                    try:
                        catfunc(row)
                    except:
                        log.error('error loading %s row: %r', category, row, exc_info=True)
                        raise

                if self.bulk:
                    num_bulk = self._finalize_bulk(category)
                    # Update resource and associations views
                    self.container.resource_registry.find_resources(restype="X", id_only=True)
                    self.container.resource_registry.find_associations(predicate="X", id_only=True)

            except IOError, ioe:
                log.warn("Resource category file %s error: %s" % (filename, str(ioe)), exc_info=True)
            finally:
                if csvfile is not None:
                    csvfile.close()
                    csvfile = None

            log.info("Loaded category %s: %d resources from scenario rows, %s bulk, %d rows skipped" % (category, row_do, num_bulk, row_skip))

    def _load_system_ids(self):
        """Read some system objects for later reference"""
        org_objs,_ = self.container.resource_registry.find_resources(name="ION", restype=RT.Org, id_only=False)
        if not org_objs:
            raise iex.BadRequest("ION org not found. Was system force_cleaned since bootstrap?")
        ion_org_id = org_objs[0]._id
        self._register_id(self.ID_ORG_ION, ion_org_id, org_objs[0])

        system_actor, _ = self.container.resource_registry.find_resources(
            RT.ActorIdentity, name=self.CFG.system.system_actor, id_only=False)
        system_actor_id = system_actor[0]._id if system_actor else 'anonymous'
        self._register_id(self.ID_SYSTEM_ACTOR, system_actor_id, system_actor[0] if system_actor else None)

    def _prepare_incremental(self):
        """
        Look in the resource registry for any resources that have a preload ID on them so that
        they can be referenced under this preload ID during this load run.
        """
        log.debug("Preparing for incremental preload. Loading prior preloaded resources for reference")

        res_objs, res_keys = self.container.resource_registry.find_resources_ext(alt_id_ns="PRE", id_only=False)
        res_preload_ids = [key['alt_id'] for key in res_keys]
        res_ids = [obj._id for obj in res_objs]

        log.debug("Found %s previously preloaded resources", len(res_objs))

        self.existing_resources = dict(zip(res_preload_ids, res_objs))

        if len(self.existing_resources) != len(res_objs):
            raise iex.BadRequest("Stored preload IDs are NOT UNIQUE!!! Cannot link to old resources")

        res_id_mapping = dict(zip(res_preload_ids, res_ids))
        self.resource_ids.update(res_id_mapping)
        res_obj_mapping = dict(zip(res_preload_ids, res_objs))
        self.resource_objs.update(res_obj_mapping)

    def _finalize_bulk(self, category):
        res = self.resource_ds.create_mult(self.bulk_objects.values(), allow_ids=True)
        log.debug("Bulk stored %d resource objects/associations into resource registry", len(res))
        num_objects = len([1 for obj in self.bulk_objects.values() if obj._get_type() != "Association"])
        self.bulk_objects.clear()
        return num_objects

    def _create_object_from_row(self, objtype, row, prefix='',
                                constraints=None, constraint_field='constraint_list',
                                contacts=None, contact_field='contacts',
                                existing_obj=None):
        """
        Construct an IONObject of a determined type from given row dict with attributes.
        Convert all attributes according to their schema target type. Supports nested objects.
        Supports edit of objects of same type.
        """
        log.trace("Create object type=%s, prefix=%s", objtype, prefix)
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
                        log.trace("Get nested object field=%s type=%s, prefix=%s", nested_obj_field, nested_obj_type, nested_prefix)
                        nested_obj = self._create_object_from_row(nested_obj_type, row, nested_prefix)
                        obj_fields[nested_obj_field] = nested_obj
                        exclude_prefix.add(nested_obj_field)
                elif fieldname in schema:
                    try:
                        if value:
                            fieldvalue = self._get_typed_value(value, schema[fieldname])
                            obj_fields[fieldname] = fieldvalue
                    except Exception:
                        log.warn("Object type=%s, prefix=%s, field=%s cannot be converted to type=%s. Value=%s",
                            objtype, prefix, fieldname, schema[fieldname]['type'], value, exc_info=True)
                        #fieldvalue = str(fieldvalue)
                else:
                    # warn about unknown fields just once -- not on each row
                    if objtype not in self.unknown_fields:
                        self.unknown_fields[objtype] = []
                    if fieldname not in self.unknown_fields[objtype]:
                        log.warn("Skipping unknown field in %s: %s%s", objtype, prefix, fieldname)
                        self.unknown_fields[objtype].append(fieldname)
        if constraints:
            obj_fields[constraint_field] = constraints
        if contacts:
            obj_fields[contact_field] = contacts

        if existing_obj:
            # Edit attributes
            if existing_obj._get_type() != objtype:
                raise iex.Inconsistent("Cannot edit resource. Type mismatch old=%s, new=%s" % (existing_obj._get_type(), objtype))
            # TODO: Don't edit empty nested attributes
            for attr in list(obj_fields.keys()):
                if not obj_fields[attr]:
                    del obj_fields[attr]
            for attr in ('alt_ids','_id','_rev','type_'):
                if attr in obj_fields:
                    del obj_fields[attr]
            existing_obj.__dict__.update(obj_fields)
            log.trace("Update object type %s using field names %s", objtype, obj_fields.keys())
            obj = existing_obj
        else:
            if self.COL_ID in row and row[self.COL_ID] and 'alt_ids' in schema:
                if 'alt_ids' in obj_fields:
                    obj_fields['alt_ids'].append("PRE:"+row[self.COL_ID])
                else:
                    obj_fields['alt_ids'] = ["PRE:"+row[self.COL_ID]]

            log.trace("Create object type %s from field names %s", objtype, obj_fields.keys())
            obj = IonObject(objtype, **obj_fields)
        return obj

    def _get_object_class(self, objtype):
        if objtype in self.obj_classes:
            return self.obj_classes[objtype]
        try:
            obj_class = named_any("interface.objects.%s" % objtype)
            self.obj_classes[objtype] = obj_class
            return obj_class
        except:
            log.error('failed to find class for type %s' % objtype)

    def _get_typed_value(self, value, schema_entry=None, targettype=None):
        """
        Performs a value type conversion according to a schema specified target type.
        """
        targettype = targettype or schema_entry["type"]
        if schema_entry and 'enum_type' in schema_entry:
            enum_clzz = getattr(objects, schema_entry['enum_type'])
            return enum_clzz._value_map[value]
        elif targettype is 'str':
            return str(value)
        elif targettype is 'bool':
            lvalue = value.lower()
            if lvalue == 'true' or lvalue == '1':
               return True
            elif lvalue == 'false' or lvalue == '' or lvalue == '0':
                return False
            else:
                raise iex.BadRequest("Value %s is no bool" % value)
        elif targettype is 'int':
            try:
                return int(value)
            except Exception:
                log.warn("Value %s is type %s not type %s" % (value, type(value), targettype))
                return ast.literal_eval(value)
        elif targettype is 'float':
            try:
                return float(value)
            except Exception:
                log.warn("Value %s is type %s not type %s" % (value, type(value), targettype))
                return ast.literal_eval(value)
        elif targettype is 'simplelist':
            if value.startswith('[') and value.endswith(']'):
                value = value[1:len(value)-1].strip()
            elif not value.strip():
                return []
            return list(value.split(','))
        else:
            log.trace('parsing value as %s: %s', targettype, value)
            return ast.literal_eval(value)

    def _get_service_client(self, service):
        return get_service_registry().services[service].client(process=self)

    def _register_id(self, alias, resid, res_obj=None):
        if alias in self.resource_ids:
            raise iex.BadRequest("ID alias %s used twice" % alias)
        self.resource_ids[alias] = resid
        self.resource_objs[alias] = res_obj
        log.trace("Added resource alias=%s to id=%s", alias, resid)

    def _get_resource_obj(self, res_id):
        """Returns a resource object from one of the memory locations for given preload or internal ID"""
        if self.bulk and res_id in self.bulk_objects:
            return self.bulk_objects[res_id]
        elif res_id in self.resource_objs:
            return self.resource_objs[res_id]
        else:
            # Real ID not alias - reverse lookup
            alias_ids = [alias_id for alias_id,int_id in self.resource_ids.iteritems() if int_id==res_id]
            if alias_ids:
                return self.resource_objs[alias_ids[0]]
        return None

    def _get_alt_id(self, res_obj, prefix):
        alt_ids = getattr(res_obj, 'alt_ids', [])
        for alt_id in alt_ids:
            if alt_id.startswith(prefix+":"):
                alt_id_str = alt_id[len(prefix)+1:]
                return alt_id_str

    def _get_op_headers(self, row, force_user=False):
        headers = {}
        owner_id = row.get(self.COL_OWNER, None)
        if owner_id:
            owner_id = self.resource_ids[owner_id]
            headers['ion-actor-id'] = owner_id
            headers['ion-actor-roles'] = {'ION': ['ION_MANAGER', 'ORG_MANAGER']}
            headers['expiry'] = '0'
        elif force_user:
            return self._get_system_actor_headers()
        return headers

    def _basic_resource_create(self, row, restype, prefix, svcname, svcop,
                               constraints=None, constraint_field='constraint_list',
                               contacts=None, contact_field='contacts',
                               set_attributes=None, support_bulk=False,
                               **kwargs):
        """
        Orchestration method doing the following:
        - create an object from a row,
        - add any defined constraints,
        - make a service call to create resource for given object,
        - share resource in a given Org
        - store newly created resource id and obj for future reference
        - (optional) support bulk create/update
        """
        res_id_alias = row[self.COL_ID]
        existing_obj = None
        if res_id_alias in self.resource_ids:
            # TODO: Catch case when ID used twice
            existing_obj = self.resource_objs[res_id_alias]

        res_obj = self._create_object_from_row(restype, row, prefix,
                                               constraints=constraints, constraint_field=constraint_field,
                                               contacts=contacts, contact_field=contact_field,
                                               existing_obj=existing_obj)
        if set_attributes:
            for attr, attr_val in set_attributes.iteritems():
                setattr(res_obj, attr, attr_val)

        if existing_obj:
            res_id = self.resource_ids[res_id_alias]
            if self.bulk and support_bulk:
                self.bulk_objects[res_id] = res_obj
            else:
                # TODO: Use the appropriate service call here
                self.container.resource_registry.update(res_obj)
        else:
            if self.bulk and support_bulk:
                res_id = self._create_bulk_resource(res_obj, res_id_alias)
                headers = self._get_op_headers(row)
                self._resource_assign_owner(headers, res_obj)
            else:
                svc_client = self._get_service_client(svcname)
                headers = self._get_op_headers(row, force_user=True)
                res_id = getattr(svc_client, svcop)(res_obj, headers=headers, **kwargs)
                if res_id:
                    res_obj._id = res_id
                self._register_id(res_id_alias, res_id, res_obj)
            self._resource_assign_org(row, res_id)
        return res_id

    def _create_bulk_resource(self, res_obj, res_alias=None):
        if not hasattr(res_obj, "_id"):
            res_obj._id = create_unique_resource_id()
        ts = get_ion_ts()
        if hasattr(res_obj, "ts_created") and not res_obj.ts_created:
            res_obj.ts_created = ts
        if hasattr(res_obj, "ts_updated") and not res_obj.ts_updated:
            res_obj.ts_updated = ts
        res_id = res_obj._id
        self.bulk_objects[res_id] = res_obj
        if res_alias:
            self._register_id(res_alias, res_id, res_obj)
        return res_id

    def _resource_advance_lcs(self, row, res_id, restype=None):
        """
        Change lifecycle state of object to requested state. Supports bulk.
        """
        lcsm = get_restype_lcsm(restype)
        initial_lcstate = lcsm.initial_state if lcsm else "DEPLOYED_AVAILABLE"

        lcstate = row.get(self.COL_LCSTATE, None)
        if lcstate:
            if self.bulk and res_id in self.bulk_objects:
                self.bulk_objects[res_id].lcstate = lcstate
            else:
                imat, ivis = initial_lcstate.split("_")
                mat, vis = lcstate.split("_")
                if mat != imat:
                    self.container.resource_registry.set_lifecycle_state(res_id, "%s_PRIVATE" % mat)
                if vis != ivis:
                    self.container.resource_registry.set_lifecycle_state(res_id, "%s_%s" % (mat, vis))

    def _resource_assign_org(self, row, res_id):
        """
        Shares the resource in the given orgs. Supports bulk.
        """
        org_ids = row.get(self.COL_ORGS, None)
        if org_ids:
            org_ids = self._get_typed_value(org_ids, targettype="simplelist")
            for org_id in org_ids:
                org_res_id = self.resource_ids[org_id]
                if self.bulk and res_id in self.bulk_objects:
                    # Note: org_id is alias, res_id is internal ID
                    org_obj = self._get_resource_obj(org_id)
                    res_obj = self._get_resource_obj(res_id)
                    # Create association to given Org
                    # Simulate OMS.assign_resource_to_observatory_org -> Org MS.share_resource
                    assoc_obj = self._create_association(org_obj, PRED.hasResource, res_obj)
                else:
                    svc_client = self._get_service_client("observatory_management")
                    svc_client.assign_resource_to_observatory_org(res_id, self.resource_ids[org_id], headers=self._get_system_actor_headers())

    def _resource_assign_owner(self, headers, res_obj):
        if self.bulk and 'ion-actor-id' in headers:
            owner_id = headers['ion-actor-id']
            user_obj = self._get_resource_obj(owner_id)
            if owner_id and owner_id != 'anonymous':
                self._create_association(res_obj, PRED.hasOwner, user_obj)

    def _get_contacts(self, row, field='contact_ids', type=None):
        return self._get_members(self.contact_defs, row, field, type, 'contact')

    def _get_constraints(self, row, field='constraint_ids', type=None):
        return self._get_members(self.constraint_defs, row, field, type, 'constraint')

    def _get_members(self, value_map, row, field, obj_type, member_type):
        values = []
        value = row[field]
        if value:
            names = self._get_typed_value(value, targettype="simplelist")
            if names:
                for name in names:
                    if name not in value_map:
                        msg = 'invalid ' + member_type + ': ' + name + ' (from value=' + value + ')'
                        if self.COL_ID in row:
                            msg = 'id ' + row[self.COL_ID] + ' refers to an ' + msg
                        if obj_type:
                            msg = obj_type + ' ' + msg
                        raise iex.BadRequest(msg)
                    value = value_map[name]
                    values.append(value)
        return values

    def _create_association(self, subject=None, predicate=None, obj=None):
        """
        Create an association between two IonObjects with a given predicate.
        Supports bulk mode
        """
        if self.bulk:
            if not subject or not predicate or not obj:
                raise iex.BadRequest("Association must have all elements set: %s/%s/%s" % (subject, predicate, obj))
            if "_id" not in subject:
                raise iex.BadRequest("Subject id not available")
            subject_id = subject._id
            st = subject._get_type()

            if "_id" not in obj:
                raise iex.BadRequest("Object id not available")
            object_id = obj._id
            ot = obj._get_type()

            assoc_id = create_unique_association_id()
            assoc_obj = IonObject("Association",
                s=subject_id, st=st,
                p=predicate,
                o=object_id, ot=ot,
                ts=get_ion_ts())
            assoc_obj._id = assoc_id
            self.bulk_objects[assoc_id] = assoc_obj
            return assoc_id, '1-norev'
        else:
            return self.container.resource_registry.create_association(subject, predicate, obj)

    # --------------------------------------------------------------------------------------------------
    # Add specific types of resources below

    def _load_Contact(self, row):
        """ create constraint IonObject but do not insert into DB,
            cache in dictionary for inclusion in other preload objects """
        id = row[self.COL_ID]
        log.debug('creating contact: ' + id)
        if id in self.contact_defs:
            raise iex.BadRequest('contact with ID already exists: ' + id)

        roles = self._get_typed_value(row['c/roles'], targettype='simplelist')
        del row['c/roles']
        #        phones = self._get_typed_value(row['c/phones'], targettype='simplelist')
        phones = self._parse_phones(row['c/phones'])
        del row['c/phones']

        contact = self._create_object_from_row("ContactInformation", row, "c/")
        contact.roles = roles
        contact.phones = phones

        self.contact_defs[id] = contact

    def _parse_dict(self, text):
        """ parse a SIMPLE dictionary of unquoted string keys and values
        """
        out = { }
        pairs = text.split(',')
        for pair in pairs:
            fields = pair.split(':')
            key = fields[0].strip()
            value = fields[1].strip()
            out[key] = value
        return out

    def _parse_phones(self, text):
        if ':' in text:
            out = []
            for type,number in self._parse_dict(text).iteritems():
                out.append(IonObject("Phone", phone_number=number, phone_type=type))
            return out
        elif text:
            return [ IonObject("Phone", phone_number=text.strip(), phone_type='office') ]
        else:
            return []

    def _load_Constraint(self, row):
        """ create constraint IonObject but do not insert into DB,
            cache in dictionary for inclusion in other preload objects """
        id = row[self.COL_ID]
        if id in self.constraint_defs:
            raise iex.BadRequest('constraint with ID already exists: ' + id)
        type = row['type']
        if type=='geospatial' or type=='geo' or type=='space':
            self.constraint_defs[id] = self._create_geospatial_constraint(row)
        elif type=='temporal' or type=='temp' or type=='time':
            self.constraint_defs[id] = self._create_temporal_constraint(row)
        else:
            raise iex.BadRequest('constraint type must be either geospatial or temporal, not ' + type)

    def _load_CoordinateSystem(self, row):
        gcrs = self._create_object_from_row("GeospatialCoordinateReferenceSystem", row, "m/")
        id = row[self.COL_ID]
        self.resource_ids[id] = gcrs

    def _load_CoordinateSystem_OOI(self):
        fakerow = {}
        fakerow[self.COL_ID] = 'OOI_SUBMERGED_CS'
        fakerow['m/geospatial_geodetic_crs'] = 'http://www.opengis.net/def/crs/EPSG/0/4326'
        fakerow['m/geospatial_vertical_crs'] = 'http://www.opengis.net/def/cs/EPSG/0/6498'
        fakerow['m/geospatial_latitude_units'] = 'degrees_north'
        fakerow['m/geospatial_longitude_units'] = 'degrees_east'
        fakerow['m/geospatial_vertical_units'] = 'meter'
        fakerow['m/geospatial_vertical_positive'] = 'down'

        self._load_CoordinateSystem(fakerow)

    def _create_geospatial_constraint(self, row):
        z = row['vertical_direction']
        if z=='depth':
            min = float(row['top'])
            max = float(row['bottom'])
        elif z=='elevation':
            min = float(row['bottom'])
            max = float(row['top'])
        else:
            raise iex.BadRequest('vertical_direction must be "depth" or "elevation", not ' + z)
        constraint = IonObject("GeospatialBounds",
            geospatial_latitude_limit_north=float(row['north']),
            geospatial_latitude_limit_south=float(row['south']),
            geospatial_longitude_limit_east=float(row['east']),
            geospatial_longitude_limit_west=float(row['west']),
            geospatial_vertical_min=min,
            geospatial_vertical_max=max)
        return constraint

    def _create_temporal_constraint(self, row):
        format = row['time_format'] or DEFAULT_TIME_FORMAT
        start = calendar.timegm(time.strptime(row['start'], format))
        end = calendar.timegm(time.strptime(row['end'], format))
        return IonObject("TemporalBounds", start_datetime=start, end_datetime=end)

    def _get_system_actor_headers(self):
        return {'ion-actor-id': self.resource_ids[self.ID_SYSTEM_ACTOR],
               'ion-actor-roles': {'ION': ['ION_MANAGER', 'ORG_MANAGER']},
               'expiry':'0'}

    def _load_User(self, row):
        # TODO: Make the calls below with an actor_id for the web server
        alias = row['ID']
        subject = row["subject"]
        name = row["name"]
        description = row['description']
        ims = self._get_service_client("identity_management")

        # Prepare contact and UserInfo attributes
        contacts = self._get_contacts(row, field='contact_id', type='User')
        if len(contacts) > 1:
            raise iex.BadRequest('User %s defined with too many contacts (should be 1)' % alias)
        contact = contacts[0] if len(contacts)==1 else None
        user_attrs = dict(name=name, description=description)
        if contact:
            user_attrs['name'] = "%s %s" % (contact.individual_names_given, contact.individual_name_family)
            user_attrs['contact'] = contact

        # Build ActorIdentity
        actor_name = "Identity for %s" % user_attrs['name']
        actor_identity_obj = IonObject("ActorIdentity", name=actor_name, alt_ids=["PRE:"+alias])
        actor_id = ims.create_actor_identity(actor_identity_obj, headers=self._get_system_actor_headers())
        actor_identity_obj._id = actor_id
        self._register_id(alias, actor_id, actor_identity_obj)

        # Build UserCredentials
        user_credentials_obj = IonObject("UserCredentials", name=subject,
            description="Default credentials for %s" % user_attrs['name'])
        ims.register_user_credentials(actor_id, user_credentials_obj, headers=self._get_system_actor_headers())

        # Build UserInfo
        user_info_obj = IonObject("UserInfo", **user_attrs)
        ims.create_user_info(actor_id, user_info_obj, headers=self._get_system_actor_headers())

    def _load_Org(self, row):
        log.trace("Loading Org (ID=%s)", row[self.COL_ID])
        contacts = self._get_contacts(row, field='contact_id', type='Org')
        res_obj = self._create_object_from_row("Org", row, "org/")
        if contacts:
            res_obj.contacts = [contacts[0]]
        log.trace("Org: %s", res_obj)

        headers = self._get_op_headers(row)

        res_id = None
        org_type = row["org_type"]
        if org_type == "MarineFacility":
            svc_client = self._get_service_client("observatory_management")
            res_id = svc_client.create_marine_facility(res_obj, headers=headers)
        elif org_type == "VirtualObservatory":
            svc_client = self._get_service_client("observatory_management")
            res_id = svc_client.create_virtual_observatory(res_obj, headers=headers)
        else:
            log.warn("Unknown Org type: %s" % org_type)

        if res_id:
            res_obj._id = res_id
            self._register_id(row[self.COL_ID], res_id, res_obj)

    def _load_UserRole(self, row):
        org_id = row["org_id"]
        if org_id:
            org_id = self.resource_ids[org_id]

        user_id = self.resource_ids[row["user_id"]]
        role_name = row["role_name"]

        svc_client = self._get_service_client("org_management")

        auto_enroll = self._get_typed_value(row["auto_enroll"], targettype="bool")
        if auto_enroll:
            svc_client.enroll_member(org_id, user_id, headers=self._get_system_actor_headers())

        if role_name != "ORG_MEMBER":
            svc_client.grant_role(org_id, user_id, role_name, headers=self._get_system_actor_headers())

    def _load_SensorModel(self, row):
        row['sm/reference_urls'] = repr(self._get_typed_value(row['sm/reference_urls'], targettype="simplelist"))
        self._basic_resource_create(row, "SensorModel", "sm/",
            "instrument_management", "create_sensor_model",
            support_bulk=True)

    def _get_org_ids(self, ooi_rd_list):
        if not ooi_rd_list:
            return ""
        marine_ios = set()
        for ooi_rd in ooi_rd_list:
            marine_io = self.ooi_loader.get_marine_io(ooi_rd)
            if marine_io == "CG":
                marine_ios.add("MF_CGSN")
            elif marine_io == "RSN":
                marine_ios.add("MF_RSN")
            elif marine_io == "EA":
                marine_ios.add("MF_EA")
        return ",".join(marine_ios)

    def _match_filter(self, rdlist):
        """Returns true if at least one item in given list (or comma separated str) matches a filter in ooifilter"""
        if not self.ooifilter:
            return True
        if not rdlist:
            return False
        if type(rdlist) is str:
            rdlist = [val.strip() for val in rdlist.split(",")]
        for item in rdlist:
            if item in self.ooifilter:
                return True
        return False

    def _load_PlatformModel(self, row):
        res_id = self._basic_resource_create(row, "PlatformModel", "pm/",
            "instrument_management", "create_platform_model",
            support_bulk=True)

    def _load_PlatformModel_OOI(self):
        ooi_objs = self.ooi_loader.get_type_assets("nodetype")

        for ooi_id, ooi_obj in ooi_objs.iteritems():
            fakerow = {}
            fakerow[self.COL_ID] = ooi_id + "_PM"
            fakerow['pm/name'] = ooi_obj['name']
            fakerow['pm/alt_ids'] = "['OOI:" + ooi_id + "_PM" + "']"
            fakerow['org_ids'] = self._get_org_ids(ooi_obj.get('array_list', None))

            if not self._match_filter(ooi_obj.get('array_list', None)):
                continue

            self._load_PlatformModel(fakerow)

    def _load_InstrumentModel(self, row):
        row['im/reference_urls'] = repr(self._get_typed_value(row['im/reference_urls'], targettype="simplelist"))
        #raw_stream_def = row['raw_stream_def']
        #parsed_stream_def = row['parsed_stream_def']
        #row['im/stream_configuration'] = "{'raw': '%s', 'parsed': '%s'}" % (raw_stream_def, parsed_stream_def)

        res_id = self._basic_resource_create(row, "InstrumentModel", "im/",
            "instrument_management", "create_instrument_model",
            support_bulk=True)

    def _load_InstrumentModel_OOI(self):
        ooi_objs = self.ooi_loader.get_type_assets("subseries")

        for ooi_id, ooi_obj in ooi_objs.iteritems():
            series_obj = self.ooi_loader.get_type_assets("series")[ooi_id[:6]]
            class_obj = self.ooi_loader.get_type_assets("class")[ooi_id[:5]]
            family_obj = self.ooi_loader.get_type_assets("family")[class_obj['family']]
            fakerow = {}
            fakerow[self.COL_ID] = ooi_id
            fakerow['im/name'] = ooi_obj['name']
            fakerow['im/alt_ids'] = "['OOI:" + ooi_id + "']"
            fakerow['im/description'] = ooi_obj['description']
            fakerow['im/instrument_family'] = class_obj['family']
            fakerow['raw_stream_def'] = ''
            fakerow['parsed_stream_def'] = ''
            fakerow['org_ids'] = self._get_org_ids(class_obj.get('array_list', None))
            reference_urls = []
            addl = {}
            if ooi_obj.get('makemodel', None) and ooi_obj['makemodel'][0]:
                # TODO: Catch case where there is more than one makemodel per subseries
                makemodel_obj = self.ooi_loader.get_type_assets("makemodel")[ooi_obj['makemodel'][0]]
                fakerow['im/manufacturer'] = makemodel_obj['Manufacturer']
                fakerow['im/manufacturer_url'] = makemodel_obj['Vendor Website']
                addl.update(dict(connector=makemodel_obj['Connector'],
                    makemodel=ooi_obj['makemodel'][0],
                    makemodel_description=makemodel_obj['Make_Model_Description'],
                    input_voltage_range=makemodel_obj['Input Voltage Range'],
                    interface=makemodel_obj['Interface'],
                    makemodel_url=makemodel_obj['Make/Model Website'],
                    output_description=makemodel_obj['Output Description'],
                ))
                if makemodel_obj['Make/Model Website']: reference_urls.append(makemodel_obj['Make/Model Website'])
            fakerow['im/reference_urls'] = ",".join(reference_urls)
            addl['alternate_name'] = ooi_obj['Alternate Instrument Class Name']
            addl['class_long_name'] = ooi_obj['ClassLongName']
            addl['comments'] = ooi_obj['Comments']
            fakerow['im/addl'] = repr(addl)

            if not self._match_filter(class_obj.get('array_list', None)):
                continue

            self._load_InstrumentModel(fakerow)

    def _load_Observatory(self, row):
        constraints = self._get_constraints(row, type='Observatory')
        coordinate_name = row['coordinate_system']

        res_id = self._basic_resource_create(row, "Observatory", "obs/",
            "observatory_management", "create_observatory",
            constraints=constraints, constraint_field='constraint_list',
            set_attributes=dict(coordinate_reference_system=self.resource_ids[coordinate_name]) if coordinate_name else None,
            support_bulk=True)

    def _load_Observatory_OOI(self):
        ooi_objs = self.ooi_loader.get_type_assets("array")
        for ooi_id, ooi_obj in ooi_objs.iteritems():
            fakerow = {}
            fakerow[self.COL_ID] = ooi_id
            fakerow['obs/name'] = ooi_obj['name']
            fakerow['obs/alt_ids'] = "['OOI:" + ooi_id + "']"
            fakerow['constraint_ids'] = ''
            fakerow['coordinate_system'] = 'OOI_SUBMERGED_CS'
            fakerow['org_ids'] = self._get_org_ids([ooi_id])

            if not self._match_filter(ooi_id):
                continue

            self._load_Observatory(fakerow)

    def _load_Subsite(self, row):
        constraints = self._get_constraints(row, type='Subsite')
        coordinate_name = row['coordinate_system']

        res_id = self._basic_resource_create(row, "Subsite", "site/",
            "observatory_management", "create_subsite",
            constraints=constraints, constraint_field='constraint_list',
            set_attributes=dict(coordinate_reference_system=self.resource_ids[coordinate_name]) if coordinate_name else None,
            support_bulk=True)

        headers = self._get_op_headers(row)
        psite_id = row.get("parent_site_id", None)
        if psite_id:
            if self.bulk:
                psite_obj = self._get_resource_obj(psite_id)
                site_obj = self._get_resource_obj(row[self.COL_ID])
                self._create_association(psite_obj, PRED.hasSite, site_obj)
            else:
                svc_client = self._get_service_client("observatory_management")
                svc_client.assign_site_to_site(res_id, self.resource_ids[psite_id],
                    headers=headers)

    def _load_Subsite_OOI(self):
        ooi_objs = self.ooi_loader.get_type_assets("site")
        for ooi_id, ooi_obj in ooi_objs.iteritems():
            fakerow = {}
            fakerow[self.COL_ID] = ooi_id
            fakerow['site/name'] = ooi_obj['name']
            fakerow['site/alt_ids'] = "['OOI:" + ooi_id + "']"
            fakerow['constraint_ids'] = ''
            fakerow['coordinate_system'] = 'OOI_SUBMERGED_CS'
            fakerow['parent_site_id'] = ooi_id[:2]
            fakerow['org_ids'] = self._get_org_ids([ooi_id[:2]])

            if not self._match_filter(ooi_id[:2]):
                continue

            self._load_Subsite(fakerow)

        ooi_objs = self.ooi_loader.get_type_assets("subsite")
        for ooi_id, ooi_obj in ooi_objs.iteritems():
            constrow = {}
            const_id1 = ''
            if ooi_obj['latitude'] or ooi_obj['longitude'] or ooi_obj['depth_subsite']:
                const_id1 = ooi_id + "_const1"
                constrow[self.COL_ID] = const_id1
                constrow['type'] = 'geospatial'
                constrow['south'] = ooi_obj['latitude'] or '0.0'
                constrow['north'] = ooi_obj['latitude'] or '0.0'
                constrow['west'] = ooi_obj['longitude'] or '0.0'
                constrow['east'] = ooi_obj['longitude'] or '0.0'
                constrow['vertical_direction'] = 'depth'
                constrow['top'] = ooi_obj['depth_subsite'] or '0.0'
                constrow['bottom'] = ooi_obj['depth_subsite'] or '0.0'
                self._load_Constraint(constrow)

            fakerow = {}
            fakerow[self.COL_ID] = ooi_id
            fakerow['site/name'] = ooi_obj['name']
            fakerow['site/alt_ids'] = "['OOI:" + ooi_id + "']"
            fakerow['constraint_ids'] = const_id1
            fakerow['coordinate_system'] = 'OOI_SUBMERGED_CS'
            fakerow['parent_site_id'] = ooi_id[:4]

            if not self._match_filter(ooi_id[:2]):
                continue

            self._load_Subsite(fakerow)

    def _load_PlatformSite(self, row):
        constraints = self._get_constraints(row, type='PlatformSite')
        coordinate_name = row['coordinate_system']

        res_id = self._basic_resource_create(row, "PlatformSite", "ps/",
            "observatory_management", "create_platform_site",
            constraints=constraints, constraint_field='constraint_list',
            set_attributes=dict(coordinate_reference_system=self.resource_ids[coordinate_name]) if coordinate_name else None,
            support_bulk=True)

        svc_client = self._get_service_client("observatory_management")

        headers = self._get_op_headers(row)
        psite_id = row.get("parent_site_id", None)
        if psite_id:
            if self.bulk:
                psite_obj = self._get_resource_obj(psite_id)
                site_obj = self._get_resource_obj(row[self.COL_ID])
                self._create_association(psite_obj, PRED.hasSite, site_obj)
            else:
                svc_client.assign_site_to_site(res_id, self.resource_ids[psite_id],
                    headers=headers)

        pm_ids = row["platform_model_ids"]
        if pm_ids:
            pm_ids = self._get_typed_value(pm_ids, targettype="simplelist")
            for pm_id in pm_ids:
                if self.bulk:
                    model_obj = self._get_resource_obj(pm_id)
                    site_obj = self._get_resource_obj(row[self.COL_ID])
                    self._create_association(site_obj, PRED.hasModel, model_obj)
                else:
                    svc_client.assign_platform_model_to_platform_site(self.resource_ids[pm_id], res_id,
                        headers=headers)

    def _load_PlatformSite_OOI(self):

        def _load_platform(ooi_id, ooi_obj):
            constrow = {}
            const_id1 = ''
            if ooi_obj.get('latitude',None) or ooi_obj.get('longitude',None) or ooi_obj.get('depth_subsite',None):
                const_id1 = ooi_id + "_const1"
                constrow[self.COL_ID] = const_id1
                constrow['type'] = 'geospatial'
                constrow['south'] = ooi_obj['latitude'] or '0.0'
                constrow['north'] = ooi_obj['latitude'] or '0.0'
                constrow['west'] = ooi_obj['longitude'] or '0.0'
                constrow['east'] = ooi_obj['longitude'] or '0.0'
                constrow['vertical_direction'] = 'depth'
                constrow['top'] = ooi_obj['depth_subsite'] or '0.0'
                constrow['bottom'] = ooi_obj['depth_subsite'] or '0.0'
                self._load_Constraint(constrow)

            fakerow = {}
            fakerow[self.COL_ID] = ooi_id
            fakerow['ps/name'] = ooi_obj.get('name', ooi_id)
            fakerow['ps/alt_ids'] = "['OOI:" + ooi_id + "']"
            fakerow['constraint_ids'] = const_id1
            fakerow['coordinate_system'] = 'OOI_SUBMERGED_CS'
            if ooi_obj.get('is_platform', False):
                fakerow['parent_site_id'] = ooi_id[:8]
            else:
                fakerow['parent_site_id'] = ooi_obj.get('platform_id', '')
            fakerow['platform_model_ids'] = ooi_id[9:11] + "_PM"
            fakerow['org_ids'] = self._get_org_ids([ooi_id[:2]])

            if not self._match_filter(ooi_id[:2]):
                return

            self._load_PlatformSite(fakerow)

        ooi_objs = self.ooi_loader.get_type_assets("node")
        # Pass 1: platform nodes
        for ooi_id, ooi_obj in ooi_objs.iteritems():
            if ooi_obj.get('is_platform', False):
                _load_platform(ooi_id, ooi_obj)
        # Pass 2: child nodes
        for ooi_id, ooi_obj in ooi_objs.iteritems():
            if not ooi_obj.get('is_platform', False):
                _load_platform(ooi_id, ooi_obj)

    def _load_InstrumentSite(self, row):
        constraints = self._get_constraints(row, type='InstrumentSite')
        coordinate_name = row['coordinate_system']

        res_id = self._basic_resource_create(row, "InstrumentSite", "is/",
            "observatory_management", "create_instrument_site",
            constraints=constraints, constraint_field='constraint_list',
            set_attributes=dict(coordinate_reference_system=self.resource_ids[coordinate_name]) if coordinate_name else None,
            support_bulk=True)

        svc_client = self._get_service_client("observatory_management")

        headers = self._get_op_headers(row)
        psite_id = row.get("parent_site_id", None)
        if psite_id:
            if self.bulk:
                psite_obj = self._get_resource_obj(psite_id)
                site_obj = self._get_resource_obj(row[self.COL_ID])
                self._create_association(psite_obj, PRED.hasSite, site_obj)
            else:
                svc_client.assign_site_to_site(res_id, self.resource_ids[psite_id],
                    headers=headers)

        im_ids = row["instrument_model_ids"]
        if im_ids:
            im_ids = self._get_typed_value(im_ids, targettype="simplelist")
            for im_id in im_ids:
                if self.bulk:
                    model_obj = self._get_resource_obj(im_id)
                    site_obj = self._get_resource_obj(row[self.COL_ID])
                    self._create_association(site_obj, PRED.hasModel, model_obj)
                else:
                    svc_client.assign_instrument_model_to_instrument_site(self.resource_ids[im_id], res_id,
                        headers=headers)

    def _load_InstrumentSite_OOI(self):
        ooi_objs = self.ooi_loader.get_type_assets("instrument")

        for ooi_id, ooi_obj in ooi_objs.iteritems():
            constrow = {}
            const_id1 = ''
            if ooi_obj['latitude'] or ooi_obj['longitude'] or ooi_obj['depth_port_max'] or ooi_obj['depth_port_min']:
                const_id1 = ooi_id + "_const1"
                constrow[self.COL_ID] = const_id1
                constrow['type'] = 'geospatial'
                constrow['south'] = ooi_obj['latitude'] or '0.0'
                constrow['north'] = ooi_obj['latitude'] or '0.0'
                constrow['west'] = ooi_obj['longitude'] or '0.0'
                constrow['east'] = ooi_obj['longitude'] or '0.0'
                constrow['vertical_direction'] = 'depth'
                constrow['top'] = ooi_obj['depth_port_min'] or '0.0'
                constrow['bottom'] = ooi_obj['depth_port_max'] or '0.0'
                self._load_Constraint(constrow)

            fakerow = {}
            fakerow[self.COL_ID] = ooi_id
            fakerow['is/name'] = ooi_id
            fakerow['is/alt_ids'] = "['OOI:" + ooi_id + "']"
            fakerow['constraint_ids'] = const_id1
            fakerow['coordinate_system'] = 'OOI_SUBMERGED_CS'
            fakerow['org_ids'] = self._get_org_ids([ooi_id[:2]])
            fakerow['instrument_model_ids'] = ooi_obj['instrument_model']
            fakerow['parent_site_id'] = ooi_id[:14]

            if not self._match_filter(ooi_id[:2]):
                continue

            self._load_InstrumentSite(fakerow)

    def _load_StreamDefinition(self, row):
        res_obj = self._create_object_from_row("StreamDefinition", row, "sdef/")
        pname = row["param_dict_name"]
        svc_client = self._get_service_client("dataset_management")
        parameter_dictionary_id = svc_client.read_parameter_dictionary_by_name(pname, id_only=True,
            headers=self._get_system_actor_headers())
        svc_client = self._get_service_client("pubsub_management")
        res_id = svc_client.create_stream_definition(name=res_obj.name, parameter_dictionary_id=parameter_dictionary_id,
            headers=self._get_system_actor_headers())
        self._register_id(row[self.COL_ID], res_id)

    def _load_ParameterDefs(self, row):
        param_type = row['Parameter Type']
        if param_type == 'record':
            param_type = RecordType()
        elif param_type == 'array':
            param_type = ArrayType()
        else:
            try:
                param_type = QuantityType(value_encoding = np.dtype(row['Parameter Type']))
            except TypeError:
                log.exception('Invalid parameter type for parameter %s: %s', row['Name'], row['Parameter Type'])

        context = ParameterContext(name=row['Name'], param_type=param_type)
        context.uom = row['Unit of Measure']
        additional_attrs = {
            'Attributes':'attributes',
            'Index Key':'index_key',
            'Ion Name':'ion_name',
            'Standard Name':'standard_name',
            'Long Name':'long_name',
            'OOI Short Name':'ooi_short_name',
            'CDM Data Type':'cdm_data_type',
            'Variable Reports':'variable_reports',
            'References List':'references_list',
            'Description': 'description',
            'Code Reports':'code_reports'
            }
        if row['Fill Value'] and row['Parameter Type'] not in ('array','row'):
            if 'uint' in row['Parameter Type']:
                context.fill_value = abs(int(row['Fill Value']))
            elif 'int' in row['Parameter Type']:
                context.fill_value = int(row['Fill Value'])
            else:
                context.fill_value = float(row['Fill Value'])

        if row['Axis']:
            s = row['Axis'].lower()
            if s == 'lat':
                context.axis = AxisTypeEnum.LAT
            elif s == 'lon':
                context.axis = AxisTypeEnum.LON
        for key in additional_attrs.iterkeys():
            if key in row and row[key]:
                setattr(context, additional_attrs[key], row[key])

        dataset_management = self._get_service_client('dataset_management')
        context_id = dataset_management.create_parameter_context(
            name=row['Name'], parameter_context=context.dump(),
            description=row['Description'],
            headers=self._get_system_actor_headers())

    def _load_ParameterDictionary(self, row):
        s = re.sub(r'\s+','',row['parameters'])
        contexts = s.split(',')
        dataset_management = self._get_service_client('dataset_management')
        try:
            context_ids = [dataset_management.read_parameter_context_by_name(i)._id for i in contexts]
            temporal_parameter = row['temporal_parameter'] or ''
            dataset_management.create_parameter_dictionary(name=row['name'],
                parameter_context_ids=context_ids,
                temporal_context=temporal_parameter,
                headers=self._get_system_actor_headers())
        except NotFound as e:
            log.error('Parameter dictionary %s missing context: %s', row['name'], e.message)

    def _load_PlatformDevice(self, row):
        contacts = self._get_contacts(row, field='contact_ids', type='InstrumentDevice')
        res_id = self._basic_resource_create(row, "PlatformDevice", "pd/",
            "instrument_management", "create_platform_device", contacts=contacts,
            support_bulk=True)

        if self.bulk:
            # Create DataProducer and association
            pd_obj = self._get_resource_obj(row[self.COL_ID])
            pd_alias = self._get_alt_id(pd_obj, "PRE")
            data_producer_obj = IonObject(RT.DataProducer, name=pd_obj.name,
                description="Primary DataProducer for PlatformDevice %s" % pd_obj.name,
                producer_context=IonObject(OT.InstrumentProducerContext), is_primary=True)
            dp_id = self._create_bulk_resource(data_producer_obj, pd_alias+"_DPPR")
            self._create_association(pd_obj, PRED.hasDataProducer, data_producer_obj)

        ims_client = self._get_service_client("instrument_management")
        headers = self._get_op_headers(row)
        ass_id = row["platform_model_id"]
        if ass_id:
            if self.bulk:
                model_obj = self._get_resource_obj(ass_id)
                device_obj = self._get_resource_obj(row[self.COL_ID])
                self._create_association(device_obj, PRED.hasModel, model_obj)
            else:
                ims_client.assign_platform_model_to_platform_device(self.resource_ids[ass_id], res_id,
                    headers=headers)

        self._resource_advance_lcs(row, res_id, "PlatformDevice")

    def _load_PlatformDevice_OOI(self):
        ooi_objs = self.ooi_loader.get_type_assets("nodetype")

        for ooi_id, ooi_obj in ooi_objs.iteritems():
            fakerow = {}
            fakerow[self.COL_ID] = ooi_id + "_PD"
            fakerow['pd/name'] = "%s (%s)" % (ooi_obj.get('name', ''), ooi_id)
            fakerow['org_ids'] = self._get_org_ids(ooi_obj.get('array_list', None))
            fakerow['platform_model_id'] = ooi_id + "_PM"
            fakerow['contact_ids'] = ''

            if not self._match_filter(ooi_obj.get('array_list', None)):
                continue

            self._load_PlatformDevice(fakerow)

    def _load_InstrumentDevice(self, row):
        row['id/reference_urls'] = repr(self._get_typed_value(row['id/reference_urls'], targettype="simplelist"))
        contacts = self._get_contacts(row, field='contact_ids', type='InstrumentDevice')
        res_id = self._basic_resource_create(row, "InstrumentDevice", "id/",
            "instrument_management", "create_instrument_device", contacts=contacts,
            support_bulk=True)

        if self.bulk:
            # Create DataProducer and association
            id_obj = self._get_resource_obj(row[self.COL_ID])
            id_alias = self._get_alt_id(id_obj, "PRE")
            data_producer_obj = IonObject(RT.DataProducer, name=id_obj.name,
                description="Primary DataProducer for InstrumentDevice %s" % id_obj.name,
                producer_context=IonObject(OT.InstrumentProducerContext), is_primary=True)
            dp_id = self._create_bulk_resource(data_producer_obj, id_alias+"_DPPR")
            self._create_association(id_obj, PRED.hasDataProducer, data_producer_obj)

        #        rr = self._get_service_client("resource_registry")
#        attachment_ids = self._get_typed_value(row['attachment_ids'], targettype="simplelist")
#        if attachment_ids:
#            log.trace('adding attachments to instrument device %s: %r', res_id, attachment_ids)
#            for id in attachment_ids:
#                rr.create_association(res_id, PRED.hasAttachment, self.resource_ids[id])

        ims_client = self._get_service_client("instrument_management")
        headers = self._get_op_headers(row)
        ass_id = row["instrument_model_id"]
        if ass_id:
            if self.bulk:
                model_obj = self._get_resource_obj(ass_id)
                device_obj = self._get_resource_obj(row[self.COL_ID])
                self._create_association(device_obj, PRED.hasModel, model_obj)
            else:
                ims_client.assign_instrument_model_to_instrument_device(self.resource_ids[ass_id], res_id,
                    headers=headers)
        ass_id = row["platform_device_id"]# if 'platform_device_id' in row else None
        if ass_id:
            if self.bulk:
                parent_obj = self._get_resource_obj(ass_id)
                device_obj = self._get_resource_obj(row[self.COL_ID])
                self._create_association(parent_obj, PRED.hasDevice, device_obj)
            else:
                ims_client.assign_instrument_device_to_platform_device(res_id, self.resource_ids[ass_id],
                    headers=headers)

        self._resource_advance_lcs(row, res_id, "InstrumentDevice")

    def _load_InstrumentDevice_OOI(self):
        ooi_objs = self.ooi_loader.get_type_assets("instrument")

        for ooi_id, ooi_obj in ooi_objs.iteritems():
            fakerow = {}
            fakerow[self.COL_ID] = ooi_id + "_ID"
            fakerow['id/name'] = ooi_id
            fakerow['org_ids'] = self._get_org_ids([ooi_id[:2]])
            ooi_rd = OOIReferenceDesignator(ooi_id)
            fakerow['instrument_model_id'] = ooi_rd.subseries_rd
            fakerow['platform_device_id'] = ooi_rd.node_type + "_PD"
            fakerow['id/reference_urls'] = ''
            fakerow['contact_ids'] = ''

            if not self._match_filter(ooi_id[:2]):
                continue

            self._load_InstrumentDevice(fakerow)

    def _load_SensorDevice(self, row):
        res_id = self._basic_resource_create(row, "SensorDevice", "sd/",
            "instrument_management", "create_sensor_device",
            support_bulk=True)

        ims_client = self._get_service_client("instrument_management")
        headers = self._get_op_headers(row)
        ass_id = row["sensor_model_id"]
        if ass_id:
            if self.bulk:
                model_obj = self._get_resource_obj(ass_id)
                device_obj = self._get_resource_obj(row[self.COL_ID])
                self._create_association(device_obj, PRED.hasModel, model_obj)
            else:
                ims_client.assign_sensor_model_to_sensor_device(self.resource_ids[ass_id], res_id,
                    headers=headers)
        ass_id = row["instrument_device_id"]
        if ass_id:
            if self.bulk:
                parent_obj = self._get_resource_obj(ass_id)
                device_obj = self._get_resource_obj(row[self.COL_ID])
                self._create_association(parent_obj, PRED.hasDevice, device_obj)
            else:
                ims_client.assign_sensor_device_to_instrument_device(res_id, self.resource_ids[ass_id],
                    headers=headers)
        self._resource_advance_lcs(row, res_id, "SensorDevice")

    def _load_StreamConfiguration(self, row):
        """ parse and save for use in *AgentInstance objects """
        obj = self._create_object_from_row("StreamConfiguration", row, "cfg/")
        self.stream_config[row['ID']] = obj

    def _load_InstrumentAgent(self, row):
        stream_config_names = self._get_typed_value(row['stream_configurations'], targettype="simplelist")
        stream_configurations = [ self.stream_config[name] for name in stream_config_names ]

        res_id = self._basic_resource_create(row, "InstrumentAgent", "ia/",
            "instrument_management", "create_instrument_agent",
            set_attributes=dict(stream_configurations=stream_configurations),
            support_bulk=True)

        if self.bulk:
            # Create DataProducer and association
            ia_obj = self._get_resource_obj(row[self.COL_ID])
            proc_def_obj = IonObject(RT.ProcessDefinition)
            pd_id = self._create_bulk_resource(proc_def_obj)
            self._create_association(ia_obj, PRED.hasProcessDefinition, proc_def_obj)

        svc_client = self._get_service_client("instrument_management")

        headers = self._get_op_headers(row)
        im_ids = row["instrument_model_ids"]
        if im_ids:
            im_ids = self._get_typed_value(im_ids, targettype="simplelist")
            for im_id in im_ids:
                if self.bulk:
                    model_obj = self._get_resource_obj(im_id)
                    agent_obj = self._get_resource_obj(row[self.COL_ID])
                    self._create_association(model_obj, PRED.hasAgentDefinition, agent_obj)
                else:
                    svc_client.assign_instrument_model_to_instrument_agent(self.resource_ids[im_id], res_id,
                        headers=headers)

        self._resource_advance_lcs(row, res_id, "InstrumentAgent")

    def _load_InstrumentAgent_OOI(self):
        ooi_objs = self.ooi_loader.get_type_assets("instrument")

        for ooi_id, ooi_obj in ooi_objs.iteritems():
            fakerow = {}
            fakerow[self.COL_ID] = ooi_id + "_IA"
            fakerow['ia/name'] = "Instrument Agent for " + ooi_id
            fakerow['org_ids'] = self._get_org_ids([ooi_id[:2]])
            ooi_rd = OOIReferenceDesignator(ooi_id)
            fakerow['instrument_model_ids'] = ooi_rd.subseries_rd
            fakerow['stream_configurations'] = ""

            if not self._match_filter(ooi_id[:2]):
                continue

            self._load_InstrumentAgent(fakerow)

    def _load_InstrumentAgentInstance(self, row):
        # create basic object from simple fields
        agent_instance = self._create_object_from_row("InstrumentAgentInstance", row, "iai/")

        # add more complicated attributes
        agent_instance.driver_config = { 'comms_config': { 'addr':  row['comms_server_address'],
                                                           'port':  int(row['comms_server_port']) } }

        agent_instance.port_agent_config = { 'device_addr':   row['iai/comms_device_address'],
                                             'device_port':   int(row['iai/comms_device_port']),
                                             'process_type':  PortAgentProcessType.UNIX,
                                             'port_agent_addr': 'localhost',
                                             'type': PortAgentType.ETHERNET,
                                             'binary_path':   "port_agent",
                                             'command_port':  int(row['comms_server_cmd_port']),
                                             'data_port':     int(row['comms_server_port']),
                                             'log_level':     5,  }

        agent_id = self.resource_ids[row["instrument_agent_id"]]
        device_id = self.resource_ids[row["instrument_device_id"]]
        client = self._get_service_client("instrument_management")
        headers = self._get_op_headers(row)
        client.create_instrument_agent_instance(
            agent_instance, instrument_agent_id=agent_id, instrument_device_id=device_id,
            headers=headers)

    def _load_PlatformAgent(self, row):
        log.debug("_load_PlatformAgent row %s " % str(row))

        stream_config_names = self._get_typed_value(row['stream_configurations'], targettype="simplelist")
        stream_configurations = [ self.stream_config[name] for name in stream_config_names ]

        res_id = self._basic_resource_create(row, "PlatformAgent", "pa/",
            "instrument_management", "create_platform_agent",
            set_attributes=dict(stream_configurations=stream_configurations),
            support_bulk=True)

        if self.bulk:
            # Create DataProducer and association
            pa_obj = self._get_resource_obj(row[self.COL_ID])
            proc_def_obj = IonObject(RT.ProcessDefinition)
            pd_id = self._create_bulk_resource(proc_def_obj)
            self._create_association(pa_obj, PRED.hasProcessDefinition, proc_def_obj)

        svc_client = self._get_service_client("instrument_management")
        headers = self._get_op_headers(row)
        model_ids = row["platform_model_ids"]
        if model_ids:
            model_ids = self._get_typed_value(model_ids, targettype="simplelist")
            for model_id in model_ids:
                if self.bulk:
                    model_obj = self._get_resource_obj(model_id)
                    agent_obj = self._get_resource_obj(row[self.COL_ID])
                    self._create_association(model_obj, PRED.hasAgentDefinition, agent_obj)
                else:
                    svc_client.assign_platform_model_to_platform_agent(self.resource_ids[model_id], res_id,
                        headers=headers)
        self._resource_advance_lcs(row, res_id, "InstrumentAgent")

    def _load_PlatformAgent_OOI(self):
        ooi_objs = self.ooi_loader.get_type_assets("nodetype")

        for ooi_id, ooi_obj in ooi_objs.iteritems():
            fakerow = {}
            fakerow[self.COL_ID] = ooi_id + "_PA"
            fakerow['pa/name'] = "Platform Agent for " + ooi_id
            fakerow['platform_model_ids'] = ooi_id + "_PM"
            fakerow['org_ids'] = self._get_org_ids(ooi_obj.get('array_list', None))
            fakerow['stream_configurations'] = ""

            if not self._match_filter(ooi_obj.get('array_list', None)):
                continue

            self._load_PlatformAgent(fakerow)

    def _load_PlatformAgentInstance(self, row):
        # create object with simple field types -- name, description
        agent_instance = self._create_object_from_row("PlatformAgentInstance", row, "pai/")

        # construct values for more complex fields
        platform_id = row['platform_id']
        platform_agent_id = self.resource_ids[row['platform_agent_id']]
        platform_device_id = self.resource_ids[row['platform_device_id']]
        platform_agent = self.resource_objs[row['platform_agent_id']]

        driver_config = self._parse_dict(row['driver_config'])
        driver_config['dvr_cls'] = platform_agent.driver_class
        driver_config['dvr_mod'] = platform_agent.driver_module

        #TODO: allow child platforms
        platform_topology = { platform_id: [] }

        #
        device_dict = self._HACK_get_device_dict(platform_id)

        admap = { platform_id: device_dict }
        platform_config = { 'platform_id':             platform_id,
                            'platform_topology':       platform_topology,
                            'agent_device_map':        admap,
                            'agent_streamconfig_map':  None,  # can we just omit?
                            'driver_config':           driver_config }
        agent_instance.agent_config = { 'platform_config': platform_config }

#        stream_config_names = self._get_typed_value(row['stream_configurations'], targettype="simplelist")
#        agent_instance.stream_configurations = [ self.stream_config[name] for name in stream_config_names ]

        headers = self._get_op_headers(row)
        res_id = self._get_service_client("instrument_management").create_platform_agent_instance(
            agent_instance, platform_agent_id, platform_device_id, headers=headers)
        self.resource_ids[row['ID']] = res_id

    #       TODO:
    #           lots of other parameters are necessary, but not part of the object.  somehow they must be saved for later actions.
    #        driver_config = self._parse_dict(row['driver_config'])
    #        agent_config = self._parse_dict(row['agent_config'])
    #        stream_definition = self.resource_objs[row['stream_definition']]

    def _HACK_get_device_dict(self, platform_id):
        """ TODO: remove from preload and the initial object def.  instead query OmsClient from IMS when the platform agent is started
            TODO: set a property in the agent instance to identify which OmsClient to use (need to support platforms from different Orgs)

            query the simulated OmsClient for information about the platform
        """

        simulator = OmsClientFactory.create_instance()

        # get network information (from: test_oms_launch2._prepare_platform_ports)
        port_dicts = []
        platform_port_info = simulator.getPlatformPorts(platform_id)
        for port_id, port in platform_port_info[platform_id].iteritems():
            port_dicts.append(dict(port_id=port_id, ip_address=port['comms']['ip']))

        # get attribute information (from: test_oms_launch2._prepare_platform_attributes)
        attribute_dicts = []
        platform_attribute_info = simulator.getPlatformAttributes(platform_id)
        for name, attribute in platform_attribute_info[platform_id].iteritems():
            attribute_dicts.append(dict(id=name, monitor_rate=attribute['monitorCycleSeconds'], units=attribute['units']))

        # package as needed by agent instance
        return { 'ports': port_dicts,
                 'platform_monitor_attributes': attribute_dicts }


    def _load_DataProcessDefinition(self, row):
        res_id = self._basic_resource_create(row, "DataProcessDefinition", "dpd/",
                                            "data_process_management", "create_data_process_definition")

        svc_client = self._get_service_client("data_process_management")

        input_strdef = row["input_stream_defs"]
        if input_strdef:
            input_strdef = self._get_typed_value(input_strdef, targettype="simplelist")
        log.trace("Assigning input StreamDefinition to DataProcessDefinition for %s" % input_strdef)
        headers = self._get_op_headers(row)

        for insd in input_strdef:
            svc_client.assign_input_stream_definition_to_data_process_definition(self.resource_ids[insd], res_id,
                headers=headers)

        output_strdef = row["output_stream_defs"]
        if output_strdef:
            output_strdef = self._get_typed_value(output_strdef, targettype="dict")
        for binding, strdef in output_strdef.iteritems():
            svc_client.assign_stream_definition_to_data_process_definition(self.resource_ids[strdef], res_id, binding,
                headers=headers)

    def _load_DataProcess(self, row):
        dpd_id = self.resource_ids[row["data_process_definition_id"]]
        log.trace("_load_DataProcess  data_product_def %s", str(dpd_id))
        in_data_product_id = self.resource_ids[row["in_data_product_id"]]
        configuration = row["configuration"]
        if configuration:
            configuration = self._get_typed_value(configuration, targettype="dict")

        out_data_products = row["out_data_products"]
        if out_data_products:
            out_data_products = self._get_typed_value(out_data_products, targettype="dict")
            for name, dp_id in out_data_products.iteritems():
                out_data_products[name] = self.resource_ids[dp_id]

        svc_client = self._get_service_client("data_process_management")

        headers = self._get_op_headers(row)
        res_id = svc_client.create_data_process(dpd_id, [in_data_product_id], out_data_products, configuration, headers=headers)
        self._register_id(row[self.COL_ID], res_id)

        self._resource_assign_org(row, res_id)

        res_id = svc_client.activate_data_process(res_id, headers=self._get_system_actor_headers())

    def _load_EventProcessDefinition(self, row):
        res_id = row[self.COL_ID]
        process_def = self._create_object_from_row("ProcessDefinition", row, "epd/")
        process_def.executable = { 'module': row['module'], 'class': row['class'] }

        process_dispatcher = self._get_service_client("process_dispatcher")
        data_process_client = self._get_service_client("data_process_management")

        headers = self._get_op_headers(row)
        dbid = process_dispatcher.create_process_definition(process_definition=process_def,
            headers=headers)
        process_def._id = dbid
        self.resource_ids[res_id] = dbid
        self.resource_objs[res_id] = process_def

#        input_strdef = row["input_stream_defs"]
#        if input_strdef:
#            input_strdef = self._get_typed_value(input_strdef, targettype="simplelist")
#        log.trace("Assigning input StreamDefinition to DataProcessDefinition for %s" % input_strdef)
#        for insd in input_strdef:
#            data_process_client.assign_input_stream_definition_to_data_process_definition(self.resource_ids[insd], dbid)

    def _load_EventProcess(self, row):
        process_def_id = self.resource_ids[row["data_process_definition_id"]]
        log.trace("_load_EventProcess  event_product_def %s", str(process_def_id))
        in_data_product_id = self.resource_ids[row["in_data_product_id"]]
        configuration = row["configuration"]
        if configuration:
            configuration = self._get_typed_value(configuration, targettype="dict")

        svc_client = self._get_service_client("data_process_management")

        headers = self._get_op_headers(row)
        process_dispatcher = self._get_service_client("process_dispatcher")
        process_dispatcher.schedule_process(process_definition_id=process_def_id, configuration=configuration,
            headers=headers)
# TODO: IonObject types not available...
#        res_id = svc_client.create_data_process(dpd_id, [in_data_product_id], out_data_products, configuration, headers=headers)
#        self._register_id(row[self.COL_ID], res_id)
#        self._resource_assign_org(row, res_id)
#        res_id = svc_client.activate_data_process(res_id)

    def _load_DataProduct(self, row, do_bulk=False):
        tdom, sdom = time_series_domain()

        contacts = self._get_contacts(row, field='contact_ids', type='InstrumentDevice')
        res_obj = self._create_object_from_row("DataProduct", row, "dp/", contacts=contacts, contact_field='contacts')

        constraint_id = row['geo_constraint_id']
        if constraint_id:
            res_obj.geospatial_bounds = self.constraint_defs[constraint_id]
        gcrs_id = row['coordinate_system_id']
        if gcrs_id:
            res_obj.geospatial_coordinate_reference_system = self.resource_ids[gcrs_id]
        res_obj.spatial_domain = sdom.dump()
        res_obj.temporal_domain = tdom.dump()
        # HACK: cannot parse CSV value directly when field defined as "list"
        # need to evaluate as simplelist instead and add to object explicitly
        res_obj.available_formats = self._get_typed_value(row['available_formats'], targettype="simplelist")

        headers = self._get_op_headers(row)

        if self.bulk and do_bulk:
            # This is a non-functional, diminished version of a DataProduct, just to show up in lists
            res_id = self._create_bulk_resource(res_obj, row[self.COL_ID])
            self._resource_assign_owner(headers, res_obj)
            # Create and associate Stream
            # Create and associate DataSet
        else:
            svc_client = self._get_service_client("data_product_management")
            stream_definition_id = self.resource_ids[row["stream_def_id"]]
            res_id = svc_client.create_data_product(data_product=res_obj, stream_definition_id=stream_definition_id,
                headers=headers)
            self._register_id(row[self.COL_ID], res_id, res_obj)

            if not self.debug and row['persist_data']=='1':
                svc_client.activate_data_product_persistence(res_id, headers=headers)

        self._resource_assign_org(row, res_id)
        self._resource_advance_lcs(row, res_id, "DataProduct")

    def _load_DataProduct_OOI(self):
        ooi_objs = self.ooi_loader.get_type_assets("instrument")

        for ooi_id, ooi_obj in ooi_objs.iteritems():
            const_id1 = ''
            if ooi_obj['latitude'] or ooi_obj['longitude'] or ooi_obj['depth_port_max'] or ooi_obj['depth_port_min']:
                # At this point, the constraint was already added with the InstrumentSite
                const_id1 = ooi_id + "_const1"

            if not self._match_filter(ooi_id[:2]):
                continue

            # (1) Device Data Product - parsed
            fakerow = {}
            fakerow[self.COL_ID] = ooi_id + "_DPIDP"
            fakerow['dp/name'] = "Data Product parsed for device " + ooi_id
            fakerow['org_ids'] = self._get_org_ids([ooi_id[:2]])
            fakerow['contact_ids'] = ''
            fakerow['geo_constraint_id'] = const_id1
            fakerow['coordinate_system_id'] = 'OOI_SUBMERGED_CS'
            fakerow['available_formats'] = ''
            fakerow['stream_def_id'] = ''
            self._load_DataProduct(fakerow, do_bulk=self.bulk)

            # (2) Device Data Product - raw
            fakerow = {}
            fakerow[self.COL_ID] = ooi_id + "_DPIDR"
            fakerow['dp/name'] = "Data Product raw for device " + ooi_id
            fakerow['org_ids'] = self._get_org_ids([ooi_id[:2]])
            fakerow['contact_ids'] = ''
            fakerow['geo_constraint_id'] = const_id1
            fakerow['coordinate_system_id'] = 'OOI_SUBMERGED_CS'
            fakerow['available_formats'] = ''
            fakerow['stream_def_id'] = ''
            self._load_DataProduct(fakerow, do_bulk=self.bulk)

            # (3) Site Data Product - parsed
            fakerow = {}
            fakerow[self.COL_ID] = ooi_id + "_DPISP"
            fakerow['dp/name'] = "Data Product parsed for site " + ooi_id
            fakerow['org_ids'] = self._get_org_ids([ooi_id[:2]])
            fakerow['contact_ids'] = ''
            fakerow['geo_constraint_id'] = const_id1
            fakerow['coordinate_system_id'] = 'OOI_SUBMERGED_CS'
            fakerow['available_formats'] = ''
            fakerow['stream_def_id'] = ''
            self._load_DataProduct(fakerow, do_bulk=self.bulk)

            data_product_list = ooi_obj.get('data_product_list', [])
            for dp_id in data_product_list:
                # (4*) Site Data Product DPS - Level
                fakerow = {}
                fakerow[self.COL_ID] = ooi_id + "_" + dp_id + "_DPID"
                fakerow['dp/name'] = "Data Product for site " + ooi_id + " DPS " + dp_id
                fakerow['org_ids'] = self._get_org_ids([ooi_id[:2]])
                fakerow['contact_ids'] = ''
                fakerow['geo_constraint_id'] = const_id1
                fakerow['coordinate_system_id'] = 'OOI_SUBMERGED_CS'
                fakerow['available_formats'] = ''
                fakerow['stream_def_id'] = ''

                self._load_DataProduct(fakerow, do_bulk=self.bulk)

#Data Product Identifier
#Data Product Name
#Data Product Level
#1-D Interpolation (INTERP1) QC
#Combine QC Flags (CMBNFLG) QC
#Conductivity Compressibility Compensation (CONDCMP) QC
#DOORS L2 Science Requirement #(s)
#DOORS L2 Science Requirement Text
#DPS DCN(s)
#Evaluate Polynomial (POLYVAL) QC
#Global Range Test (GLBLRNG) QC
#Gradient Test (GREDTST) QC
#Local Range Test (LOCLRNG) QC
#Modulus (MODULUS) QC
#Notes
#Processing Flow Diagram DCN(s)
#Regime(s)
#Solar Elevation (SOLAREL) QC
#Spike Test (SPKETST) QC
#Stuck Value Test (STUCKVL) QC
#Trend Test (TRNDTST) QC
#Units

    def _load_DataProductLink(self, row, do_bulk=False):
        dp_id = self.resource_ids[row["data_product_id"]]
        res_id = self.resource_ids[row["input_resource_id"]]
        type = row['resource_type']

        if type=='InstrumentDevice' or type=='PlatformDevice':
            if self.bulk and do_bulk:
                id_obj = self._get_resource_obj(row["input_resource_id"])
                dp_obj = self._get_resource_obj(row["data_product_id"])
                parent_obj = self._get_resource_obj(row["input_resource_id"] + "_DPPR")

                data_producer_obj = IonObject(RT.DataProducer, name=id_obj.name,
                    description="Subordinate DataProducer for InstrumentDevice %s" % id_obj.name)
                dp_id = self._create_bulk_resource(data_producer_obj)
                self._create_association(id_obj, PRED.hasOutputProduct, dp_obj)
                self._create_association(id_obj, PRED.hasDataProducer, data_producer_obj)
                self._create_association(dp_obj, PRED.hasDataProducer, data_producer_obj)
                self._create_association(data_producer_obj, PRED.hasParent, parent_obj)
            else:
                svc_client = self._get_service_client("data_acquisition_management")
                svc_client.assign_data_product(res_id, dp_id, headers=self._get_system_actor_headers())
        elif type=='InstrumentSite':
            if self.bulk and do_bulk:
                # Why create a site data product here???
                pass
            else:
                svc_client = self._get_service_client('observatory_management')
                svc_client.create_site_data_product(res_id, dp_id, headers=self._get_system_actor_headers())

    def _load_DataProductLink_OOI(self):
        ooi_objs = self.ooi_loader.get_type_assets("instrument")

        for ooi_id, ooi_obj in ooi_objs.iteritems():
            if not self._match_filter(ooi_id[:2]):
                continue

            fakerow = {}
            fakerow['data_product_id'] = ooi_id + "_DPIDP"
            fakerow['input_resource_id'] = ooi_id + "_ID"
            fakerow['resource_type'] = 'InstrumentDevice'
            self._load_DataProductLink(fakerow, do_bulk=self.bulk)

            fakerow = {}
            fakerow['data_product_id'] = ooi_id + "_DPIDR"
            fakerow['input_resource_id'] = ooi_id + "_ID"
            fakerow['resource_type'] = 'InstrumentDevice'
            self._load_DataProductLink(fakerow, do_bulk=self.bulk)

            # TODO: Step (3) from data product
            #fakerow = {}
            #fakerow['data_product_id'] = ooi_id + "_DPISP"
            #fakerow['input_resource_id'] = ooi_id + "_ID"
            #fakerow['resource_type'] = 'InstrumentDevice'
            #self._load_DataProductLink(fakerow, do_bulk=self.bulk)

            data_product_list = ooi_obj.get('data_product_list', [])
            for dp_id in data_product_list:
                fakerow = {}
                fakerow['data_product_id'] = ooi_id + "_" + dp_id + "_DPID"
                fakerow['input_resource_id'] = ooi_id + "_ID"
                fakerow['resource_type'] = 'InstrumentDevice'

                self._load_DataProductLink(fakerow, do_bulk=self.bulk)

    def _load_Attachment(self, row):
        log.info("Loading Attachment")

        res_id = self.resource_ids[row["resource_id"]]
        att_obj = self._create_object_from_row("Attachment", row, "att/")
        filename = row["file_path"]
        if not filename:
            raise iex.BadRequest('attachment did not include a filename: ' + row[self.COL_ID])

        path = "%s/%s" % (self.attachment_path, filename)
        try:
            with open(path, "rb") as f:
                att_obj.content = f.read()
        except IOError, ioe:
            # warn instead of fail here
            log.warn("Failed to open attachment file: %s/%s" % (path, ioe))

        att_id = self.container.resource_registry.create_attachment(res_id, att_obj)
        self._register_id(row[self.COL_ID], att_id, att_obj)

    def _load_WorkflowDefinition(self, row):
        log.info("Loading WorkflowDefinition")

        workflow_def_obj = self._create_object_from_row("WorkflowDefinition", row, "wfd/")
        workflow_client = self._get_service_client("workflow_management")

        # Create the workflow steps
        steps_string = row["steps"]
        workflow_step_ids = []
        if steps_string:
            workflow_step_ids = self._get_typed_value(steps_string, targettype="simplelist")
        else:
            log.info("No steps found for workflow definition. Ignoring this entry")
            return

        # Locate the data process def objects and add them to the workflow def
        for step_id in workflow_step_ids:
            workflow_step_obj = IonObject('DataProcessWorkflowStep', data_process_definition_id=self.resource_ids[step_id])
            workflow_def_obj.workflow_steps.append(workflow_step_obj)

        headers = self._get_op_headers(row)

        # Create it in the resource registry
        workflow_def_id = workflow_client.create_workflow_definition(workflow_def_obj,
            headers=headers)

        self._register_id(row[self.COL_ID], workflow_def_id, workflow_def_obj)

    def _load_Workflow(self,row):
        workflow_obj = self._create_object_from_row("Workflow", row, "wf/")
        workflow_client = self._get_service_client("workflow_management")
        workflow_def_id = self.resource_ids[row["wfd_id"]]
        in_dp_id = self.resource_ids[row["in_dp_id"]]

        # prepare the config dict
        configuration = row['configuration']
        if configuration:
            configuration = self._get_typed_value(configuration, targettype="dict")
            configuration["in_dp_id"] = in_dp_id

        persist_data_flag = False
        if row["persist_data"] == "TRUE":
            persist_data_flag = True

        headers = self._get_op_headers(row)

        # Create and start the workflow
        workflow_id, workflow_product_id = workflow_client.create_data_process_workflow(
            workflow_definition_id=workflow_def_id,
            input_data_product_id=in_dp_id, persist_workflow_data_product=persist_data_flag,
            configuration=configuration, timeout=30,
            headers=headers)

    def _load_Deployment(self,row):
        constraints = self._get_constraints(row, type='Deployment')
        deployment = self._create_object_from_row("Deployment", row, "d/", constraints=constraints)
        coordinate_name = row['coordinate_system']
        if coordinate_name:
            deployment.coordinate_reference_system = self.resource_ids[coordinate_name]

        device_id = self.resource_ids[row['device_id']]
        site_id = self.resource_ids[row['site_id']]

        oms = self._get_service_client("observatory_management")
        ims = self._get_service_client("instrument_management")

        headers = self._get_op_headers(row)

        deployment_id = oms.create_deployment(deployment, headers=headers)
        oms.deploy_instrument_site(site_id, deployment_id, headers=headers)
        ims.deploy_instrument_device(device_id, deployment_id, headers=headers)

        if row['activate']=='1':
            oms.activate_deployment(deployment_id, headers=headers)

    def delete_ooi_assets(self):
        res_ids = []

        ooi_asset_types = ['InstrumentModel',
                           'PlatformModel',
                           'Observatory',
                           'Subsite',
                           'PlatformSite',
                           'InstrumentSite',
                           'InstrumentAgent',
                           'InstrumentDevice',
                           'PlatformAgent',
                           'PlatformDevice',
                           'DataProduct'
                           ]

        for restype in ooi_asset_types:
            res_is_list, _ = self.container.resource_registry.find_resources(restype, id_only=True)
            res_ids.extend(res_is_list)
            #log.debug("Found %s resources of type %s" % (len(res_is_list), restype))

        docs = self.resource_ds.read_doc_mult(res_ids)

        for doc in docs:
            doc['_deleted'] = True

        # TODO: Also delete associations

        self.resource_ds.create_doc_mult(docs, allow_ids=True)
        log.info("Deleted %s OOI resources and associations", len(docs))


