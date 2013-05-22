#!/usr/bin/env python

"""Process that loads ION resources via service calls based on definitions in spreadsheets using loader functions.
    @see https://confluence.oceanobservatories.org/display/CIDev/R2+System+Preload

    Examples:
      bin/pycc -x ion.processes.bootstrap.ion_loader.IONLoader op=load path=master scenario=R2_DEMO
      bin/pycc -x ion.processes.bootstrap.ion_loader.IONLoader op=load path=res/preload/r2_ioc/R2PreloadedResources.xlsx scenario=R2_DEMO
      bin/pycc -x ion.processes.bootstrap.ion_loader.IONLoader op=load path="https://docs.google.com/spreadsheet/pub?key=0AttCeOvLP6XMdG82NHZfSEJJOGdQTkgzb05aRjkzMEE&output=xls" scenario=R2_DEMO
      bin/pycc -x ion.processes.bootstrap.ion_loader.IONLoader op=load path=res/preload/r2_ioc scenario=R2_DEMO

      bin/pycc -x ion.processes.bootstrap.ion_loader.IONLoader op=loadui path=res/preload/r2_ioc
      bin/pycc -x ion.processes.bootstrap.ion_loader.IONLoader op=loadui path=https://userexperience.oceanobservatories.org/database-exports/

      bin/pycc -x ion.processes.bootstrap.ion_loader.IONLoader op=load path=master assets=res/preload/r2_ioc/ooi_assets scenario=R2_DEMO loadooi=True
      bin/pycc -x ion.processes.bootstrap.ion_loader.IONLoader op=load path=res/preload/r2_ioc scenario=R2_DEMO loadooi=True assets=res/preload/r2_ioc/ooi_assets
      bin/pycc -x ion.processes.bootstrap.ion_loader.IONLoader op=load path=res/preload/r2_ioc scenario=R2_DEMO loadui=True

      bin/pycc -x ion.processes.bootstrap.ion_loader.IONLoader cfg=res/preload/r2_ioc/config/ooi_load_config.yml

      bin/pycc -x ion.processes.bootstrap.ion_loader.IONLoader op=parseooi assets=res/preload/r2_ioc/ooi_assets
      bin/pycc -x ion.processes.bootstrap.ion_loader.IONLoader op=deleteooi

    Options:
      ui_path= override location to get UI preload files (default is path + '/ui_assets')
      assets= override location to get OOI asset file (default is path + '/ooi_assets')
      assetmappings= override location for OOI mapping spreadsheet (default is GoogleDoc)
      attachments= override location to get file attachments (default is path)
      ooifilter= one or comma separated list of CE,CP,GA,GI,GP,GS,ES to limit ooi resource import
      ooiexclude= one or more categories to NOT import in the OOI import
      ooiuntil= datetime of latest planned deployment date to consider for data product etc import mm/dd/yyyy
      bulk= if True, uses RR bulk insert operations to load, not service calls
      debug= if True, allows a few shortcuts to perform faster loads
      exportui= if True, writes interface/ui_specs.json with UI object
      cfg= Path to a preload config file that allows scripted preload runs with defined params

    TODO:
      support attachments using HTTP URL
      Owner, Events with bulk
      Set lifecycle state through appropriate service operations
"""

__author__ = 'Michael Meisinger, Ian Katz, Thomas Lennan, Jonathan Newbrough'


import simplejson as json
import datetime
import ast
import calendar
import copy
import csv
import re
import requests
import time
from udunitspy.udunits2 import UdunitsError

from pyon.core.bootstrap import get_service_registry
from pyon.core.exception import NotFound
from pyon.datastore.datastore import DatastoreManager, DataStore
from pyon.ion.identifier import create_unique_resource_id, create_unique_association_id
from pyon.ion.resource import get_restype_lcsm
from pyon.public import log, ImmediateProcess, iex, IonObject, RT, PRED, OT, LCS, AS
from pyon.util.containers import get_ion_ts, named_any, dict_merge
from pyon.util.config import Config

from ion.agents.port.port_agent_process import PortAgentProcessType, PortAgentType
from ion.core.ooiref import OOIReferenceDesignator
from ion.processes.bootstrap.ooi_loader import OOILoader
from ion.processes.bootstrap.ui_loader import UILoader
from ion.services.dm.utility.granule_utils import time_series_domain
from ion.services.dm.utility.types import TypesManager
from ion.util.geo_utils import GeoUtils
from ion.util.parse_utils import parse_dict, parse_phones, get_typed_value
from ion.util.xlsparser import XLSParser

from coverage_model.parameter import ParameterContext
from coverage_model import NumexprFunction, PythonFunction, QuantityType

from interface import objects
from interface.objects import StreamAlertType

from ooi.timer import Accumulator, Timer
stats = Accumulator(persist=True)

# format for time values within the preload data
DEFAULT_TIME_FORMAT = "%Y-%m-%dT%H:%M:%S"

# sometimes the HTTP download from google returns only partial results (causing errors parsing).
# allow this many tries to get a clean, parseable document before giving up.
HTTP_RETRIES=5

## can set ui_path to keywords 'default' for TESTED_UI_ASSETS or 'candidate' for CANDIDATE_UI_ASSETS
TESTED_UI_ASSETS = 'https://userexperience.oceanobservatories.org/database-exports/'
CANDIDATE_UI_ASSETS = 'https://userexperience.oceanobservatories.org/database-exports/Candidates'

### this master URL has the latest changes, but if columns have changed, it may no longer work with this commit of the loader code
# Edit the doc here: https://docs.google.com/spreadsheet/ccc?key=0AttCeOvLP6XMdG82NHZfSEJJOGdQTkgzb05aRjkzMEE
MASTER_DOC = "https://docs.google.com/spreadsheet/pub?key=0AttCeOvLP6XMdG82NHZfSEJJOGdQTkgzb05aRjkzMEE&output=xls"

### the URL below should point to a COPY of the master google spreadsheet that works with this version of the loader
TESTED_DOC = "https://docs.google.com/spreadsheet/pub?key=0AiJoHeWBzmnAdDU2RF9RNl91WndSY25QSlU5cDAwZ1E&output=xls"
#
### while working on changes to the google doc, use this to run test_loader.py against the master spreadsheet
#TESTED_DOC=MASTER_DOC

# URL of the mapping spreadsheet for OOI assets
OOI_MAPPING_DOC = "https://docs.google.com/spreadsheet/pub?key=0AttCeOvLP6XMdFVUeDdoUTU0b0NFQ1dCVDhuUjY0THc&output=xls"

# The preload spreadsheets (tabs) in the order they should be loaded
DEFAULT_CATEGORIES = [
    'Constraint',                       # in memory only - all scenarios loaded
    'Contact',                          # in memory only - all scenarios loaded
    'User',
    'Org',
    #'Policy',
    'UserRole',                         # no resource - association only
    'CoordinateSystem',                 # in memory only - all scenarios loaded
    'ParameterFunctions',
    'ParameterDefs',
    'ParameterDictionary',
    'Alerts',                           # in memory only - all scenarios loaded
    'StreamConfiguration',              # in memory only - all scenarios loaded
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
    'InstrumentAgent',
    'InstrumentDevice',
    'ExternalDataProvider',
    'ExternalDatasetModel',
    'ExternalDataset',
    'ExternalDatasetAgent',
    'ExternalDatasetAgentInstance',
    'InstrumentAgentInstance',
    'DataProduct',
    'TransformFunction',
    'DataProcessDefinition',
    'DataProcess',
    'Parser',
    'Attachment',
    'DataProductLink',                  # no resource but complex service call
    'WorkflowDefinition',
    'Workflow',
    'Deployment',
    'Scheduler',
    ]

# The following lists all categories that define information used by other categories.
# A definition in these categories has no persistent side effect on the system.
DEFINITION_CATEGORIES = [
    'Constraint',
    'Contact',
    'CoordinateSystem',
    'Alerts',
    'StreamConfiguration',
]

# The following lists the scenarios that are always ignored
IGNORE_SCENARIOS = ["", "DOC", "DOC:README", "STOP!", "X"]

COL_SCENARIO = "Scenario"
COL_ID = "ID"
COL_OWNER = "owner_id"
COL_LCSTATE = "lcstate"
COL_ORGS = "org_ids"

ID_ORG_ION = "ORG_ION"
ID_SYSTEM_ACTOR = "USER_SYSTEM"
ID_WEB_AUTH_ACTOR = "USER_WEB_AUTH"

UUID_RE = '^[0-9a-fA-F]{32}$'

class IONLoader(ImmediateProcess):

    def __init__(self, *a, **b):
        super(IONLoader, self).__init__(*a,**b)

        # initialize these here instead of on_start
        # to support using IONLoader as a utility -- not just as a process
        self.obj_classes = {}     # Cache of class for object types
        self.resource_ids = {}    # Holds a mapping of preload IDs to internal resource ids
        self.resource_objs = {}   # Holds a mapping of preload IDs to the actual resource objects
        self.existing_resources = None
        self.unknown_fields = {} # track unknown fields so we only warn once
        self.constraint_defs = {} # alias -> value for refs, since not stored in DB
        self.contact_defs = {} # alias -> value for refs, since not stored in DB
        self.stream_config = {} # name -> obj for StreamConfiguration objects, used by *AgentInstance
        self.alerts = {} # id -> alert definition dict
        self.external_dataset_producer_id = {} # keep producer ID for later use by AgentInstance
        self.object_definitions = None

        # process to use for RPC communications (override to use as utility, default to use as process)
        self.rpc_sender = self

    def on_start(self):
        cfg = self.CFG.get("cfg", None)
        if cfg:
            log.warn("ION loader scripted mode using config file: %s", cfg)
            self.preload_cfg = Config([cfg]).data
            load_sequence = self.preload_cfg["load_sequence"]
            for num, step_cfg in enumerate(load_sequence):
                log.info("Executing preload step %s '%s'", num, step_cfg['name'])
                docstr = step_cfg.get("docstring", None)
                if docstr:
                    log.debug("Explanation: "+ docstr)
                step_config_override = step_cfg.get("config", {})
                log.debug("Step config override: %s", step_config_override)
                step_config = copy.deepcopy(self.CFG)
                dict_merge(step_config, step_config_override, inplace=True)
                self._do_preload(step_config)
                log.info("-------------------------- Completed step '%s' --------------------------", step_cfg['name'])
        else:
            self.preload_cfg = None
            self._do_preload(self.CFG)

    def _do_preload(self, config):
        """
        One "run" of preload with one set of config arguments.
        """

        # Main operation to perform this run.
        op = config.get("op", None)

        # Additional parameters
        self.path = config.get("path", None) or TESTED_DOC # handle case where path is explicitly set to None
        if self.path=='master':
            self.path = MASTER_DOC
        self.attachment_path = config.get("attachments", self.path + '/attachments')
        self.asset_path = config.get("assets", self.path + "/ooi_assets")
        default_ui_path = self.path if self.path.startswith('http') else self.path + "/ui_assets"

        self.assetmapping_path = config.get("assetmappings", OOI_MAPPING_DOC)

        self.ui_path = config.get("ui_path", default_ui_path)
        if self.ui_path=='default':
            self.ui_path = TESTED_UI_ASSETS
        elif self.ui_path=='candidate':
            self.ui_path = CANDIDATE_UI_ASSETS

        self.debug = config.get("debug", False)        # Debug mode with certain shorthands
        self.ooiuntil = config.get("ooiuntil", "9/1/2013")  # Don't import stuff later than given date
        if self.ooiuntil:
            self.ooiuntil = datetime.datetime.strptime(self.ooiuntil, "%m/%d/%Y")
        self.exportui = config.get("exportui", False)  # Save UI JSON file

        # External loader tools
        self.ui_loader = UILoader(self)
        self.ooi_loader = OOILoader(self, asset_path=self.asset_path, mapping_path=self.assetmapping_path)
        self.resource_ds = DatastoreManager.get_datastore_instance(DataStore.DS_RESOURCES, DataStore.DS_PROFILE.RESOURCES)

        log.info("IONLoader: {op=%s, path=%s}", op, self.path)
        if not op:
            raise iex.BadRequest("No operation specified")

        # Perform operations
        if op == "load":
            scenarios = config.get("scenario", None)
            if not scenarios:
                raise iex.BadRequest("Must provide scenarios to load: scenario=sc1,sc2,...")
            log.debug("Scenarios: %s", scenarios)

            category_csv = config.get("categories", None)
            self.categories = category_csv.split(",") if category_csv else DEFAULT_CATEGORIES

            self.loadooi = config.get("loadooi", False)    # Import OOI asset data
            self.loadui = config.get("loadui", False)      # Import UI asset data
            self.update = config.get("update", False)      # Support update to existing resources
            self.bulk = config.get("bulk", False)          # Use bulk insert where available
            self.ooifilter = config.get("ooifilter", None) # Filter OOI import to RD prefixes (e.g. array "CE,GP")
            self.ooiexclude = config.get("ooiexclude", '') # Don't import the listed categories
            if self.ooiexclude:
                self.ooiexclude = self.ooiexclude.split(',')

            if self.loadooi:
                self.ooi_loader.extract_ooi_assets()
                self.ooi_loader.analyze_ooi_assets(self.ooiuntil)
            if self.loadui:
                specs_path = 'interface/ui_specs.json' if self.exportui else None
                self.ui_loader.load_ui(self.ui_path, specs_path=specs_path)

            # Loads internal bootstrapped resource ids that will be referenced during preload
            self._load_system_ids()

            # Load existing resources by preload ID
            self._prepare_incremental()

            scenarios = scenarios.split(',')
            self.load_ion(scenarios)

        elif op == "parseooi":
            self.ooi_loader.extract_ooi_assets()
            self.ooi_loader.analyze_ooi_assets(self.ooiuntil)
            self.ooi_loader.report_ooi_assets()

        elif op == "deleteooi":
            if self.debug:
                self.ooi_loader.delete_ooi_assets()
            else:
                raise iex.BadRequest("deleteooi not allowed if debug==False")

        elif op == "loadui":
            specs_path = 'interface/ui_specs.json' if self.exportui else None
            self.ui_loader.load_ui(self.ui_path, specs_path=specs_path)

        elif op == "deleteui":
            self.ui_loader.delete_ui()

        else:
            raise iex.BadRequest("Operation unknown: %s" % op)

    def on_quit(self):
        pass

    def _read_and_parse(self, scenarios):
        """ read data records from appropriate source and extract usable rows,
            complete all IO and parsing -- save only a dict[by category] of lists[usable rows] of dicts[by columns]
        """

        # support use-case:
        #   x = IonLoader()
        #   x.object_definitions = my_funky_dict_of_lists_of_dicts
        #   x.load_ion()
        #
        if self.object_definitions:
            log.info("Object definitions already provided, NOT loading from path")
        else:
            # but here is the normal, expected use-case
            #
            log.info("Loading preload data from: %s", self.path)

            # Fetch the spreadsheet directly from a URL (from a GoogleDocs published spreadsheet)
            if self.path.startswith('http'):
                self._read_http(scenarios)
            elif self.path.endswith(".xlsx"):
                self._read_xls_file(scenarios)
            else:
                self._read_csv_files(scenarios)

    def _read_http(self, scenarios):
        """ read from google doc or similar HTTP XLS document """
        self.object_definitions = None
        for attempt in xrange(HTTP_RETRIES):
            length = 0
            try:
                contents = requests.get(self.path).content
                length = len(contents)
                self._parse_xls(contents, scenarios)
                break
            except:
                log.warn("Failed to parse preload document (read %d bytes)", length, exc_info=True)
        if not self.object_definitions:
            raise iex.BadRequest("failed to read and parse URL %d times" % HTTP_RETRIES)
        log.debug("Read and parsed URL (%d bytes)", length)

    def _read_xls_file(self, scenarios):
        """ read from XLS file """
        with open(self.path, "rb") as f:
            contents = f.read()
            log.debug("Loaded xlsx file, size=%s", len(contents))
            self._parse_xls(contents, scenarios)

    def _parse_xls(self, contents, scenarios):
        """ handle XLS parsing for http or file """
        csv_docs = XLSParser().extract_csvs(contents)
        self.object_definitions = {}
        for category in self.categories:
            reader = csv.DictReader(csv_docs[category], delimiter=',')
            self.object_definitions[category] = self._select_rows(reader, category, scenarios)

    def _read_csv_files(self,scenarios):
        """ read CSV files """
        self.object_definitions = {}
        for category in self.categories:
            filename = "%s/%s.csv" % (self.path, category)
            with open(filename, "rb") as f:
                reader = csv.DictReader(f, delimiter=',')
                self.object_definitions[category] = self._select_rows(reader, category, scenarios)

    def _select_rows(self, reader, category, scenarios):
        """ select subset of rows applicable to scenario """
        row_skip = row_do = 0
        rows = []
        for row in reader:
            if (category in DEFINITION_CATEGORIES and any(sc not in IGNORE_SCENARIOS for sc in row[COL_SCENARIO].split(","))) \
                or any(sc in scenarios for sc in row[COL_SCENARIO].split(",")):
                row_do += 1
                rows.append(row)
            else:
                row_skip += 1
                if COL_ID in row:
                    log.trace('skipping %s row %s in scenario %s', category, row[COL_ID], row[COL_SCENARIO])
                else:
                    log.trace('skipping %s row in scenario %s: %r', category, row[COL_SCENARIO], row)
        log.debug('parsed entries for category %s: using %d rows, skipping %d rows', category, row_do, row_skip)
        return rows

    def _load_system_ids(self):
        """Read some system objects for later reference"""
        org_objs,_ = self.container.resource_registry.find_resources(name="ION", restype=RT.Org, id_only=False)
        if not org_objs:
            raise iex.BadRequest("ION org not found. Was system force_cleaned since bootstrap?")
        ion_org_id = org_objs[0]._id
        self._register_id(ID_ORG_ION, ion_org_id, org_objs[0])

        system_actor, _ = self.container.resource_registry.find_resources(
            RT.ActorIdentity, name=self.CFG.system.system_actor, id_only=False)
        system_actor_id = system_actor[0]._id if system_actor else 'anonymous'
        self._register_id(ID_SYSTEM_ACTOR, system_actor_id, system_actor[0] if system_actor else None)

        webauth_actor, _ = self.container.resource_registry.find_resources(
            RT.ActorIdentity, name=self.CFG.get_safe("system.web_authentication_actor", "web_authentication"), id_only=False)
        webauth_actor_id = webauth_actor[0]._id if webauth_actor else 'anonymous'
        self._register_id(ID_WEB_AUTH_ACTOR, webauth_actor_id, webauth_actor[0] if webauth_actor else None)

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

    # -------------------------------------------------------------------------

    def load_ion(self, scenarios):
        """
        Loads resources for one scenario, by parsing input spreadsheets for all resource categories
        in defined order, executing service calls for all entries in the scenario.
        Can load the spreadsheets from http or file location.
        Optionally imports OOI assets at the beginning of each category.
        """
        if self.debug:
            log.warn("WARNING: Debug==True. Certain shortcuts will be taken for easier development")
        if self.bulk:
            log.warn("WARNING: Bulk load is ENABLED. Making bulk RR calls to create resources/associations. No policy checks!")
        if self.loadooi and self.ooiuntil:
            log.warn("WARNING: Loading OOI assets only until %s cutoff date!", self.ooiuntil)

        # read everything ahead of time, not on the fly
        # that way if the Nth CSV is garbled, you don't waste time preloading the other N-1
        # before you see an error
        self._read_and_parse(scenarios)

        count = len(self.categories)
        index = 0
        for category in self.categories:
            t = Timer() if stats.is_log_enabled() else None
            index += 1
            self.bulk_objects = {}      # This keeps objects to be bulk inserted/updated at the end of a category

            # First load all OOI assets for this category
            if self.loadooi and category not in self.ooiexclude:
                catfunc_ooi = getattr(self, "_load_%s_OOI" % category, None)
                if catfunc_ooi:
                    log.debug('Loading OOI assets for %s', category)
                    catfunc_ooi()
                if t:
                    t.complete_step('preload.%s.catfunc' % category)
            # Now load entries from preload spreadsheet top to bottom where scenario matches
            if category not in self.object_definitions or not self.object_definitions[category]:
                log.debug('no rows for category: %s', category)

            for row in self.object_definitions.get(category, []):
                if COL_ID in row:
                    log.trace('handling %s row %s: %r', category, row[COL_ID], row)
                else:
                    log.trace('handling %s row: %r', category, row)

                try:
                    self.load_row(category, row)
                except:
                    log.error('error loading %s row: %r', category, row, exc_info=True)
                    raise

            row_count = len(self.object_definitions.get(category, []))
            if t:
                t.complete_step('preload.%s.load_row'%category)
            if self.bulk:
                num_bulk = self._finalize_bulk(category)
                # Update resource and associations views
                self.container.resource_registry.find_resources(restype="X", id_only=True)
                self.container.resource_registry.find_associations(predicate="X", id_only=True)
                # should we assert that num_bulk==row_count??
                log.info("bulk loaded category %s: %d rows, %s bulk", category, row_count, num_bulk)
                if t:
                    t.complete_step('preload.%s.bulk_load'%category)
            else:
                log.info("loaded category %s (%d/%d): %d rows", category, index, count, row_count)
            if t:
                stats.add(t)
                stats.add_value('preload.%s.row_count'%category, row_count)

    def load_row(self, type, row):
        """ expose for use by utility function """
        func = getattr(self, "_load_%s" % type)
        func(row)

    def _finalize_bulk(self, category):
        res = self.resource_ds.create_mult(self.bulk_objects.values(), allow_ids=True)
        log.debug("Bulk stored %d resource objects/associations into resource registry", len(res))
        num_objects = len([1 for obj in self.bulk_objects.values() if obj.type_ != "Association"])
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
                            fieldvalue = get_typed_value(value, schema[fieldname])
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
            if existing_obj.type_ != objtype:
                raise iex.Inconsistent("Cannot edit resource. Type mismatch old=%s, new=%s" % (existing_obj.type_, objtype))
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
            if COL_ID in row and row[COL_ID] and 'alt_ids' in schema:
                if 'alt_ids' in obj_fields:
                    obj_fields['alt_ids'].append("PRE:"+row[COL_ID])
                else:
                    obj_fields['alt_ids'] = ["PRE:"+row[COL_ID]]

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

    def _get_service_client(self, service):
        return get_service_registry().services[service].client(process=self.rpc_sender)

    def _register_id(self, alias, resid, res_obj=None):
        """Keep preload resource in internal dict for later reference"""
        if alias in self.resource_ids:
            raise iex.BadRequest("ID alias %s used twice" % alias)
        self.resource_ids[alias] = resid
        self.resource_objs[alias] = res_obj
        log.trace("Added resource alias=%s to id=%s", alias, resid)

    def _read_resource_id(self, res_id):
        existing_obj = self.container.resource_registry.read(res_id)
        self.resource_objs[res_id] = existing_obj
        self.resource_ids[res_id] = res_id
        return existing_obj

    def _get_resource_id(self, alias_id):
        """Returns resource ID from preload alias ID, scanning also for real resource IDs to be loaded"""
        if alias_id in self.resource_ids:
            return self.resource_ids[alias_id]
        elif re.match(UUID_RE, alias_id):
            # This is obviously an ID of a real resource - let it fail if not existing
            self._read_resource_id(alias_id)
            log.debug("Referencing existing resource via direct ID: %s", alias_id)
            return alias_id
        else:
            raise KeyError(alias_id)

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

        log.debug("_get_resource_obj(): No object found for '%s'", res_id)
        return None

    def _get_alt_id(self, res_obj, prefix):
        alt_ids = getattr(res_obj, 'alt_ids', [])
        for alt_id in alt_ids:
            if alt_id.startswith(prefix+":"):
                alt_id_str = alt_id[len(prefix)+1:]
                return alt_id_str

    def _get_op_headers(self, row, force_user=False):
        headers = {}
        owner_id = row.get(COL_OWNER, None)
        if owner_id:
            owner_id = self.resource_ids[owner_id]
            headers['ion-actor-id'] = owner_id
            headers['ion-actor-roles'] = {'ION': ['ION_MANAGER', 'ORG_MANAGER']}
            headers['expiry'] = '0'
        elif force_user:
            return self._get_system_actor_headers()
        return headers

    def _get_system_actor_headers(self):
        return {'ion-actor-id': self.resource_ids[ID_SYSTEM_ACTOR],
                'ion-actor-roles': {'ION': ['ION_MANAGER', 'ORG_MANAGER']},
                'expiry':'0'}

    def _get_webauth_actor_headers(self):
        return {'ion-actor-id': self.resource_ids[ID_WEB_AUTH_ACTOR],
                'ion-actor-roles': {'ION': ['ION_MANAGER', 'ORG_MANAGER']},
                'expiry':'0'}

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

    def _before_cutoff(self, ooi_obj):
        """Indicates whether a given OOI parsed asset is first used before a cutoff date"""
        deploy_date = ooi_obj.get("deploy_date", None)
        if self.ooiuntil and (not deploy_date or deploy_date > self.ooiuntil):
            return False
        else:
            return True

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
        res_id_alias = row[COL_ID]
        existing_obj = None
        if res_id_alias in self.resource_ids:
            # TODO: Catch case when ID used twice
            existing_obj = self.resource_objs[res_id_alias]
        elif re.match(UUID_RE, res_id_alias):
            # This is obviously an ID of a real resource
            try:
                existing_obj = self._read_resource_id(res_id_alias)
                log.debug("Updating existing resource via direct ID: %s", res_id_alias)
            except NotFound as nf:
                pass  # Ok it was not there after all

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
        initial_lcmat = lcsm.initial_state if lcsm else LCS.DEPLOYED
        initial_lcav = lcsm.initial_availability if lcsm else AS.AVAILABLE

        lcstate = row.get(COL_LCSTATE, None)
        if lcstate:
            row_lcmat, row_lcav = lcstate.split("_", 1)
            if self.bulk and res_id in self.bulk_objects:
                self.bulk_objects[res_id].lcstate = row_lcmat
                self.bulk_objects[res_id].availability = row_lcav
            else:
                if row_lcmat != initial_lcmat:    # Vertical transition
                    self.container.resource_registry.set_lifecycle_state(res_id, row_lcmat)
                if row_lcav != initial_lcav:      # Horizontal transition
                    self.container.resource_registry.set_lifecycle_state(res_id, row_lcav)
        elif self.bulk and res_id in self.bulk_objects:
            # Set the lcs to resource type appropriate initial values
            self.bulk_objects[res_id].lcstate = initial_lcmat
            self.bulk_objects[res_id].availability = initial_lcav

    def _resource_assign_org(self, row, res_id):
        """
        Shares the resource in the given orgs. Supports bulk.
        """
        org_ids = row.get(COL_ORGS, None)
        if org_ids:
            org_ids = get_typed_value(org_ids, targettype="simplelist")
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
            names = get_typed_value(value, targettype="simplelist")
            if names:
                for name in names:
                    if name not in value_map:
                        msg = 'invalid ' + member_type + ': ' + name + ' (from value=' + value + ')'
                        if COL_ID in row:
                            msg = 'id ' + row[COL_ID] + ' refers to an ' + msg
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
            st = subject.type_

            if "_id" not in obj:
                raise iex.BadRequest("Object id not available")
            object_id = obj._id
            ot = obj.type_

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
        """
        DEFINITION category. Load and keep IonObject for reference by other categories. No side effects.
        Keeps contact information objects.
        """
        cont_id = row[COL_ID]
        log.trace('creating contact: ' + cont_id)
        if cont_id in self.contact_defs:
            raise iex.BadRequest('contact with ID already exists: ' + cont_id)

        roles = get_typed_value(row['c/roles'], targettype='simplelist')
        del row['c/roles']
        phones = parse_phones(row['c/phones'])
        del row['c/phones']

        contact = self._create_object_from_row("ContactInformation", row, "c/")
        contact.roles = roles
        contact.phones = phones

        self.contact_defs[cont_id] = contact

    def _load_Contact_OOI(self):
        if self.debug:
            controw = {}
            controw[COL_ID] = "ORG_CONTACT"
            controw["c/individual_names_given"] = "Mike"
            controw["c/individual_name_family"] = "Manager"
            controw["c/organization_name"] = "UNoIt"
            controw["c/position_name"] = "Manager"
            controw["c/email"] = "mikemanagerooi@gmail.com"
            controw["c/roles"] = "primary"
            controw["c/phones"] = "619-555-1212"
            self._load_Contact(controw)

    def _load_Constraint(self, row):
        """
        DEFINITION category. Load and keep IonObject for reference by other categories. No side effects.
        Keeps geospatial/temporal constraints
        """
        const_id = row[COL_ID]
        if const_id in self.constraint_defs:
            raise iex.BadRequest('constraint with ID already exists: ' + const_id)
        const_type = row['type']
        if const_type == 'geospatial' or const_type == 'geo' or const_type == 'space':
            self.constraint_defs[const_id] = self._create_geospatial_constraint(row)
        elif const_type == 'temporal' or const_type == 'temp' or const_type == 'time':
            self.constraint_defs[const_id] = self._create_temporal_constraint(row)
        else:
            raise iex.BadRequest('constraint type must be either geospatial or temporal, not ' + const_type)

    def _load_CoordinateSystem(self, row):
        """
        DEFINITION category. Load and keep IonObject for reference by other categories. No side effects.
        Keeps coordinate system definition objects.
        """
        gcrs = self._create_object_from_row("GeospatialCoordinateReferenceSystem", row, "m/")
        cs_id = row[COL_ID]
        self.resource_ids[cs_id] = gcrs

    def _load_CoordinateSystem_OOI(self):
        newrow = {}
        newrow[COL_ID] = 'OOI_SUBMERGED_CS'
        newrow['m/geospatial_geodetic_crs'] = 'http://www.opengis.net/def/crs/EPSG/0/4326'
        newrow['m/geospatial_vertical_crs'] = 'http://www.opengis.net/def/cs/EPSG/0/6498'
        newrow['m/geospatial_latitude_units'] = 'degrees_north'
        newrow['m/geospatial_longitude_units'] = 'degrees_east'
        newrow['m/geospatial_vertical_units'] = 'meter'
        newrow['m/geospatial_vertical_positive'] = 'down'

        self._load_CoordinateSystem(newrow)

    def _create_geospatial_constraint(self, row):
        z = row['vertical_direction']
        if z == 'depth':
            vmin = float(row['top'])
            vmax = float(row['bottom'])
        elif z == 'elevation':
            vmin = float(row['bottom'])
            vmax = float(row['top'])
        else:
            raise iex.BadRequest('vertical_direction must be "depth" or "elevation", not ' + z)
        constraint = IonObject("GeospatialBounds",
                               geospatial_latitude_limit_north=float(row['north']),
                               geospatial_latitude_limit_south=float(row['south']),
                               geospatial_longitude_limit_east=float(row['east']),
                               geospatial_longitude_limit_west=float(row['west']),
                               geospatial_vertical_min=vmin,
                               geospatial_vertical_max=vmax)
        return constraint

    def _create_temporal_constraint(self, row):
        format = row['time_format'] or DEFAULT_TIME_FORMAT
        start = str(calendar.timegm(time.strptime(row['start'], format)))
        end = str(calendar.timegm(time.strptime(row['end'], format)))
        return IonObject("TemporalBounds", start_datetime=start, end_datetime=end)


    def _load_Policy(self, row):
        if not self.CFG.get_safe("system.load_policy", False):
            return

        policy_obj = self._create_object_from_row("Policy", row, "p/")

        pms_client = self._get_service_client("policy_management")
        #headers = self._get_op_headers(row)

        policy_type	= row['policy_type']
        preconditions = row['preconditions']
        policy_rule	= row['policy_rule']
        service_name = row['service_name']
        process_name = row['process_name']
        resource_id = row.get('resource_id', None)

        # Create various types of policy
        if policy_type == "CommonServiceAccessPolicy":
            policy_id = pms_client.create_common_service_access_policy(
                policy_name=policy_obj.name,
                description=policy_obj.description,
                policy_rule=policy_rule or policy_obj.definition,
                headers=self._get_system_actor_headers())

        elif policy_type == "ServiceAccessPolicy" and service_name:
            policy_id = pms_client.create_service_access_policy(
                service_name=service_name,
                policy_name=policy_obj.name,
                description=policy_obj.description,
                policy_rule=policy_rule or policy_obj.definition,
                headers=self._get_system_actor_headers())

        elif policy_type == "ProcessOperationPreconditionPolicy" and process_name:
            policy_id = pms_client.add_process_operation_precondition_policy(
                process_name=process_name,
                op="",
                preconditions=preconditions,
                headers=self._get_system_actor_headers())

        elif policy_type == "OperationPreconditionPolicy":
            policy_id = pms_client.add_operation_precondition_policy(
                process_name=process_name,
                op="",
                preconditions=preconditions,
                headers=self._get_system_actor_headers())

        elif policy_type == "ResourceAccessPolicy" and resource_id:
            policy_id = pms_client.create_resource_access_policy(
                resource_id=resource_id,
                policy_name=policy_obj.name,
                description=policy_obj.description,
                policy_rule=policy_rule or policy_obj.definition,
                headers=self._get_system_actor_headers())

        elif policy_type == "Policy":
            policy_id = pms_client.create_policy(
                policy=policy_obj,
                headers=self._get_system_actor_headers())

        self._register_id(row[COL_ID], policy_id, policy_obj)

        # If needed for policy:
        #self._resource_assign_org(row, policy_id)


    def _load_User(self, row):
        alias = row[COL_ID]
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

        headers = self._get_webauth_actor_headers()

        if alias in self.resource_ids or re.match(UUID_RE, alias):
            # Update cases
            if alias in self.resource_ids:
                actor_obj = self.resource_objs[alias]
            else:
                actor_obj = self._read_resource_id(alias)
            actor_id = alias

            # Update UserInfo etc
            user_info_obj = ims.find_user_info_by_id(actor_id)

            # Add credentials
            if subject:
                uc_list,_ = self.container.resource_registry.find_resources(RT.UserCredentials, None, name=subject, id_only=True)
                if not uc_list:
                    # Add a new credentials set
                    # TODO: Delete the old credentials?
                    user_credentials_obj = IonObject("UserCredentials", name=subject,
                                                     description="Default credentials for %s" % user_info_obj.name)
                    ims.register_user_credentials(actor_id, user_credentials_obj, headers=headers)

        else:
            # Build ActorIdentity
            actor_name = "Identity for %s" % user_attrs['name']
            actor_identity_obj = IonObject("ActorIdentity", name=actor_name, alt_ids=["PRE:"+alias])
            log.trace("creating user %s with headers: %r", user_attrs['name'], headers)
            actor_id = ims.create_actor_identity(actor_identity_obj, headers=headers)
            actor_identity_obj._id = actor_id
            self._register_id(alias, actor_id, actor_identity_obj)

            # Build UserCredentials
            if subject:
                user_credentials_obj = IonObject("UserCredentials", name=subject,
                    description="Default credentials for %s" % user_attrs['name'])
                ims.register_user_credentials(actor_id, user_credentials_obj, headers=headers)

            # Build UserInfo
            user_info_obj = IonObject("UserInfo", **user_attrs)
            ims.create_user_info(actor_id, user_info_obj, headers=headers)

    def _load_User_OOI(self):
        if self.debug:
            if not self._get_resource_obj("USER_1"):
                userrow = {}
                userrow["ID"] = "USER_1"
                userrow["subject"] = "/DC=org/DC=cilogon/C=US/O=Google/CN=Owen Ownerrep A893"
                userrow["name"] = "Owen Ownerrep"
                userrow["description"] = "Demonstration User"
                userrow["contact_id"] = "ORG_CONTACT"
                self._load_User(userrow)

    def _load_Org(self, row):
        log.trace("Loading Org (ID=%s)", row[COL_ID])
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
            raise iex.BadRequest("Unknown Org type: %s" % org_type)

        res_obj._id = res_id
        self._register_id(row[COL_ID], res_id, res_obj)

    def _load_Org_OOI(self):
        ooi_orgs = [
            {"ID":"MF_RSN", "owner_id":"USER_1", "org_type":"MarineFacility",
             "org/name":"RSN Facility", "org/org_governance_name": "RSN",
             "org/description":"Marine Infrastructure managed by RSN Marine IO",
             "org/institution/name":"Univ. of Washington", "contact_id":"ORG_CONTACT"},
            {"ID":"MF_CGSN", "owner_id":"USER_1", "org_type":"MarineFacility",
             "org/name":"CGSN Facility", "org/org_governance_name": "CGSN",
             "org/description":"Marine Infrastructure managed by CGSN Marine IO",
             "org/institution/name":"Woods Hole Oceanographic Institution", "contact_id":"ORG_CONTACT"},
            {"ID":"MF_EA", "owner_id":"USER_1", "org_type":"MarineFacility",
             "org/name":"EA Facility", "org/org_governance_name": "EA",
             "org/description":"Marine Infrastructure managed by EA Marine IO",
             "org/institution/name":"Oregon State University Institution", "contact_id":"ORG_CONTACT"},
        ]
        if self.debug:
            for org in ooi_orgs:
                if not self._get_resource_obj(org[COL_ID]):
                    self._load_Org(org)

    def _load_UserRole(self, row):
        org_id = row["org_id"]
        if org_id:
            org_id = self.resource_ids[org_id]

        user_id = self._get_resource_id(row["user_id"])   # Accepts a non-preloaded resource as well
        role_name = row["role_name"]
        svc_client = self._get_service_client("org_management")

        if get_typed_value(row['auto_enroll'], targettype="bool"):
            svc_client.enroll_member(org_id, user_id, headers=self._get_system_actor_headers())

        if role_name != "ORG_MEMBER":
            svc_client.grant_role(org_id, user_id, role_name, headers=self._get_system_actor_headers())

    def _load_PlatformModel(self, row):
        self._basic_resource_create(row, "PlatformModel", "pm/",
            "instrument_management", "create_platform_model",
            support_bulk=True)

    def _load_PlatformModel_OOI(self):
        ooi_objs = self.ooi_loader.get_type_assets("nodetype")

        for ooi_id, ooi_obj in ooi_objs.iteritems():
            if not self._before_cutoff(ooi_obj):
                continue

            newrow = {}
            newrow[COL_ID] = ooi_id + "_PM"
            newrow['pm/name'] = ooi_obj['name']
            newrow['pm/description'] = "Node Type: %s" % ooi_id
            newrow['pm/alt_ids'] = "['OOI:" + ooi_id + "_PM" + "']"
            newrow['org_ids'] = self.ooi_loader.get_org_ids(ooi_obj.get('array_list', None))

            if not self._match_filter(ooi_obj.get('array_list', None)):
                continue

            self._load_PlatformModel(newrow)

    def _load_InstrumentModel(self, row):
        row['im/reference_urls'] = repr(get_typed_value(row['im/reference_urls'], targettype="simplelist"))
        self._basic_resource_create(row, "InstrumentModel", "im/",
            "instrument_management", "create_instrument_model",
            support_bulk=True)

    def _load_InstrumentModel_OOI(self):
        class_objs = self.ooi_loader.get_type_assets("class")
        series_objs = self.ooi_loader.get_type_assets("series")
        subseries_objs = self.ooi_loader.get_type_assets("subseries")
        family_objs = self.ooi_loader.get_type_assets("family")
        makemodel_objs = self.ooi_loader.get_type_assets("makemodel")

        for ooi_id, series_obj in series_objs.iteritems():
            class_obj = class_objs[series_obj['Class']]
            class_name = class_obj["name"]
            if "DEPRECATED" in class_name:
                continue
            if not self._before_cutoff(series_obj):
                continue

            family_obj = family_objs[class_obj['family']]
            makemodel_obj = makemodel_objs[series_obj['makemodel']] if series_obj.get('makemodel', None) else None
            subseries_obj = subseries_objs.get(ooi_id + "01", None)
            newrow = {}
            newrow[COL_ID] = ooi_id
            newrow['im/name'] = "%s (%s-%s)" % (class_name, series_obj['Class'], series_obj['Series'])
            newrow['im/alt_ids'] = "['OOI:" + ooi_id + "']"
            newrow['im/description'] = series_obj['description']
            newrow['im/instrument_family'] = family_obj['name']   # DEPRECATED. Remove when UI db updated.
            newrow['im/family_id'] = family_obj['id']
            newrow['im/family_name'] = family_obj['name']
            newrow['im/class_id'] = class_obj['id']
            newrow['im/class_name'] = class_obj['name']
            newrow['im/class_alternate_name'] = class_obj['Alternate Instrument Class Name']
            newrow['im/class_description'] = class_obj['description']
            newrow['im/series_id'] = series_obj['id']
            newrow['im/series_name'] = series_obj['name']
            newrow['im/subseries_id'] = subseries_obj['id'] if subseries_obj else ""
            newrow['im/subseries_name'] = subseries_obj['name'] if subseries_obj else ""
            newrow['im/configuration'] = subseries_obj['Instrument Configuration'] if subseries_obj else ""
            newrow['im/ooi_make_model'] = makemodel_obj['name'] if makemodel_obj else ""
            newrow['im/manufacturer'] = makemodel_obj['Manufacturer'] if makemodel_obj else ""
            newrow['im/manufacturer_url'] = makemodel_obj['Vendor Website'] if makemodel_obj else ""
            newrow['im/reference_designator'] = ooi_id
            newrow['org_ids'] = self.ooi_loader.get_org_ids(series_obj.get('array_list', None))
            reference_urls = []
            addl = {}
            if makemodel_obj:
                addl.update(dict(connector=makemodel_obj['Connector'],
                    makemodel_description=makemodel_obj['Make_Model_Description'],
                    input_voltage_range=makemodel_obj['Input Voltage Range'],
                    interface=makemodel_obj['Interface'],
                    output_description=makemodel_obj['Output Description'],
                ))
                if makemodel_obj['Make/Model Website']: reference_urls.append(makemodel_obj['Make/Model Website'])
            newrow['im/reference_urls'] = ",".join(reference_urls)
            addl['class_long_name'] = series_obj['ClassLongName']
            addl['comments'] = series_obj['Comments']
            newrow['im/addl'] = repr(addl)

            if not self._match_filter(class_obj.get('array_list', None)):
                continue

            self._load_InstrumentModel(newrow)

    def _calc_geospatial_point_center(self, site):
        siteTypes = [RT.Site, RT.Subsite, RT.Observatory, RT.PlatformSite, RT.InstrumentSite, RT.Deployment]
        if site and site.type_ in siteTypes:
            # if the geospatial_bounds is set then calculate the geospatial_point_center
            for constraint in site.constraint_list:
                if constraint.type_ == OT.GeospatialBounds:
                    site.geospatial_point_center = GeoUtils.calc_geospatial_point_center(constraint)

    def _load_Observatory(self, row):
        constraints = self._get_constraints(row, type='Observatory')
        coordinate_name = row['coordinate_system']

        res_id = self._basic_resource_create(row, "Observatory", "obs/",
            "observatory_management", "create_observatory",
            constraints=constraints, constraint_field='constraint_list',
            set_attributes=dict(coordinate_reference_system=self.resource_ids[coordinate_name]) if coordinate_name else None,
            support_bulk=True)

        if self.bulk:
            fofr_obj = self._get_resource_obj(res_id)
            self._calc_geospatial_point_center(fofr_obj)

    def _load_Observatory_OOI(self):
        # Observatory resources are created from aggregate SAF subsite assets (i.e. one or
        # multiple SAF subsites make one Observatory).
        ooi_objs = self.ooi_loader.get_type_assets("ssite")
        for ooi_id, ooi_obj in ooi_objs.iteritems():
            constrow = {}
            const_id1 = ooi_obj['rd'] + "_const1"
            constrow[COL_ID] = const_id1
            constrow['type'] = 'geospatial'
            constrow['south'] = ooi_obj['lat_south'] or '0.0'
            constrow['north'] = ooi_obj['lat_north'] or '0.0'
            constrow['west'] = ooi_obj['lon_west'] or '0.0'
            constrow['east'] = ooi_obj['lon_east'] or '0.0'
            constrow['vertical_direction'] = 'depth'
            constrow['top'] = ooi_obj['depth_min'] or '0.0'
            constrow['bottom'] = ooi_obj['depth_max'] or '0.0'
            self._load_Constraint(constrow)

            subsite_rd_list = ooi_obj['subsite_rd_list']
            newrow = {}
            newrow[COL_ID] = ooi_obj['rd']
            newrow['obs/name'] = ooi_obj['name']
            newrow['obs/description'] = "Subsite: %s" % ", ".join(subsite_rd_list)
            newrow['obs/alt_ids'] = "['OOI:" + ooi_obj['rd'] + "']"
            newrow['obs/local_name'] = ooi_obj['local_name']
            newrow['obs/reference_designator'] = ooi_obj['rd']
            newrow['obs/spatial_area_name'] = ooi_obj['geo_area']
            newrow['constraint_ids'] = const_id1
            newrow['coordinate_system'] = 'OOI_SUBMERGED_CS'
            newrow['org_ids'] = self.ooi_loader.get_org_ids([ooi_obj['rd']])

            if not self._match_filter(ooi_obj['rd']):
                continue

            self._load_Observatory(newrow)

    def _load_Subsite(self, row):
        constraints = self._get_constraints(row, type='Subsite')
        coordinate_name = row['coordinate_system']

        res_id = self._basic_resource_create(row, "Subsite", "site/",
            "observatory_management", "create_subsite",
            constraints=constraints, constraint_field='constraint_list',
            set_attributes=dict(coordinate_reference_system=self.resource_ids[coordinate_name]) if coordinate_name else None,
            support_bulk=True)

        if self.bulk:
            fofr_obj = self._get_resource_obj(res_id)
            self._calc_geospatial_point_center(fofr_obj)

        headers = self._get_op_headers(row)
        psite_id = row.get("parent_site_id", None)
        if psite_id:
            if self.bulk:
                psite_obj = self._get_resource_obj(psite_id)
                site_obj = self._get_resource_obj(row[COL_ID])
                self._create_association(psite_obj, PRED.hasSite, site_obj)
            else:
                svc_client = self._get_service_client("observatory_management")
                svc_client.assign_site_to_site(res_id, self.resource_ids[psite_id],
                    headers=headers)

    def _load_Subsite_OOI(self):
        # Not needed for current OOI import. Only one level of geospatial site is used.
        pass

    def _load_PlatformSite(self, row):
        constraints = self._get_constraints(row, type='PlatformSite')
        coordinate_name = row['coordinate_system']

        res_id = self._basic_resource_create(row, "PlatformSite", "ps/",
            "observatory_management", "create_platform_site",
            constraints=constraints, constraint_field='constraint_list',
            set_attributes=dict(coordinate_reference_system=self.resource_ids[coordinate_name]) if coordinate_name else None,
            support_bulk=True)

        if self.bulk:
            fofr_obj = self._get_resource_obj(res_id)
            self._calc_geospatial_point_center(fofr_obj)

        svc_client = self._get_service_client("observatory_management")

        headers = self._get_op_headers(row)
        psite_id = row.get("parent_site_id", None)
        if psite_id:
            if self.bulk:
                psite_obj = self._get_resource_obj(psite_id)
                site_obj = self._get_resource_obj(row[COL_ID])
                self._create_association(psite_obj, PRED.hasSite, site_obj)
            else:
                svc_client.assign_site_to_site(res_id, self.resource_ids[psite_id],
                    headers=headers)

        pm_ids = row["platform_model_ids"]
        if pm_ids:
            pm_ids = get_typed_value(pm_ids, targettype="simplelist")
            for pm_id in pm_ids:
                if self.bulk:
                    model_obj = self._get_resource_obj(pm_id)
                    site_obj = self._get_resource_obj(row[COL_ID])
                    self._create_association(site_obj, PRED.hasModel, model_obj)
                else:
                    svc_client.assign_platform_model_to_platform_site(self.resource_ids[pm_id], res_id,
                        headers=headers)

    def _load_PlatformSite_OOI(self):
        subsite_objs = self.ooi_loader.get_type_assets("subsite")
        ssite_objs = self.ooi_loader.get_type_assets("ssite")

        def _load_platform(ooi_id, ooi_obj):
            if not self._before_cutoff(ooi_obj):
                return

            ooi_rd = OOIReferenceDesignator(ooi_id)

            const_id1 = ''
            if ooi_obj.get('latitude', None) or ooi_obj.get('longitude', None) or ooi_obj.get('depth_subsite', None):
                const_id1 = ooi_id + "_const1"
                constrow = {}
                constrow[COL_ID] = const_id1
                constrow['type'] = 'geospatial'
                constrow['south'] = ooi_obj['latitude'] or '0.0'
                constrow['north'] = ooi_obj['latitude'] or '0.0'
                constrow['west'] = ooi_obj['longitude'] or '0.0'
                constrow['east'] = ooi_obj['longitude'] or '0.0'
                constrow['vertical_direction'] = 'depth'
                constrow['top'] = ooi_obj['depth_subsite'] or '0.0'
                constrow['bottom'] = ooi_obj['depth_subsite'] or '0.0'
                self._load_Constraint(constrow)
            elif ooi_obj.get('is_platform', False):
                ss = subsite_objs[ooi_rd.subsite_rd]
                ss_mod = ssite_objs[ss['ssite']]
                const_id1 = ss_mod['rd'] + "_const1"
            else:
                ss = subsite_objs[ooi_obj.get('platform_id', '')[:8]]
                ss_mod = ssite_objs[ss['ssite']]
                const_id1 = ss_mod['rd'] + "_const1"

            newrow = {}
            newrow[COL_ID] = ooi_id
            newrow['ps/name'] = ooi_obj.get('name', ooi_id)
            newrow['ps/alt_ids'] = "['OOI:" + ooi_id + "']"
            newrow['ps/local_name'] = ooi_obj['local_name']
            newrow['ps/reference_designator'] = ooi_id
            newrow['constraint_ids'] = const_id1
            newrow['coordinate_system'] = 'OOI_SUBMERGED_CS'
            if ooi_obj.get('is_platform', False):
                # This is a top level platform (for a station)
                ss = subsite_objs[ooi_rd.subsite_rd]
                ss_mod = ssite_objs[ss['ssite']]
                newrow['parent_site_id'] = ss_mod['rd']
                newrow['ps/description'] = "Node (platform): %s" % ooi_id
                newrow['ps/alt_resource_type'] = "StationSite"
            else:
                newrow['parent_site_id'] = ooi_obj.get('platform_id', '')
                newrow['ps/description'] = "Node (child): %s" % ooi_id
                newrow['ps/alt_resource_type'] = "PlatformComponentSite"
            newrow['platform_model_ids'] = ooi_id[9:11] + "_PM"
            newrow['org_ids'] = self.ooi_loader.get_org_ids([ooi_id[:2]])

            uplink_node = ooi_obj.get('uplink_node', None)
            uplink_port = ooi_obj.get('uplink_port', None)
            if uplink_node and uplink_port:
                if uplink_port.startswith("X"):
                    newrow['ps/planned_uplink_port/port_type'] = "EXPANSION"
                    port_rd = "%s-0%s" % (uplink_node, uplink_port[1:])
                else:
                    newrow['ps/planned_uplink_port/port_type'] = "PAYLOAD"
                    port_rd = "%s-%s" % (uplink_node, uplink_port)
                newrow['ps/planned_uplink_port/reference_designator'] = port_rd

            if not self._match_filter(ooi_id[:2]):
                return

            self._load_PlatformSite(newrow)

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

        if self.bulk:
            fofr_obj = self._get_resource_obj(res_id)
            self._calc_geospatial_point_center(fofr_obj)

        svc_client = self._get_service_client("observatory_management")

        headers = self._get_op_headers(row)
        psite_id = row.get("parent_site_id", None)
        if psite_id:
            if self.bulk:
                psite_obj = self._get_resource_obj(psite_id)
                site_obj = self._get_resource_obj(row[COL_ID])
                self._create_association(psite_obj, PRED.hasSite, site_obj)
            else:
                svc_client.assign_site_to_site(res_id, self.resource_ids[psite_id],
                    headers=headers)

        im_ids = row["instrument_model_ids"]
        if im_ids:
            im_ids = get_typed_value(im_ids, targettype="simplelist")
            for im_id in im_ids:
                if self.bulk:
                    model_obj = self._get_resource_obj(im_id)
                    site_obj = self._get_resource_obj(row[COL_ID])
                    self._create_association(site_obj, PRED.hasModel, model_obj)
                else:
                    svc_client.assign_instrument_model_to_instrument_site(self.resource_ids[im_id], res_id,
                        headers=headers)

    def _load_InstrumentSite_OOI(self):
        inst_objs = self.ooi_loader.get_type_assets("instrument")
        node_objs = self.ooi_loader.get_type_assets("node")
        class_objs = self.ooi_loader.get_type_assets("class")
        series_objs = self.ooi_loader.get_type_assets("series")

        for inst_id, inst_obj in inst_objs.iteritems():
            ooi_rd = OOIReferenceDesignator(inst_id)
            node_obj = node_objs[ooi_rd.node_rd]
            if not self._before_cutoff(inst_obj) or not self._before_cutoff(node_obj):
                continue

            constrow = {}
            const_id1 = ''
            if inst_obj['latitude'] or inst_obj['longitude'] or inst_obj['depth_port_max'] or inst_obj['depth_port_min']:
                const_id1 = inst_id + "_const1"
                constrow[COL_ID] = const_id1
                constrow['type'] = 'geospatial'
                constrow['south'] = inst_obj['latitude'] or '0.0'
                constrow['north'] = inst_obj['latitude'] or '0.0'
                constrow['west'] = inst_obj['longitude'] or '0.0'
                constrow['east'] = inst_obj['longitude'] or '0.0'
                constrow['vertical_direction'] = 'depth'
                constrow['top'] = inst_obj['depth_port_min'] or '0.0'
                constrow['bottom'] = inst_obj['depth_port_max'] or '0.0'
                self._load_Constraint(constrow)

            class_obj = class_objs[ooi_rd.inst_class]
            series_obj = series_objs[ooi_rd.series_rd]
            inst_name = "%s (%s-%s)" % (class_obj['name'], series_obj['Class'], series_obj['Series'])
            newrow = {}
            newrow[COL_ID] = inst_id
            newrow['is/name'] = inst_name
            newrow['is/description'] = "Instrument: %s" % inst_id
            newrow['is/alt_ids'] = "['OOI:" + inst_id + "']"
            newrow['is/local_name'] = inst_name
            newrow['is/planned_uplink_port/port_type'] = "PAYLOAD"
            newrow['is/planned_uplink_port/reference_designator'] = ooi_rd.port_rd
            newrow['is/reference_designator'] = inst_id
            newrow['constraint_ids'] = const_id1
            newrow['coordinate_system'] = 'OOI_SUBMERGED_CS'
            newrow['org_ids'] = self.ooi_loader.get_org_ids([inst_id[:2]])
            newrow['instrument_model_ids'] = inst_obj['instrument_model']
            newrow['parent_site_id'] = inst_id[:14]

            if not self._match_filter(inst_id[:2]):
                continue

            self._load_InstrumentSite(newrow)

    def _load_StreamDefinition(self, row):
        if not row['parameter_dictionary'] and row['parameter_dictionary'] not in self.resource_ids:
            log.error('Stream Definition %s refers to unknown parameter dictionary: %s', row['ID'], row['parameter_dictionary'])
            return
        res_obj = self._create_object_from_row("StreamDefinition", row, "sdef/")

        svc_client = self._get_service_client("dataset_management")
        reference_designator = row['reference_designator']
        available_fields = row['available_fields']
        if available_fields:
            available_fields = available_fields.split(',')
            available_fields = [i.strip() for i in available_fields]
            for i,field in enumerate(available_fields):
                if field.startswith('PD') and field in self.resource_objs:
                    available_fields[i] = self.resource_objs[field].name
        
        parameter_dictionary_id = self.resource_ids[row['parameter_dictionary']]
        svc_client = self._get_service_client("pubsub_management")
        res_id = svc_client.create_stream_definition(name=res_obj.name, parameter_dictionary_id=parameter_dictionary_id,
                stream_configuration={'reference_designator' : reference_designator} if reference_designator else None,
                available_fields = available_fields or None,
            headers=self._get_system_actor_headers())
        self._register_id(row[COL_ID], res_id, res_obj)

        # Set alt_ids so that resource can be found in incremental preload runs
        sdef = self.container.resource_registry.read(res_id)
        sdef.alt_ids = ['PRE:'+row[COL_ID]]
        self.container.resource_registry.update(sdef)

    def _load_StreamDefinition_OOI(self):
        pass
        # TODO: We need streams for
        # - instrument, platform agents
        # - All data products

    def _conflict_report(self, row_id, name, reason):
        log.warn('''
------- Conflict Report -------
Conflict with %s
Parameter Name: %s
Reason: %s
-------------------------------''', row_id, name, reason)

    def _load_ParameterDictionary(self, row):
        dataset_management = self._get_service_client('dataset_management')
        types_manager = TypesManager(dataset_management, self.resource_ids, self.resource_objs)
        if row['SKIP']:
            self._conflict_report(row['ID'], row['name'], row['SKIP'])
            return

        name = row['name']
        definitions = row['parameter_ids'].replace(' ','').split(',')
        try:
            if row['temporal_parameter']:
                temporal_parameter_name = self.resource_objs[row['temporal_parameter']].name
            else:
                temporal_parameter_name = ''
        except KeyError:
            temporal_parameter_name = ''

        context_ids = {}
        for i in definitions:
            try:
                res_id = self.resource_ids[i]
                if res_id not in context_ids:
                    context_ids[res_id] = 0
                else:
                    log.warning('Duplicate: %s (%s)', name, i)
                context_ids[self.resource_ids[i]] = 0
                res = self.resource_objs[i]
                context = ParameterContext.load(res.parameter_context)
                lookup_values = types_manager.get_lookup_value_ids(context)
                for val in lookup_values:
                    context_ids[val] = 0
                if hasattr(context,'qc_contexts'):
                    for qc in context.qc_contexts:
                        if qc not in self.resource_ids:
                            obj = dataset_management.read_parameter_context(qc)
                            self._register_id(qc, qc, obj)
                    definitions.extend(context.qc_contexts)
            except KeyError:
                pass

        if not context_ids:
            log.warning('No valid parameters: %s', row['name'])
            return
        try:
            pdict_id = dataset_management.create_parameter_dictionary(name=name, parameter_context_ids=context_ids.keys(),
                                                                      temporal_context=temporal_parameter_name,
                                                                      headers=self._get_system_actor_headers())
            # Set alt_ids so that resource can be found in incremental preload runs
            pdict = self.container.resource_registry.read(pdict_id)
            pdict.alt_ids = ['PRE:'+row[COL_ID]]
            self.container.resource_registry.update(pdict)
        except Exception:
            log.exception('%s has a problem', row['name'])
            return

        self._register_id(row[COL_ID], pdict_id, pdict)

    def _load_Parser(self, row):
        name        = row['name']
        module      = row['parser/module']
        method      = row['parser/method']
        config      = row['parser/config']
        description = row['description']

        data_acquisition = self._get_service_client('data_acquisition_management')
        parser_id = data_acquisition.create_parser(name=name, module=module, method=method, config=config, description=description)
        self._register_id(row[COL_ID], parser_id)
    
    def _load_ParameterFunctions(self, row):
        if row['SKIP']:
            self._conflict_report(row['ID'], row['Name'], row['SKIP'])
            return

        name      = row['Name']
        ftype     = row['Function Type']
        func_expr = row['Function']
        owner     = row['Owner']
        args      = ast.literal_eval(row['Args'])
        #kwargs    = row['Kwargs']
        descr     = row['Description']

        dataset_management = self._get_service_client('dataset_management')
        func = None
        if ftype == 'NumexprFunction':
            func = NumexprFunction(row['Name'], func_expr, args)
        elif ftype == 'PythonFunction':
            func = PythonFunction(name, owner, func_expr, args, None)
        else:
            self._conflict_report(row['ID'], row['Name'], 'Unsupported Function Type: %s' % ftype)
            return

        func_id = dataset_management.create_parameter_function(name=name, parameter_function=func.dump(),
                                                               description=descr, headers=self._get_system_actor_headers())
        # Set alt_ids so that resource can be found in incremental preload runs
        func_obj = self.container.resource_registry.read(func_id)
        func_obj.alt_ids=['PRE:'+row[COL_ID]]
        self.container.resource_registry.update(func_obj)
        self._register_id(row[COL_ID], func_id, func_obj)

    def _load_ParameterDefs(self, row):
        if row['SKIP']:
            self._conflict_report(row['ID'], row['Name'], row['SKIP'])
            return

        name         = row['Name']
        ptype        = row['Parameter Type']
        encoding     = row['Value Encoding']
        uom          = row['Unit of Measure'] or 'undefined'
        code_set     = row['Code Set']
        fill_value   = row['Fill Value']
        display_name = row['Display Name']
        std_name     = row['Standard Name']
        long_name    = row['Long Name']
        references   = row['confluence']
        description  = row['Description']
        pfid         = row['Parameter Function ID']
        pmap         = row['Parameter Function Map']
        sname        = row['Data Product Identifier']
        precision    = row['Precision']
        param_id     = row['ID']
        lookup_value = row['Lookup Value']
        qc           = row['QC Functions']

        dataset_management = self._get_service_client('dataset_management')
        
        #validate unit of measure
        # allow google doc to include more maintainable "key: value, key: value" instead of python "{ 'key': 'value', 'key': 'value' }"
        pmap = pmap if pmap.startswith('{') else repr(parse_dict(pmap))

        if pfid and ptype!='function':
            log.warn('Parameter %s (%s) has type %s, did not expect function %s', row['ID'], name, ptype, pfid)
        #validate parameter type
        try:
            tm = TypesManager(dataset_management, self.resource_ids, self.resource_objs)
            param_type = tm.get_parameter_type(ptype, encoding,code_set,pfid, pmap)
            context = ParameterContext(name=name, param_type=param_type)
            context.uom = uom
            try:
                tm.get_unit(uom)
            except UdunitsError as e:
                log.warning('Parameter %s (%s) has invalid units: %s', name,param_id, uom)
            context.fill_value = tm.get_fill_value(fill_value, encoding, param_type)
            context.reference_urls = references
            context.internal_name = name
            context.display_name = display_name
            context.standard_name = std_name
            context.ooi_short_name = sname
            context.description = description
            context.precision = precision
            if lookup_value:
                if lookup_value.lower() == 'true':
                    context.lookup_value = name
                    context.document_key = ''
                else:
                    if '||' in lookup_value:
                        context.lookup_value,context.document_key = lookup_value.split('||')
                    else:
                        context.lookup_value = name
                        context.document_key = lookup_value

            if qc:
                try:
                    if isinstance(context.param_type, QuantityType):
                        context.qc_contexts = tm.make_qc_functions(name,qc,self._register_id)
                except KeyError:
                    pass

        except TypeError as e:
            log.exception(e.message)
            self._conflict_report(row['ID'], row['Name'], e.message)
            return
        except:
            log.exception('Could not load the following parameter definition: %s', row)
            return

        context_dump = context.dump()

        try:
            json.dumps(context_dump)
        except Exception as e:
            self._conflict_report(row['ID'], row['Name'], e.message)
            return
        try:
            creation_args = dict(
                name=name, parameter_context=context_dump,
                description=description,
                reference_urls=[references],
                parameter_type=ptype,
                internal_name=name,
                value_encoding=encoding,
                code_report=code_set,
                units=uom,
                fill_value=fill_value,
                display_name=display_name,
                parameter_function_map=pmap,
                standard_name=std_name,
                ooi_short_name=sname,
                precision=precision,
                headers=self._get_system_actor_headers())
            if pfid:
                try:
                    creation_args['parameter_function_id'] = self.resource_ids[pfid]
                except KeyError:
                    pass
            context_id = dataset_management.create_parameter_context(**creation_args)
            context_obj = self.container.resource_registry.read(context_id)
            context_obj.alt_ids = ['PRE:'+row[COL_ID]]
            self.container.resource_registry.update(context_obj)
        except AttributeError as e:
            if e.message == "'dict' object has no attribute 'read'":
                self._conflict_report(row['ID'], row['Name'], 'Something is not JSON compatible.')
                return
            else:
                self._conflict_report(row['ID'], row['Name'], e.message)
                return
        self._register_id(row[COL_ID], context_id, context_obj)

    def _load_PlatformDevice(self, row):
        contacts = self._get_contacts(row, field='contact_ids', type='PlatformDevice')
        res_id = self._basic_resource_create(row, "PlatformDevice", "pd/",
            "instrument_management", "create_platform_device", contacts=contacts,
            support_bulk=True)

        if self.bulk:
            # Create DataProducer and association
            pd_obj = self._get_resource_obj(row[COL_ID])
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
                device_obj = self._get_resource_obj(row[COL_ID])
                self._create_association(device_obj, PRED.hasModel, model_obj)
            else:
                ims_client.assign_platform_model_to_platform_device(self.resource_ids[ass_id], res_id,
                    headers=headers)


        ass_id = row.get("platform_device_id", None)

        #link child platform to parent platfrom
        if ass_id:
            log.debug('_load_PlatformDevice platform_device_id:  %s',  ass_id  )
            log.debug('_load_PlatformDevice _get_resource_obj(ass_id):  %s',  self._get_resource_obj(ass_id)   )
            log.debug('_load_PlatformDevice self.resource_ids[ass_id]:  %s',  self.resource_ids[ass_id]   )
            if self.bulk:
                parent_obj = self._get_resource_obj(ass_id)
                device_obj = self._get_resource_obj(row[COL_ID])
                self._create_association(parent_obj, PRED.hasDevice, device_obj)
            else:
                ims_client.assign_platform_device_to_platform_device(child_platform_device_id=res_id, platform_device_id=self.resource_ids[ass_id])



        oms_client = self._get_service_client("observatory_management")
        network_parent_id = row.get("network_parent_id", None)
        if network_parent_id:
            if self.bulk:
                parent_obj = self._get_resource_obj(network_parent_id)
                device_obj = self._get_resource_obj(row[COL_ID])
                self._create_association(device_obj, PRED.hasNetworkParent, parent_obj)
            else:
                oms_client.assign_device_to_network_parent(self.resource_ids[network_parent_id], res_id,
                                                           headers=headers)

        self._resource_advance_lcs(row, res_id, "PlatformDevice")

    def _load_PlatformDevice_ext(self, row):
        # HACK: This is to set the network parent after creating the device
        headers = self._get_op_headers(row)
        res_id = self._get_resource_id(row[COL_ID])
        oms_client = self._get_service_client("observatory_management")
        network_parent_id = row.get("network_parent_id", None)
        if network_parent_id:
            if self.bulk:
                parent_obj = self._get_resource_obj(network_parent_id)
                device_obj = self._get_resource_obj(row[COL_ID])
                self._create_association(device_obj, PRED.hasNetworkParent, parent_obj)
            else:
                oms_client.assign_device_to_network_parent(self.resource_ids[network_parent_id], res_id,
                                                           headers=headers)

    def _load_PlatformDevice_OOI(self):
        node_objs = self.ooi_loader.get_type_assets("node")

        for node_id, node_obj in node_objs.iteritems():
            if not self._before_cutoff(node_obj):
                continue

            newrow = {}
            newrow[COL_ID] = node_id + "_PD"
            newrow['pd/name'] = "%s" % node_obj.get('name', '')
            newrow['pd/description'] = "Platform %s device #01" % node_id
            newrow['org_ids'] = self.ooi_loader.get_org_ids([node_id[:2]])
            newrow['platform_model_id'] = node_id[9:11] + "_PM"
            newrow['contact_ids'] = ''
            newrow['network_parent_id'] = ""
            newrow['lcstate'] = "PLANNED_AVAILABLE"

            if not self._match_filter([node_id[:2]]):
                continue

            self._load_PlatformDevice(newrow)

        for node_id, node_obj in node_objs.iteritems():
            if not self._before_cutoff(node_obj):
                continue

            newrow = {}
            newrow[COL_ID] = node_id + "_PD"
            uplink_node = node_obj.get('uplink_node', "")
            newrow['network_parent_id'] = uplink_node + "_PD" if uplink_node and self._get_resource_obj(uplink_node + "_PD") else ""

            if not self._match_filter([node_id[:2]]):
                continue

            self._load_PlatformDevice_ext(newrow)

    def _load_InstrumentDevice(self, row):
        row['id/reference_urls'] = repr(get_typed_value(row['id/reference_urls'], targettype="simplelist"))
        contacts = self._get_contacts(row, field='contact_ids', type='InstrumentDevice')
        res_id = self._basic_resource_create(row, "InstrumentDevice", "id/",
            "instrument_management", "create_instrument_device", contacts=contacts,
            support_bulk=True)

        if self.bulk:
            # Create DataProducer and association
            id_obj = self._get_resource_obj(row[COL_ID])
            id_alias = self._get_alt_id(id_obj, "PRE")
            data_producer_obj = IonObject(RT.DataProducer, name=id_obj.name,
                description="Primary DataProducer for InstrumentDevice %s" % id_obj.name,
                producer_context=IonObject(OT.InstrumentProducerContext), is_primary=True)
            dp_id = self._create_bulk_resource(data_producer_obj, id_alias+"_DPPR")
            self._create_association(id_obj, PRED.hasDataProducer, data_producer_obj)

        ims_client = self._get_service_client("instrument_management")
        headers = self._get_op_headers(row)
        ass_id = row["instrument_model_id"]
        if ass_id:
            if self.bulk:
                model_obj = self._get_resource_obj(ass_id)
                device_obj = self._get_resource_obj(row[COL_ID])
                self._create_association(device_obj, PRED.hasModel, model_obj)
            else:
                ims_client.assign_instrument_model_to_instrument_device(self.resource_ids[ass_id], res_id,
                    headers=headers)
        ass_id = row["platform_device_id"]# if 'platform_device_id' in row else None
        if ass_id:
            if self.bulk:
                parent_obj = self._get_resource_obj(ass_id)
                device_obj = self._get_resource_obj(row[COL_ID])
                self._create_association(parent_obj, PRED.hasDevice, device_obj)
            else:
                ims_client.assign_instrument_device_to_platform_device(res_id, self.resource_ids[ass_id],
                    headers=headers)

        self._resource_advance_lcs(row, res_id, "InstrumentDevice")

    def _is_cabled(self, ooi_rd):
        # TODO: Refine this algorithm!
        return ooi_rd.marine_io == "RSN" or ooi_rd.subsite_rd == "CE02SHBP" or ooi_rd.subsite_rd == "CE04OSBP"

    def _load_InstrumentDevice_OOI(self):
        inst_objs = self.ooi_loader.get_type_assets("instrument")
        node_objs = self.ooi_loader.get_type_assets("node")
        class_objs = self.ooi_loader.get_type_assets("class")

        for ooi_id, inst_obj in inst_objs.iteritems():
            ooi_rd = OOIReferenceDesignator(ooi_id)
            node_obj = node_objs[ooi_rd.node_rd]
            if not self._before_cutoff(inst_obj) or not self._before_cutoff(node_obj):
                continue

            node_id = ooi_id[:14]
            if not node_obj.get('is_platform', False):
                node_id = node_obj.get('platform_id')
                node_obj = node_objs[node_id]
                if not node_obj.get('is_platform', False):
                    log.warn("Node %s is not a platform!!" % node_id)

            ooi_rd = OOIReferenceDesignator(ooi_id)
            newrow = {}
            newrow[COL_ID] = ooi_id + "_ID"
            newrow['id/name'] = "%s on %s" % (class_objs[ooi_rd.inst_class]['name'], node_objs[ooi_id[:14]]['name'])
            newrow['id/description'] = "Instrument %s device #01" % ooi_id
            newrow['id/reference_urls'] = ''
            newrow['org_ids'] = self.ooi_loader.get_org_ids([ooi_id[:2]])
            newrow['instrument_model_id'] = ooi_rd.series_rd
            # TODO: Only set the following for non cabled instruments
            if self._is_cabled(ooi_rd):
                newrow['platform_device_id'] = ""
            else:
                newrow['platform_device_id'] = node_id + "_PD"
            newrow['contact_ids'] = ''
            newrow['lcstate'] = "PLANNED_AVAILABLE"

            if not self._match_filter(ooi_id[:2]):
                continue

            self._load_InstrumentDevice(newrow)

    def _parse_alert_range(self, expression):
#        lower_bound	lower_rel_op	value_id	upper_rel_op	upper_bound
        out = {}
        if not expression:
            return out

        # split string expression into one of 3 possible arrays:
        # 5<temp or 5<=temp        --> lower bound only: number, [=]field
        # temp<5 or temp<=5        --> upper bound only: field, [=]number
        # 5<temp<10 or 5<=temp<=10 --> upper and lower: number, [=]field, [=]number
        parts = [ s.strip() for s in expression.split('<') ]
        try:
            # if first part is a number, expression begins with: number <[=] field ...
            # evaluate lower bound
            out['lower_bound'] = float(parts[0])
            lower_closed = parts[1].startswith('=')
            out['lower_rel_op'] = '<=' if lower_closed else '<'
            out['value_id'] = parts[1][1:] if lower_closed else parts[1]
            if len(parts)==2:
                return out
            # shift value for evaluation of upper bound
            parts = parts[1:]
        except ValueError:
            # otherwise expression must be: field <[=] number
            out['value_id'] = parts[0]
        # evaluate upper bound
        upper_closed = parts[1].startswith('=')
        out['upper_rel_op'] = '<=' if upper_closed else '<'
        upper_value = parts[1][1:] if upper_closed else parts[1]
        out['upper_bound'] = float(upper_value)
        return out

    def _load_Alerts(self, row):
        """
        DEFINITION category. Load and keep object for reference by other categories. No side effects.
        Keeps alert definition dicts.
        """
        # Hack so we don't break load work already done.
        if row['type'] == 'ALERT':
            row['type'] = 'ALARM'
        # alert is just a dict
        alert = {
            'name': row['name'],
            'description': row['message'],
            'alert_type': getattr(StreamAlertType, row['type'])
        }
        # add 5 parameters representing the value and range
        alert.update( self._parse_alert_range(row['range']) )
        # add additional freeform entries
        alert.update( parse_dict(row['config']) )
        # save for use in resources
        self.alerts[row[COL_ID]] = alert

    def _load_StreamConfiguration(self, row):
        """
        DEFINITION category. Load and keep IonObject for reference by other categories. No side effects.
        Keeps stream configuration object for use in *AgentInstance categories.
        """

#        alerts = []
#        if row['alerts']:
#            for id in row['alerts'].split(','):
#                copy = dict(self.alerts[id.strip()])
#                copy['kwargs']['stream_name'] = row['cfg/stream_name']
#                alerts.append(copy)
#            row['cfg/alarms'] = repr(alarms)  # _create_object_from_row won't take list directly, tries to eval(str) or raise ValueException
#            log.trace('adding alarms to StreamConfiguration %s: %r', row[COL_ID], alarms)
        obj = self._create_object_from_row("StreamConfiguration", row, "cfg/")
        self.stream_config[row['ID']] = obj

    def _load_PlatformAgent(self, row):
        stream_config_names = get_typed_value(row['stream_configurations'], targettype="simplelist")
        stream_configurations = [ self.stream_config[name] for name in stream_config_names ]

        res_id = self._basic_resource_create(row, "PlatformAgent", "pa/",
                                             "instrument_management", "create_platform_agent",
                                             set_attributes=dict(stream_configurations=stream_configurations),
                                             support_bulk=True)

        if self.bulk:
            # Create DataProducer and association
            pa_obj = self._get_resource_obj(row[COL_ID])
            proc_def_obj = IonObject(RT.ProcessDefinition)
            pd_id = self._create_bulk_resource(proc_def_obj)
            self._create_association(pa_obj, PRED.hasProcessDefinition, proc_def_obj)

        svc_client = self._get_service_client("instrument_management")
        headers = self._get_op_headers(row)
        model_ids = row["platform_model_ids"]
        if model_ids:
            model_ids = get_typed_value(model_ids, targettype="simplelist")
            for model_id in model_ids:
                if self.bulk:
                    model_obj = self._get_resource_obj(model_id)
                    agent_obj = self._get_resource_obj(row[COL_ID])
                    self._create_association(agent_obj, PRED.hasModel, model_obj)
                else:
                    svc_client.assign_platform_model_to_platform_agent(self.resource_ids[model_id], res_id,
                                                                       headers=headers)

        self._resource_advance_lcs(row, res_id, "InstrumentAgent")

    def _load_PlatformAgent_OOI(self):
        ooi_objs = self.ooi_loader.get_type_assets("platformagent")
        nodetype_objs = self.ooi_loader.get_type_assets("nodetype")

        for ooi_id, ooi_obj in ooi_objs.iteritems():
            if ooi_obj['agent_type'] == "PlatformAgent":
                newrow = {}
                newrow[COL_ID] = ooi_id + "_PA"
                newrow['pa/name'] = ooi_obj['name']
                newrow['pa/description'] = "Platform Agent for " + ooi_id
                node_types = ["%s_PM" % nt for nt in ooi_obj['node_types'].split(',') if self._get_resource_obj("%s_PM" % nt)]
                newrow['platform_model_ids'] = ','.join(node_types)
                newrow['org_ids'] = self.ooi_loader.get_org_ids(ooi_obj.get('array_list', None))
                newrow['stream_configurations'] = ""
                newrow['lcstate'] = "DEPLOYED_AVAILABLE"

                if not self._match_filter(ooi_obj.get('array_list', None)):
                    continue

                self._load_PlatformAgent(newrow)

    def _load_PlatformAgentInstance(self, row):
        # construct values for more complex fields

        alerts_config = [ self.alerts[id.strip()] for id in row['alerts'].split(',') ] if row['alerts'].strip() else []

        platform_id = row['platform_id']
        platform_agent_id = self.resource_ids[row['platform_agent_id']]
        platform_device_id = self.resource_ids[row['platform_device_id']]
        pubrate = row['publish_rate']

        driver_config = parse_dict(row['driver_config'])
        log.debug("driver_config = %s", driver_config)

        # Note: platform_id currently expected by PlatformAgent as follows:
        agent_config = {
            'platform_config': {'platform_id': platform_id},
            'aparam_pubrate_config' : pubrate
        }
        # TODO determine how to finally indicate this platform_id.)

        res_id = self._basic_resource_create(row, "PlatformAgentInstance", "pai/",
                                             "instrument_management", "create_platform_agent_instance",
                                             set_attributes=dict(agent_config=agent_config,
                                                                 driver_config=driver_config,
                                                                 alerts=alerts_config),
                                             )

        client = self._get_service_client("instrument_management")
        client.assign_platform_agent_to_platform_agent_instance(platform_agent_id, res_id)
        client.assign_platform_agent_instance_to_platform_device(res_id, platform_device_id)

        self.resource_ids[row['ID']] = res_id

    def _load_PlatformAgentInstance_OOI(self):
        pass

    def _load_InstrumentAgent(self, row):
        stream_config_names = get_typed_value(row['stream_configurations'], targettype="simplelist")
        stream_configurations = [ self.stream_config[name] for name in stream_config_names ]

        res_id = self._basic_resource_create(row, "InstrumentAgent", "ia/",
            "instrument_management", "create_instrument_agent",
            set_attributes=dict(stream_configurations=stream_configurations),
            support_bulk=True)

        if self.bulk:
            # Create DataProducer and association
            ia_obj = self._get_resource_obj(row[COL_ID])
            proc_def_obj = IonObject(RT.ProcessDefinition)
            pd_id = self._create_bulk_resource(proc_def_obj)
            self._create_association(ia_obj, PRED.hasProcessDefinition, proc_def_obj)

        svc_client = self._get_service_client("instrument_management")

        headers = self._get_op_headers(row)
        im_ids = row["instrument_model_ids"]
        if im_ids:
            im_ids = get_typed_value(im_ids, targettype="simplelist")
            for im_id in im_ids:
                if self.bulk:
                    model_obj = self._get_resource_obj(im_id)
                    agent_obj = self._get_resource_obj(row[COL_ID])
                    self._create_association(agent_obj, PRED.hasModel, model_obj)
                else:
                    svc_client.assign_instrument_model_to_instrument_agent(self.resource_ids[im_id], res_id,
                        headers=headers)

        self._resource_advance_lcs(row, res_id, "InstrumentAgent")

    def _load_InstrumentAgent_OOI(self):
        agent_objs = self.ooi_loader.get_type_assets("instagent")

        for ooi_id, agent_obj in agent_objs.iteritems():
            if agent_obj.get('active', False):
                ooi_rd = OOIReferenceDesignator(ooi_id)
                newrow = {}
                newrow[COL_ID] = "IA_" + ooi_id
                newrow['ia/name'] = "Instrument Agent " + ooi_id
                newrow['ia/description'] = "Supports models: " + ",".join(agent_obj.get('series_list', []))
                newrow['org_ids'] = self.ooi_loader.get_org_ids([ooi_id[:2]])
                series_list = agent_obj.get('series_list', [])
                series_list = [sid for sid in series_list if self._get_resource_obj(sid)]
                newrow['instrument_model_ids'] = ",".join(series_list)
                newrow['stream_configurations'] = ""
                newrow['lcstate'] = "DEPLOYED_AVAILABLE"

                if not self._match_filter(ooi_id[:2]):
                    continue

                self._load_InstrumentAgent(newrow)

    def _load_ExternalDataProvider(self, row):
        contacts = self._get_contacts(row, field='contact_id')
        if len(contacts) > 1:
            raise iex.BadRequest('External dataset %s has too many contacts (should be 1)' % row[COL_ID])
        contact = contacts[0] if len(contacts)==1 else None
        institution = self._create_object_from_row("Institution", row, "i/")

        self._basic_resource_create(row, "ExternalDataProvider", "p/",
            "data_acquisition_management", "create_external_data_provider",
            set_attributes=dict(institution=institution, contact=contact))

    def _load_ExternalDatasetModel(self, row):
        # ID, lcstate, name, description, dataset_type
        self._basic_resource_create(row, 'ExternalDatasetModel', 'edm/', 'data_acquisition_management', 'create_external_dataset_model')

    def _load_ExternalDataset(self, row):
        # ID	owner_id	lcstate	org_ids	contact_id	name	description	data_sampling	parameters
        contacts = self._get_contacts(row, field='contact_id')
        if len(contacts) > 1:
            raise iex.BadRequest('External dataset %s has too many contacts (should be 1)' % row[COL_ID])
        contact = contacts[0] if len(contacts)==1 else None

        model = self._get_resource_id(row['model'])
        params = parse_dict(row['parameters'])
        sampling = getattr(objects.DatasetDescriptionDataSamplingEnum, row['data_sampling'] if row['data_sampling'] else 'NONE')
        descriptor = objects.DatasetDescription(data_sampling=sampling, parameters=params)
        dataset = IonObject(RT.ExternalDataset, name=row['name'], description=row['description'], dataset_description=descriptor,
            contact=contact, alt_ids=['PRE:'+row[COL_ID]], lcstate=row[COL_LCSTATE])
        client = self._get_service_client('data_acquisition_management')
        id = client.create_external_dataset(external_dataset=dataset, external_dataset_model_id=model)
        dataset._id = id
        self._register_id(row['ID'], id, dataset)
        producer_id = client.register_external_data_set(external_dataset_id=id)
        self.external_dataset_producer_id[id] = producer_id

    def _load_ExternalDatasetAgent(self, row):
        agent = self._create_object_from_row(RT.ExternalDatasetAgent, row, 'eda/')
        model = self._get_resource_id(row['model'])
        id = self._get_service_client('data_acquisition_management').create_external_dataset_agent(external_dataset_agent=agent, external_dataset_model_id=model)
        agent._id = id
        self._register_id(row['ID'], id, agent)

    def _load_ExternalDatasetAgentInstance(self, row):
        # FIELDS IN THE ION OBJECT:
        # name='', description='', lcstate='DRAFT', availability='PRIVATE', ts_created='', ts_updated='', alt_ids=None, addl=None,
        # deployment_type=DeploymentTypeEnum.PROCESS, driver_config=None, agent_config=None, agent_process_id='',
        # alerts=None, handler_module='', handler_class='', dataset_driver_config=None, dataset_agent_config=None, dataset_agent_process_id=''):

        # Generate the data product and associate it to the ExternalDataset
        name = row['name']
        description = row['description']
        source_id = self._get_resource_id(row['source'])

        streamdef_id = self._get_resource_id(row['streamdef'])
        agent = self._get_resource_obj(row['agent'])
        agent_config = parse_dict(row['agent_config'])
        driver_config = parse_dict(row['driver_config'])


        # NOTE: unit tests show additional keys in this configuration
        # but some are handler-specific, others seem just for testing
        # TODO: come back and look again when trying to start this process
        driver_config.update( {
                'parser': {
                    'uri': row['parser_uri'],
                    'module': row['parser_module'],
                    'class': row['parser_class'],
                    'config': parse_dict(row['parser_config']),
                },
                'poller': {
                    'uri': row['poller_uri'],
                    'module': row['poller_module'],
                    'class': row['poller_class'],
                    'config': parse_dict(row['poller_config']),
                },
                'stream_def': streamdef_id,
#                'data_producer_id':self.external_dataset_producer_id[dataset_id],
                'max_records': int(row['records_per_granule']),
            } )
        agent_config.update( {
            'driver_config' : driver_config,
            'stream_config' : { },
            'agent'         : {'resource_id': source_id},
        } )

        agent_instance = IonObject(RT.ExternalDatasetAgentInstance,  name=name, description=description,
            dataset_driver_config=driver_config, dataset_agent_config=agent_config)

        client = self._get_service_client('data_acquisition_management')
        instance_id = client.create_external_dataset_agent_instance(external_dataset_agent_instance=agent_instance,
            external_dataset_agent_id=agent._id, external_dataset_id=source_id)

    def _load_InstrumentAgentInstance(self, row):
        startup_config = parse_dict(row['startup_config'])
        pubrate = row['publish_rate']

        alerts = [ self.alerts[id.strip()] for id in row['alerts'].split(',') ] if row['alerts'].strip() else []

        # define complicated attributes
        agent_config = {
            'aparam_pubrate_config' : pubrate
        }

        driver_config = { 'comms_config': { 'addr':  row['comms_server_address'],
                                                    'port':  int(row['comms_server_port']),
                                                    'cmd_port': int(row['comms_server_cmd_port']) } }

        port_agent_config = { 'device_addr':   row['comms_device_address'],
                              'device_port':   int(row['comms_device_port']),
                              'process_type':  PortAgentProcessType.UNIX,
                              'port_agent_addr': 'localhost',
                              'type': PortAgentType.ETHERNET,
                              'binary_path':   "port_agent",
                              'command_port':  int(row['comms_server_cmd_port']),
                              'data_port':     int(row['comms_server_port']),
                              'log_level':     5,  }

        res_id = self._basic_resource_create(row, "InstrumentAgentInstance", "iai/",
            "instrument_management", "create_instrument_agent_instance",
            set_attributes=dict(agent_config=agent_config,
                                driver_config=driver_config,
                                port_agent_config=port_agent_config,
                                startup_config=startup_config,
                                alerts=alerts),
            )

        agent_id = self.resource_ids[row["instrument_agent_id"]]
        device_id = self.resource_ids[row["instrument_device_id"]]
        client = self._get_service_client("instrument_management")

        client.assign_instrument_agent_to_instrument_agent_instance(agent_id, res_id)
        client.assign_instrument_agent_instance_to_instrument_device(res_id, device_id)

    def _load_InstrumentAgentInstance_OOI(self):
        pass

    def _load_TransformFunction(self,row):
        res_id = self._basic_resource_create(row,"TransformFunction", "tfm/",
                                             "data_process_management", "create_transform_function")

    def _load_DataProcessDefinition(self, row):
        res_id = self._basic_resource_create(row, "DataProcessDefinition", "dpd/",
                                            "data_process_management", "create_data_process_definition")

        svc_client = self._get_service_client("data_process_management")

        input_strdef = row["input_stream_defs"]
        if input_strdef:
            input_strdef = get_typed_value(input_strdef, targettype="simplelist")
        log.trace("Assigning input StreamDefinition to DataProcessDefinition for %s" % input_strdef)
        headers = self._get_op_headers(row)

        for insd in input_strdef:
            svc_client.assign_input_stream_definition_to_data_process_definition(self.resource_ids[insd], res_id,
                headers=headers)

        output_strdef = row["output_stream_defs"]
        if output_strdef:
            output_strdef = get_typed_value(output_strdef, targettype="dict")
        for binding, strdef in output_strdef.iteritems():
            svc_client.assign_stream_definition_to_data_process_definition(self.resource_ids[strdef], res_id, binding,
                headers=headers)

    def _load_DataProcess(self, row):
        dpd_id = self.resource_ids[row["data_process_definition_id"]]
        log.trace("_load_DataProcess  data_product_def %s", str(dpd_id))
        in_data_product_id = self.resource_ids[row["in_data_product_id"]]
        configuration = row["configuration"]
        if configuration:
            configuration = get_typed_value(configuration, targettype="dict")

        out_data_products = row["out_data_products"]
        out_data_product_ids = None
        if out_data_products:
            out_data_products = get_typed_value(out_data_products, targettype="dict")
            out_data_product_ids = [self.resource_ids[dp_id] for dp_id in out_data_products.values()]

        svc_client = self._get_service_client("data_process_management")
        
        headers = self._get_op_headers(row)
        res_id = svc_client.create_data_process(dpd_id, [in_data_product_id], out_data_product_ids, configuration, headers=headers)
        self._register_id(row[COL_ID], res_id)

        self._resource_assign_org(row, res_id)

        res_id = svc_client.activate_data_process(res_id, headers=self._get_system_actor_headers())

    def _load_DataProduct(self, row, do_bulk=False):
        tdom, sdom = time_series_domain()

        contacts = self._get_contacts(row, field='contact_ids', type='DataProduct')
        res_obj = self._create_object_from_row("DataProduct", row, "dp/", contacts=contacts, contact_field='contacts')

        constraint_id = row['geo_constraint_id']
        if constraint_id:
            res_obj.geospatial_bounds = self.constraint_defs[constraint_id]
        gcrs_id = row['coordinate_system_id']
        if gcrs_id:
            res_obj.geospatial_coordinate_reference_system = self.resource_ids[gcrs_id]
        parent_dataset_id=None
        if row['parent'] and row['parent'] in self.resource_ids:
            parent_id = self.resource_ids[row['parent']]
            parent_dataset_ids, _ = self.container.resource_registry.find_objects(parent_id,PRED.hasDataset, id_only=True)
            parent_dataset_id = parent_dataset_ids[0] if len(parent_dataset_ids) else None
        res_obj.spatial_domain = sdom.dump()
        res_obj.temporal_domain = tdom.dump()

        headers = self._get_op_headers(row)

        if self.bulk and do_bulk:
            # This is a non-functional, diminished version of a DataProduct, just to show up in lists
            res_id = self._create_bulk_resource(res_obj, row[COL_ID])
            self._resource_assign_owner(headers, res_obj)

            if res_obj.geospatial_bounds:
                res_obj.geospatial_point_center = GeoUtils.calc_geospatial_point_center(res_obj.geospatial_bounds)

            # Create and associate Stream
            # Create and associate Dataset
        else:
            svc_client = self._get_service_client("data_product_management")
            stream_definition_id = self.resource_ids[row["stream_def_id"]]
            res_id = svc_client.create_data_product(data_product=res_obj, stream_definition_id=stream_definition_id,
                    dataset_id=parent_dataset_id or None,
                headers=headers)
            self._register_id(row[COL_ID], res_id, res_obj)

            if not self.debug and get_typed_value(row['persist_data'], targettype="bool"):
                svc_client.activate_data_product_persistence(res_id, headers=headers)

        self._resource_assign_org(row, res_id)
        self._resource_advance_lcs(row, res_id, "DataProduct")

    def _load_DataProduct_OOI(self):
        node_objs = self.ooi_loader.get_type_assets("node")
        inst_objs = self.ooi_loader.get_type_assets("instrument")
        data_products = self.ooi_loader.get_type_assets("data_product")

        # For each device agent
        #   for each stream
        #     create Dataset

        # I. Platform data products (parsed)
        for node_id, node_obj in node_objs.iteritems():
            if not self._before_cutoff(node_obj):
                continue

            const_id1 = ''
            if node_obj.get('latitude', None) or node_obj.get('longitude', None) or node_obj.get('depth_subsite', None):
                # At this point, the constraint was already added with the PlatformSite
                const_id1 = node_id + "_const1"

            newrow = {}
            newrow[COL_ID] = node_id + "_DPPDP"
            newrow['dp/name'] = "Parsed - platform " + node_id
            newrow['dp/description'] = "Data Product (device, parsed) for: " + node_id
            newrow['dp/ooi_product_name'] = "Raw"
            newrow['org_ids'] = self.ooi_loader.get_org_ids([node_id[:2]])
            newrow['contact_ids'] = ''
            newrow['geo_constraint_id'] = const_id1
            newrow['coordinate_system_id'] = 'OOI_SUBMERGED_CS'
            newrow['stream_def_id'] = ''
            newrow['parent'] = ''
            self._load_DataProduct(newrow, do_bulk=self.bulk)

        # II. Instrument data products (raw, parsed, engineering, science L0, L1, L2)
        for inst_id, inst_obj in inst_objs.iteritems():
            ooi_rd = OOIReferenceDesignator(inst_id)
            node_obj = node_objs[ooi_rd.node_rd]
            if not self._before_cutoff(inst_obj) or not self._before_cutoff(node_obj):
                continue

            const_id1 = ''
            if inst_obj['latitude'] or inst_obj['longitude'] or inst_obj['depth_port_max'] or inst_obj['depth_port_min']:
                # At this point, the constraint was already added with the InstrumentSite
                const_id1 = inst_id + "_const1"

            if not self._match_filter(inst_id[:2]):
                continue

            # (1) Device Data Product - parsed
            newrow = {}
            newrow[COL_ID] = inst_id + "_DPIDP"
            newrow['dp/name'] = "Parsed - instrument " + inst_id
            newrow['dp/description'] = "Data Product (device, parsed) for: " + inst_id
            newrow['dp/ooi_product_name'] = "Raw"
            newrow['org_ids'] = self.ooi_loader.get_org_ids([inst_id[:2]])
            newrow['contact_ids'] = ''
            newrow['geo_constraint_id'] = const_id1
            newrow['coordinate_system_id'] = 'OOI_SUBMERGED_CS'
            newrow['stream_def_id'] = ''
            newrow['parent'] = ''
            self._load_DataProduct(newrow, do_bulk=self.bulk)

            # (2) Device Data Product - raw
            newrow = {}
            newrow[COL_ID] = inst_id + "_DPIDR"
            newrow['dp/name'] = "Raw - instrument " + inst_id
            newrow['dp/description'] = "Data Product (device, raw) for: " + inst_id
            newrow['dp/ooi_product_name'] = "Parsed"
            newrow['org_ids'] = self.ooi_loader.get_org_ids([inst_id[:2]])
            newrow['contact_ids'] = ''
            newrow['geo_constraint_id'] = const_id1
            newrow['coordinate_system_id'] = 'OOI_SUBMERGED_CS'
            newrow['stream_def_id'] = ''
            newrow['parent'] = ''
            self._load_DataProduct(newrow, do_bulk=self.bulk)

            # TODO - Engineering by stream

            data_product_list = inst_obj.get('data_product_list', [])
            for dp_id in data_product_list:
                dp_obj = data_products[dp_id]

                # (3*) Site Data Product DPS - Level
                newrow = {}
                newrow[COL_ID] = inst_id + "_" + dp_id + "_DPID"
                platform_obj = node_objs[node_obj['platform_id']]
                newrow['dp/name'] = "%s %s - %s on %s" % (dp_obj['name'], dp_obj['level'], inst_obj['Class'], platform_obj['name'])
                newrow['dp/description'] = "Data Product DPS %s level %s for site %s: " % (dp_id,  dp_obj['level'], inst_id)
                newrow['dp/ooi_short_name'] = dp_obj['code']
                newrow['dp/ooi_product_name'] = dp_obj['name']
                newrow['dp/processing_level_code'] = dp_obj['level']
                newrow['dp/regime'] = dp_obj.get('regime', "")
                newrow['dp/qc_cmbnflg'] = dp_obj.get('Combine QC Flags (CMBNFLG) QC', "")
                newrow['dp/qc_condcmp'] = dp_obj.get('Conductivity Compressibility Compensation (CONDCMP) QC', "")
                newrow['dp/qc_glblrng'] = dp_obj.get('Global Range Test (GLBLRNG) QC', "")
                newrow['dp/qc_gradtst'] = dp_obj.get('Gradient Test (GRADTST) QC', "")
                newrow['dp/qc_interp1'] = dp_obj.get('1-D Interpolation (INTERP1) QC', "")
                newrow['dp/qc_loclrng'] = dp_obj.get('Local Range Test (LOCLRNG) QC', "")
                newrow['dp/qc_modulus'] = dp_obj.get('Modulus (MODULUS) QC', "")
                newrow['dp/qc_polyval'] = dp_obj.get('Evaluate Polynomial (POLYVAL) QC', "")
                newrow['dp/qc_solarel'] = dp_obj.get('Solar Elevation (SOLAREL) QC', "")
                newrow['dp/qc_spketest'] = dp_obj.get('Spike Test (SPKETST) QC', "")
                newrow['dp/qc_stuckvl'] = dp_obj.get('Stuck Value Test (STUCKVL) QC', "")
                newrow['dp/qc_trndtst'] = dp_obj.get('Trend Test (TRNDTST) QC', "")
                newrow['dp/dps_dcn'] = dp_obj.get('DPS DCN(s)', "")
                newrow['dp/flow_diagram_dcn'] = dp_obj.get('Processing Flow Diagram DCN(s)', "")
                newrow['dp/doors_l2_requirement_num'] = dp_obj.get('DOORS L2 Science Requirement #(s)', "")
                newrow['dp/doors_l2_requirement_text'] = dp_obj.get('DOORS L2 Science Requirement Text', "")
                newrow['org_ids'] = self.ooi_loader.get_org_ids([inst_id[:2]])
                newrow['contact_ids'] = ''
                newrow['geo_constraint_id'] = const_id1
                newrow['coordinate_system_id'] = 'OOI_SUBMERGED_CS'
                newrow['stream_def_id'] = ''
                newrow['parent'] = ''

                self._load_DataProduct(newrow, do_bulk=self.bulk)

    def _load_DataProductLink(self, row, do_bulk=False):
        dp_id = self.resource_ids[row["data_product_id"]]
        input_res_id = row["input_resource_id"]
        restype = row['resource_type']

        svc_client = self._get_service_client("data_acquisition_management")
        headers = self._get_system_actor_headers()

        # create link from DataProduct to original source
        source_id = row.get('source_resource_id', None)
        if source_id:
            source_id = self.resource_ids[source_id]
            if self.bulk and do_bulk:
                dp_obj = self._get_resource_obj(row["data_product_id"])
                source_obj = self._get_resource_obj(source_id)
                self._create_association(dp_obj, PRED.hasSource, source_obj)
            else:
                svc_client.assign_data_product_source(dp_id, source_id, headers=headers, timeout=500)

        # Create data product assignment
        if input_res_id and (restype=='InstrumentDevice' or restype=='PlatformDevice' or restype=='ExternalDataset'):
            if input_res_id not in self.resource_ids:
                log.error('Input resource %s does not exist', input_res_id)
                return
            input_res_id = self.resource_ids.get(input_res_id)
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
                svc_client.assign_data_product(input_res_id, dp_id, headers=headers, timeout=500)

    def _load_DataProductLink_OOI(self):
        node_objs = self.ooi_loader.get_type_assets("node")
        inst_objs = self.ooi_loader.get_type_assets("instrument")

        def create_dp_link(dp_id, source_id="", res_type=""):
            newrow = {}
            newrow['data_product_id'] = dp_id
            newrow['input_resource_id'] = source_id if res_type else ""
            newrow['resource_type'] = res_type
            newrow['source_resource_id'] = source_id
            self._load_DataProductLink(newrow, do_bulk=self.bulk)

        # I. Platform data product links
        for node_id, node_obj in node_objs.iteritems():
            if not self._before_cutoff(node_obj):
                continue

            create_dp_link(node_id + "_DPPDP", node_id + "_PD", 'PlatformDevice')
            create_dp_link(node_id + "_DPPDP", node_id)

        # II. Instrument data product links
        for inst_id, inst_obj in inst_objs.iteritems():
            ooi_rd = OOIReferenceDesignator(inst_id)
            node_obj = node_objs[ooi_rd.node_rd]
            if not self._before_cutoff(inst_obj) or not self._before_cutoff(node_obj):
                continue

            if not self._match_filter(inst_id[:2]):
                continue

            create_dp_link(inst_id + "_DPIDP", inst_id + "_ID", 'InstrumentDevice')
            create_dp_link(inst_id + "_DPIDP", inst_id)

            create_dp_link(inst_id + "_DPIDR", inst_id + "_ID", 'InstrumentDevice')
            create_dp_link(inst_id + "_DPIDR", inst_id)

            # TODO: Engineering data

            data_product_list = inst_obj.get('data_product_list', [])
            for dp_id in data_product_list:
                create_dp_link(inst_id + "_" + dp_id + "_DPID", inst_id + "_ID")
                create_dp_link(inst_id + "_" + dp_id + "_DPID", inst_id)

    def _load_Attachment(self, row):
        res_id = self.resource_ids[row["resource_id"]]
        filename = row["file_path"]
        log.trace("Loading Attachment %s from file %s", res_id, filename)

        att_obj = self._create_object_from_row("Attachment", row, "att/")
        if row['parser'] and row['parser'] in self.resource_ids:
            att_obj.context = objects.ReferenceAttachmentContext(parser_id=self.resource_ids[row['parser']])
        if not filename:
            raise iex.BadRequest('attachment did not include a filename: ' + row[COL_ID])

        path = "%s/%s" % (self.attachment_path, filename)
        try:
            with open(path, "rb") as f:
                att_obj.content = f.read()
        except IOError, ioe:
            # warn instead of fail here
            log.warn("Failed to open attachment file: %s/%s" % (path, ioe))

        att_id = self.container.resource_registry.create_attachment(res_id, att_obj)
        self._register_id(row[COL_ID], att_id, att_obj)

    def _load_WorkflowDefinition(self, row):
        log.info("Loading WorkflowDefinition")

        # Create the workflow steps
        steps_string = row["steps"]
        workflow_step_ids = []
        if steps_string:
            workflow_step_ids = get_typed_value(steps_string, targettype="simplelist")
        else:
            log.info("No steps found for workflow definition. Ignoring this entry")
            return

        # Locate the data process def objects and add them to the workflow def
        workflow_steps = []
        for step_id in workflow_step_ids:
            workflow_step_obj = IonObject('DataProcessWorkflowStep', data_process_definition_id=self.resource_ids[step_id])
            workflow_steps.append(workflow_step_obj)

        res_id = self._basic_resource_create(row, "WorkflowDefinition", "wfd/",
                                             "workflow_management", "create_workflow_definition",
                                             set_attributes=dict(workflow_steps=workflow_steps))

    def _load_Workflow(self, row):
        workflow_obj = self._create_object_from_row("Workflow", row, "wf/")
        workflow_client = self._get_service_client("workflow_management")
        workflow_def_id = self.resource_ids[row["wfd_id"]]
        in_dp_id = self.resource_ids[row["in_dp_id"]]

        # prepare the config dict
        configuration = row['configuration']
        if configuration:
            configuration = get_typed_value(configuration, targettype="dict")
            configuration["in_dp_id"] = in_dp_id

        headers = self._get_op_headers(row)

        # Create and start the workflow
        workflow_client.create_data_process_workflow(
            workflow_definition_id=workflow_def_id,
            input_data_product_id=in_dp_id, persist_workflow_data_product=get_typed_value(row["persist_data"], targettype="bool"),
            configuration=configuration, timeout=30,
            headers=headers)

    def _load_Deployment(self, row):
        constraints = self._get_constraints(row, type='Deployment')
        coordinate_name = row['coordinate_system']
        context_type = row['context_type']

        context = IonObject(context_type)

        deployment_id = self._basic_resource_create(row, "Deployment", "d/",
                                             "observatory_management", "create_deployment",
                                             constraints=constraints, constraint_field='constraint_list',
                                             set_attributes={"coordinate_reference_system": self.resource_ids[coordinate_name] if coordinate_name else None,
                                                             "context": context})

        device_id = self.resource_ids[row['device_id']]
        site_id = self.resource_ids[row['site_id']]

        oms = self._get_service_client("observatory_management")
        ims = self._get_service_client("instrument_management")

        headers = self._get_op_headers(row)

        oms.deploy_instrument_site(site_id, deployment_id, headers=headers)
        ims.deploy_instrument_device(device_id, deployment_id, headers=headers)

        if get_typed_value(row['activate'], targettype="bool"):
            oms.activate_deployment(deployment_id, headers=headers)

    def _load_Deployment_OOI(self):
        node_objs = self.ooi_loader.get_type_assets("node")
        inst_objs = self.ooi_loader.get_type_assets("instrument")
        row_count = 0

        # I. Platform deployments (parsed)
        for node_id, node_obj in node_objs.iteritems():
            if not self._before_cutoff(node_obj):
                continue
            if not self._match_filter(node_id[:2]):
                continue
            if not node_obj.get('is_platform', False):
                continue

            # Create a TemporalBounds constraint
            const_id1 = ''

            newrow = {}
            newrow[COL_ID] = node_id + "_DEP"
            newrow['site_id'] = node_id
            newrow['device_id'] = node_id + "_PD"
            newrow['activate'] = "FALSE"
            newrow['d/name'] = "Deployment of platform " + node_id
            newrow['d/description'] = ""
            newrow['org_ids'] = self.ooi_loader.get_org_ids([node_id[:2]])
            newrow['constraint_ids'] = const_id1
            newrow['coordinate_system'] = 'OOI_SUBMERGED_CS'
            newrow['context_type'] = 'CabledNodeDeploymentContext'

            # TODO: If RSN primary node (past), activate and set to DEPLOYED

            self._load_Deployment(newrow)
            row_count += 1

        # II. Instrument deployments (RSN and cabled EA only)
        for inst_id, inst_obj in inst_objs.iteritems():
            ooi_rd = OOIReferenceDesignator(inst_id)
            node_obj = node_objs[ooi_rd.node_rd]
            if not self._before_cutoff(inst_obj) or not self._before_cutoff(node_obj):
                continue
            if not self._match_filter(inst_id[:2]):
                continue
            if not self._is_cabled(ooi_rd):
                continue

            # Create a TemporalBounds constraint
            const_id1 = ''

            newrow = {}
            newrow[COL_ID] = inst_id + "_DEP"
            newrow['site_id'] = inst_id
            newrow['device_id'] = inst_id + "_ID"
            newrow['activate'] = "FALSE"
            newrow['d/name'] = "Deployment of instrument " + inst_id
            newrow['d/description'] = ""
            newrow['org_ids'] = self.ooi_loader.get_org_ids([inst_id[:2]])
            newrow['constraint_ids'] = const_id1
            newrow['coordinate_system'] = 'OOI_SUBMERGED_CS'
            newrow['context_type'] = 'CabledInstrumentDeploymentContext'

            self._load_Deployment(newrow)
            row_count += 1

        log.debug("Loaded %s OOI resources", row_count)

    def _load_Scheduler(self, row):
        scheduler_type = row['type']
        event_origin = row['event_origin']
        event_subtype = row['event_subtype']

        client = self._get_service_client('scheduler')
        if scheduler_type == 'TimeOfDayTimer':
            #times_of_day are comma separated strings of the format HH:MM:SS that must be put into a lsit of dicts
            times_of_day = []
            times_of_day_string = row['times_of_day']
            list_of_strings = times_of_day_string.strip().split(',')
            for string in list_of_strings:
                HH, MM, SS = string.strip().split(':')
                times_of_day.append( {'hour':HH, 'minute':MM, 'second':SS} )

            expires = row['expires']
            tag = client.create_time_of_day_timer(times_of_day=times_of_day,  expires=expires, event_origin=event_origin, event_subtype=event_subtype)

            #if the subtype is the UNS batch timer then set the key in UNS
            if event_subtype == 'UNS_batch_timer':
                client = self._get_service_client('user_notification')
                client.set_process_batch_key(process_batch_key = event_origin)

        elif scheduler_type == 'IntervalTimer':
            start_time = row['start_time']
            interval = int(row['interval'])
            end_time = row['end_time']
            tag = client.create_interval_timer(start_time= start_time, interval=interval,  end_time=end_time, event_origin=event_origin, event_subtype=event_origin)
