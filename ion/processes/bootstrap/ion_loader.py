#!/usr/bin/env python

"""Process that loads ION resources via service calls based on given definitions

    @see https://confluence.oceanobservatories.org/display/CIDev/R2+System+Preload
    bin/pycc -x ion.processes.bootstrap.ion_loader.IONLoader op=load path=master scenario=R2_DEMO

    bin/pycc -x ion.processes.bootstrap.ion_loader.IONLoader op=load path="https://docs.google.com/spreadsheet/pub?key=0AttCeOvLP6XMdG82NHZfSEJJOGdQTkgzb05aRjkzMEE&output=xls"
    bin/pycc -x ion.processes.bootstrap.ion_loader.IONLoader op=load path=res/preload/r2_ioc scenario=R2_DEMO

    bin/pycc -x ion.processes.bootstrap.ion_loader.IONLoader op=loadui path=res/preload/r2_ioc
    bin/pycc -x ion.processes.bootstrap.ion_loader.IONLoader op=loadui path=https://userexperience.oceanobservatories.org/database-exports/

    bin/pycc -x ion.processes.bootstrap.ion_loader.IONLoader op=load path=res/preload/r2_ioc scenario=R2_DEMO
    bin/pycc -x ion.processes.bootstrap.ion_loader.IONLoader op=load path=res/preload/r2_ioc scenario=R2_DEMO loadooi=True
    bin/pycc -x ion.processes.bootstrap.ion_loader.IONLoader op=load path=res/preload/r2_ioc scenario=R2_DEMO loadui=True
    bin/pycc -x ion.processes.bootstrap.ion_loader.IONLoader op=loadooi path=res/preload/r2_ioc scenario=R2_DEMO,SCALE_TEST

    ui_path= override location to get UI preload files (default is path + '/ui_assets')
    assets= override location to get OOI asset file (default is path + '/ooi_assets')
    attachments= override location to get file attachments (default is path)

    TODO: constraints defined in multiple tables as list of IDs, but not used
    TODO: support attachments using HTTP URL
"""

__author__ = 'Michael Meisinger, Ian Katz, Thomas Lennan'

import ast
import csv
import re
import requests
import StringIO
import time
import calendar
from interface import objects

from pyon.core.bootstrap import get_service_registry
from pyon.ion.resource import get_restype_lcsm
from pyon.public import CFG, log, ImmediateProcess, iex, IonObject, RT, PRED
from pyon.util.containers import named_any, get_ion_ts
from ion.processes.bootstrap.ui_loader import UILoader
from ion.services.dm.utility.granule_utils import time_series_domain
from ion.util.parameter_yaml_IO import get_param_dict
try:
    import xlrd
except:
    log.warning('failed to import xlrd, cannot use http path')

DEBUG = True

DEFAULT_TIME_FORMAT = "%Y-%m-%dT%H:%M:%S"

### this master URL has the latest changes, but if columns have changed, it may no longer work with this commit of the loader code
MASTER_DOC = "https://docs.google.com/spreadsheet/pub?key=0AttCeOvLP6XMdG82NHZfSEJJOGdQTkgzb05aRjkzMEE&output=xls"

### the URL below should point to a COPY of the master google spreadsheet that works with this version of the loader
TESTED_DOC = "https://docs.google.com/spreadsheet/pub?key=0AgkUKqO5m-ZidG9KelcyeFVOMll0Y1BLckFFME5kdXc&output=xls"
#
### while working on changes to the google doc, use this to run test_loader.py against the master spreadsheet
#TESTED_DOC=MASTER_DOC

class IONLoader(ImmediateProcess):
    """
    """

    COL_SCENARIO = "Scenario"
    COL_ID = "ID"
    COL_OWNER = "owner_id"
    COL_LCSTATE = "lcstate"
    COL_ORGS = "org_ids"

    ID_ORG_ION = "ORG_ION"

    unknown_fields = {} # track unknown fields so we only warn once
    constraint_defs = {} # alias -> value for refs, since not stored in DB
    contact_defs = {} # alias -> value for refs, since not stored in DB

    def on_start(self):
        self.ui_loader = UILoader(self)

        global DEBUG
        op = self.CFG.get("op", None)
        self.path = self.CFG.get("path", TESTED_DOC)
        if self.path=='master':
            self.path = MASTER_DOC
        self.attachment_path = self.CFG.get("attachments", self.path + '/attachments')
        self.asset_path = self.CFG.get("assets", self.path + "/ooi_assets1")
        default_ui_path = self.path if self.path.startswith('http') else self.path + "/ui_assets"
        self.ui_path = self.CFG.get("ui_path", default_ui_path)
        scenarios = self.CFG.get("scenario", None)
        DEBUG = self.CFG.get("debug", False)
        self.loadooi = self.CFG.get("loadooi", False)
        self.loadui = self.CFG.get("loadui", False)
        self.exportui = self.CFG.get("exportui", False)

        self.obj_classes = {}
        self.resource_ids = {}    # Holds a mapping of preload labels to resource ids
        self.existing_resources = None

        self._preload_ids()

        log.info("IONLoader: {op=%s, path=%s, scenario=%s}" % (op, self.path, scenarios))
        if op:
            if op == "load":
                if not scenarios:
                    raise iex.BadRequest("Must provide scenarios to load: scenario=sc1,sc2,...")

                if self.loadooi:
                    self.extract_ooi_assets()
                if self.loadui:
                    specs_path = 'interface/ui_specs.json' if self.exportui else None
                    self.ui_loader.load_ui(self.ui_path, specs_path=specs_path)

                # Load existing resources by preload ID
                self._prepare_incremental()

                items = scenarios.split(',')
                for scenario in items:
                    self.load_ion(scenario)
            elif op == "loadooi":
                self.extract_ooi_assets()
            elif op == "loadooi1":
                self.extract_ooi_assets1()
            elif op == "loadui":
                specs_path = 'interface/ui_specs.json' if self.exportui else None
                self.ui_loader.load_ui(self.ui_path, specs_path=specs_path)
            elif op == "deleteui":
                self.ui_loader.delete_ui()
            else:
                raise iex.BadRequest("Operation unknown")
        else:
            raise iex.BadRequest("No operation specified")

    def on_quit(self):
        pass

    def load_ion(self, scenario):
        log.info("Loading from path: %s" % self.path)
        categories = ['Constraint',
                      'Contact',
                      'User',
                      'Org',
                      'UserRole',
                      'CoordinateSystem',
                      'PlatformModel',
                      'InstrumentModel',
                      'Observatory',
                      'Subsite',
                      'PlatformSite',
                      'InstrumentSite',
                      'StreamDefinition',
                      'PlatformDevice',
                      'InstrumentDevice',
                      'SensorModel',
                      'SensorDevice',
                      'InstrumentAgent',
                      'InstrumentAgentInstance',
                      'DataProcessDefinition',
                      #'IngestionConfiguration',
                      'DataProduct',
                      'DataProcess',
                      'DataProductLink',
                      'Attachment',
                      'WorkflowDefinition',
                      'Workflow',
                      'Deployment', ]


        if self.path.startswith('http'):
            preload_doc_str = requests.get(self.path).content
            log.debug("Fetched URL contents, size=%s", len(preload_doc_str))
            xls_parser = XLSParser()
            self.csv_files = xls_parser.extract_csvs(preload_doc_str)
        else:
            self.csv_files = None

        for category in categories:
            row_do, row_skip = 0, 0

            catfunc_ooi = getattr(self, "_load_%s_OOI" % category, None)
            if self.loadooi and catfunc_ooi:
                log.debug('performing OOI parsing of %s', category)
                catfunc_ooi()

            catfunc = getattr(self, "_load_%s" % category)
            filename = "%s/%s.csv" % (self.path, category)
            log.info("Loading category %s", category)
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
                    rowsc = row[self.COL_SCENARIO]
                    if not scenario == rowsc:
                        row_skip += 1
                        continue
                    row_do += 1

                    log.debug('handling %s row: %r', category, row)
                    catfunc(row)
            except IOError, ioe:
                log.warn("Resource category file %s error: %s" % (filename, str(ioe)), exc_info=True)
            finally:
                if csvfile is not None:
                    csvfile.close()
                    csvfile = None

            log.info("Loaded category %s: %d rows imported, %d rows skipped" % (category, row_do, row_skip))

    def _prepare_incremental(self):
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

    def _create_object_from_row(self, objtype, row, prefix='',
                                constraints=None, constraint_field='constraint_list',
                                contacts=None, contact_field='contact_ids'):
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
                        log.warn("Object type=%s, prefix=%s, field=%s cannot be converted to type=%s. Value=%s", objtype, prefix, fieldname, schema[fieldname]['type'], value, exc_info=True)
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
        if row[self.COL_ID] and 'alt_ids' in schema:
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
            return list(value.split(','))
        else:
            log.trace('parsing value as %s: %s', targettype, value)
            return ast.literal_eval(value)

    def _get_service_client(self, service):
        return get_service_registry().services[service].client(process=self)

    def _register_id(self, alias, resid):
        if alias in self.resource_ids:
            raise iex.BadRequest("ID alias %s used twice" % alias)
        self.resource_ids[alias] = resid
        log.debug("Added resource alias=%s to id=%s", alias, resid)

    def _get_op_headers(self, row):
        headers = {}
        owner_id = row.get(self.COL_OWNER, None)
        if owner_id:
            owner_id = self.resource_ids[owner_id]
            headers['ion-actor-id'] = owner_id
        return headers

    def _basic_resource_create(self, row, restype, prefix, svcname, svcop,
                               constraints=None, constraint_field='constraint_list',
                               contacts=None, contact_field='contacts',
                               **kwargs):
        res_obj = self._create_object_from_row(restype, row, prefix,
                                               constraints=constraints, constraint_field=constraint_field,
                                               contacts=contacts, contact_field=contact_field)
        headers = self._get_op_headers(row)
        svc_client = self._get_service_client(svcname)
        res_id = getattr(svc_client, svcop)(res_obj, headers=headers, **kwargs)
        self._register_id(row[self.COL_ID], res_id)
        self._resource_assign_org(row, res_id)
        return res_id

    def _resource_advance_lcs(self, row, res_id, restype=None):
        """ change lifecycle state of object to DEPLOYED_AVAILABLE """
        lcsm = get_restype_lcsm(restype)
        initial_lcstate = lcsm.initial_state if lcsm else "DEPLOYED_AVAILABLE"

        svc_client = self._get_service_client("resource_registry")

        lcstate = row.get(self.COL_LCSTATE, None)
        if lcstate:
            imat, ivis = initial_lcstate.split("_")
            mat, vis = lcstate.split("_")
            if mat != imat:
                svc_client.set_lifecycle_state(res_id, "%s_PRIVATE" % mat)
            if vis != ivis:
                svc_client.set_lifecycle_state(res_id, "%s_%s" % (mat, vis))

    def _resource_assign_org(self, row, res_id):
        svc_client = self._get_service_client("observatory_management")

        org_ids = row.get(self.COL_ORGS, None)
        if org_ids:
            org_ids = self._get_typed_value(org_ids, targettype="simplelist")
            for org_id in org_ids:
                svc_client.assign_resource_to_observatory_org(res_id, self.resource_ids[org_id])

    def _preload_ids(self):
        if not DEBUG:
            rr_client = self._get_service_client("resource_registry")

            org_ids,_ = rr_client.find_resources(name="ION", restype=RT.Org, id_only=True)
            if not org_ids:
                raise iex.BadRequest("ION org not found. Was system force_cleaned since bootstrap?")
            ion_org_id = org_ids[0]
            self._register_id(self.ID_ORG_ION, ion_org_id)

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

    # --------------------------------------------------------------------------------------------------
    # Add specific types of resources below
    def _load_User(self, row):
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
        user_id = ims.create_actor_identity(actor_identity_obj)
        self._register_id(alias, user_id)

        # Build UserCredentials
        user_credentials_obj = IonObject("UserCredentials", name=subject,
            description="Default credentials for %s" % user_attrs['name'])
        ims.register_user_credentials(user_id, user_credentials_obj)

        # Build UserInfo
        user_info_obj = IonObject("UserInfo", **user_attrs)
        ims.create_user_info(user_id, user_info_obj)

    def _load_Org(self, row):
        log.trace("Loading Org (ID=%s)", row[self.COL_ID])
        contacts = self._get_contacts(row, field='contact_id', type='Org')
        res_obj = self._create_object_from_row("Org", row, "org/")
        if contacts:
            if len(contacts)>1:
                raise iex.BadRequest('Org contact_id should be single value, not list')
            res_obj.contact = contacts[0]
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
            self._register_id(row[self.COL_ID], res_id)

    def _load_UserRole(self, row):
        org_id = row["org_id"]
        if org_id:
            if org_id == self.ID_ORG_ION and DEBUG:
                return
            org_id = self.resource_ids[org_id]

        user_id = self.resource_ids[row["user_id"]]
        role_name = row["role_name"]

        svc_client = self._get_service_client("org_management")

        auto_enroll = self._get_typed_value(row["auto_enroll"], targettype="bool")
        if auto_enroll:
            svc_client.enroll_member(org_id, user_id)

        if role_name != "ORG_MEMBER":
            svc_client.grant_role(org_id, user_id, role_name)

    def _load_PlatformModel(self, row):
        res_id = self._basic_resource_create(row, "PlatformModel", "pm/",
                                            "instrument_management", "create_platform_model")

    def _load_PlatformModel_OOI(self):
        for pm_def in self.platform_models.values():
            fakerow = {}
            fakerow[self.COL_ID] = pm_def['code']
            fakerow['pm/name'] = "%s (%s)" % (pm_def['code'], pm_def['name'])
            fakerow['pm/description'] = pm_def['name']
            fakerow['pm/OOI_node_type'] = pm_def['code']
            mf_id = 'MF_RSN' if pm_def['code'].startswith("R") else 'MF_CGSN'
            fakerow['mf_ids'] = mf_id

            self._load_PlatformModel(fakerow)

    def _load_Contact(self, row):
        """ create constraint IonObject but do not insert into DB,
            cache in dictionary for inclusion in other preload objects """
        id = row[self.COL_ID]
        log.debug('creating contact: ' + id)
        if id in self.contact_defs:
            raise iex.BadRequest('contact with ID already exists: ' + id)
        roles = self._get_typed_value(row['c/roles'], targettype='simplelist')
        del row['c/roles']
        contact = self._create_object_from_row("ContactInformation", row, "c/")
        contact.roles = roles
        self.contact_defs[id] = contact

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

    def _load_SensorModel(self, row):
        row['sm/reference_urls'] = repr(self._get_typed_value(row['sm/reference_urls'], targettype="simplelist"))
        self._basic_resource_create(row, "SensorModel", "sm/",
            "instrument_management", "create_sensor_model")

    def _load_InstrumentModel(self, row):
        row['im/reference_urls'] = repr(self._get_typed_value(row['im/reference_urls'], targettype="simplelist"))
        obj = self._create_object_from_row('InstrumentModel', row, 'im/')
        raw_stream_def = row['raw_stream_def']
        parsed_stream_def = row['parsed_stream_def']
        obj.stream_configuration = { 'raw': raw_stream_def, 'parsed': parsed_stream_def }
        headers = self._get_op_headers(row)
        svc_client = self._get_service_client('instrument_management')
        id = svc_client.create_instrument_model(obj, headers=headers)
        self._register_id(row[self.COL_ID], id)
        self._resource_assign_org(row, id)

    def _load_InstrumentModel_OOI(self):
        for im_def in self.instrument_models.values():
            fakerow = {}
            fakerow[self.COL_ID] = im_def['code']
            fakerow['im/name'] = im_def['name']
            fakerow['im/description'] = im_def['name']
            fakerow['im/instrument_family'] = im_def['family']
            fakerow['im/instrument_class'] = im_def['code']
            fakerow['mf_ids'] = 'MF_RSN,MF_CGSN'

            self._load_InstrumentModel(fakerow)

    def _load_Observatory(self, row):
        constraints = self._get_constraints(row, type='Observatory')
        res_obj = self._create_object_from_row("Observatory", row, "obs/",
            constraints=constraints, constraint_field='constraint_list')
        coordinate_name = row['coordinate_system']
        if coordinate_name:
            res_obj.coordinate_reference_system = self.resource_ids[coordinate_name]
        headers = self._get_op_headers(row)
        svc_client = self._get_service_client("observatory_management")
        res_id = svc_client.create_observatory(res_obj, headers=headers)
        self._register_id(row[self.COL_ID], res_id)
        self._resource_assign_org(row, res_id)


    def _load_Subsite(self, row):
        constraints = self._get_constraints(row, type='Subsite')
        res_obj = self._create_object_from_row("Subsite", row, "site/", constraints=constraints)
        coordinate_name = row['coordinate_system']
        if coordinate_name:
            res_obj.coordinate_reference_system = self.resource_ids[coordinate_name]
        headers = self._get_op_headers(row)
        svc_client = self._get_service_client("observatory_management")
        res_id = svc_client.create_subsite(res_obj, headers=headers)
        self._register_id(row[self.COL_ID], res_id)
        self._resource_assign_org(row, res_id)

        psite_id = row.get("parent_site_id", None)
        if psite_id:
            svc_client.assign_site_to_site(res_id, self.resource_ids[psite_id])

    def _load_Subsite_OOI(self):
        for site_def in self.obs_sites.values():
            fakerow = {}
            fakerow[self.COL_ID] = site_def['code']
            fakerow['obs/name'] = site_def['name']
            fakerow['obs/description'] = site_def['name']
            org_id = 'MF_RSN' if site_def['code'].startswith("R") else 'MF_CGSN'
            fakerow['org_ids'] = org_id
            self._load_Observatory(fakerow)

        for site_def in self.sub_sites.values():
            fakerow = {}
            fakerow[self.COL_ID] = site_def['code']
            fakerow['site/name'] = site_def['name']
            fakerow['site/description'] = site_def['name']
            fakerow['parent_site_id'] = site_def['parent_site']
            org_id = 'MF_RSN' if site_def['code'].startswith("R") else 'MF_CGSN'
            fakerow['org_ids'] = org_id

            self._load_Subsite(fakerow)

    def _load_PlatformSite(self, row):
        constraints = self._get_constraints(row, type='PlatformSite')
        res_obj = self._create_object_from_row("PlatformSite", row, "ps/", constraints=constraints)
        coordinate_name = row['coordinate_system']
        if coordinate_name:
            res_obj.coordinate_reference_system = self.resource_ids[coordinate_name]
        headers = self._get_op_headers(row)
        svc_client = self._get_service_client("observatory_management")
        res_id = svc_client.create_platform_site(res_obj, headers=headers)
        site_id = row["parent_site_id"]
        if site_id:
            svc_client.assign_site_to_site(res_id, self.resource_ids[site_id])
        self._register_id(row[self.COL_ID], res_id)

        pm_ids = row["platform_model_ids"]
        if pm_ids:
            pm_ids = self._get_typed_value(pm_ids, targettype="simplelist")
            for pm_id in pm_ids:
                svc_client.assign_platform_model_to_platform_site(self.resource_ids[pm_id], res_id)

    def _load_PlatformSite_OOI(self):
        for i, lp_def in enumerate(self.logical_platforms.values()):
            fakerow = {}
            fakerow[self.COL_ID] = lp_def['code']
            fakerow['ps/name'] = lp_def['name']+" "+str(i)
            fakerow['ps/description'] = lp_def['name']
            fakerow['parent_site_id'] = lp_def['site']
            fakerow['platform_model_ids'] = lp_def['platform_model']
            org_id = 'MF_RSN' if site_def['code'].startswith("R") else 'MF_CGSN'
            fakerow['org_ids'] = org_id

            self._load_PlatformSite(fakerow)

    def _load_InstrumentSite(self, row):
        constraints = self._get_constraints(row, type='InstrumentSite')

        res_obj = self._create_object_from_row("InstrumentSite", row, "is/",
            constraints=constraints, constraint_field='constraint_list')
        coordinate_name = row['coordinate_system']
        if coordinate_name:
            res_obj.coordinate_reference_system = self.resource_ids[coordinate_name]

        headers = self._get_op_headers(row)
        svc_client = self._get_service_client("observatory_management")
        res_id = svc_client.create_instrument_site(res_obj, headers=headers)
        self._register_id(row[self.COL_ID], res_id)
        self._resource_assign_org(row, res_id)

        lp_id = row["parent_site_id"]
        if lp_id:
            svc_client.assign_site_to_site(res_id, self.resource_ids[lp_id])

        im_ids = row["instrument_model_ids"]
        if im_ids:
            im_ids = self._get_typed_value(im_ids, targettype="simplelist")
            for im_id in im_ids:
                svc_client.assign_instrument_model_to_instrument_site(self.resource_ids[im_id], res_id)

    def _load_InstrumentSite_OOI(self):
        for i, li_def in enumerate(self.logical_instruments.values()):
            fakerow = {}
            fakerow[self.COL_ID] = li_def['code']
            fakerow['is/name'] = li_def['name']+" "+str(i)
            fakerow['is/description'] = li_def['name']
            fakerow['parent_site_id'] = li_def['logical_platform']
            fakerow['instrument_model_ids'] = li_def['instrument_model']
            org_id = 'MF_RSN' if site_def['code'].startswith("R") else 'MF_CGSN'
            fakerow['org_ids'] = org_id

            self._load_InstrumentSite(fakerow)

            if DEBUG and i>20:
                break

    def _load_StreamDefinition(self, row):
        res_obj = self._create_object_from_row("StreamDefinition", row, "sdef/")
#        sd_module = row["StreamContainer_module"]
#        sd_method = row["StreamContainer_method"]
        pname = row["param_dict_name"]
        svc_client = self._get_service_client("dataset_management")
        parameter_dictionary_id = svc_client.read_parameter_dictionary_by_name(pname, id_only=True)
        svc_client = self._get_service_client("pubsub_management")
        res_id = svc_client.create_stream_definition(name=res_obj.name, parameter_dictionary_id=parameter_dictionary_id)
        self._register_id(row[self.COL_ID], res_id)

    def _load_PlatformDevice(self, row):
        res_id = self._basic_resource_create(row, "PlatformDevice", "pd/",
                                            "instrument_management", "create_platform_device")
        ims_client = self._get_service_client("instrument_management")
        ass_id = row["platform_model_id"]
        if ass_id:
            ims_client.assign_platform_model_to_platform_device(self.resource_ids[ass_id], res_id)
        self._resource_advance_lcs(row, res_id, "PlatformDevice")

    def _load_SensorDevice(self, row):
        res_id = self._basic_resource_create(row, "SensorDevice", "sd/",
            "instrument_management", "create_sensor_device")
        ims_client = self._get_service_client("instrument_management")
        ass_id = row["sensor_model_id"]
        if ass_id:
            ims_client.assign_sensor_model_to_sensor_device(self.resource_ids[ass_id], res_id)
        ass_id = row["instrument_device_id"]
        if ass_id:
            ims_client.assign_sensor_device_to_instrument_device(res_id, self.resource_ids[ass_id])
        self._resource_advance_lcs(row, res_id, "SensorDevice")

    def _load_InstrumentDevice(self, row):
        contacts = self._get_contacts(row, field='contact_ids', type='InstrumentDevice')
        res_id = self._basic_resource_create(row, "InstrumentDevice", "id/",
            "instrument_management", "create_instrument_device", contacts=contacts)

#        rr = self._get_service_client("resource_registry")
#        attachment_ids = self._get_typed_value(row['attachment_ids'], targettype="simplelist")
#        if attachment_ids:
#            log.trace('adding attachments to instrument device %s: %r', res_id, attachment_ids)
#            for id in attachment_ids:
#                rr.create_association(res_id, PRED.hasAttachment, self.resource_ids[id])

        ims_client = self._get_service_client("instrument_management")
        ass_id = row["instrument_model_id"]
        if ass_id:
            ims_client.assign_instrument_model_to_instrument_device(self.resource_ids[ass_id], res_id)
        ass_id = row["platform_device_id"]# if 'platform_device_id' in row else None
        if ass_id:
            ims_client.assign_instrument_device_to_platform_device(res_id, self.resource_ids[ass_id])
        self._resource_advance_lcs(row, res_id, "InstrumentDevice")

    def _load_InstrumentAgent(self, row):
        res_id = self._basic_resource_create(row, "InstrumentAgent", "ia/",
                                            "instrument_management", "create_instrument_agent")

        svc_client = self._get_service_client("instrument_management")

        im_ids = row["instrument_model_ids"]
        if im_ids:
            im_ids = self._get_typed_value(im_ids, targettype="simplelist")
            for im_id in im_ids:
                svc_client.assign_instrument_model_to_instrument_agent(self.resource_ids[im_id], res_id)

        self._resource_advance_lcs(row, res_id, "InstrumentAgent")

    def _load_InstrumentAgentInstance(self, row):
        ia_id = row["instrument_agent_id"]
        id_id = row["instrument_device_id"]
        res_id = self._basic_resource_create(row, "InstrumentAgentInstance", "iai/",
                                            "instrument_management", "create_instrument_agent_instance",
                                            instrument_agent_id=self.resource_ids[ia_id],
                                            instrument_device_id=self.resource_ids[id_id])

    def _load_DataProcessDefinition(self, row):
        res_id = self._basic_resource_create(row, "DataProcessDefinition", "dpd/",
                                            "data_process_management", "create_data_process_definition")

        svc_client = self._get_service_client("data_process_management")

        input_strdef = row["input_stream_defs"]
        if input_strdef:
            input_strdef = self._get_typed_value(input_strdef, targettype="simplelist")
        log.trace("Assigning input StreamDefinition to DataProcessDefinition for %s" % input_strdef)
        for insd in input_strdef:
            svc_client.assign_input_stream_definition_to_data_process_definition(self.resource_ids[insd], res_id)

        output_strdef = row["output_stream_defs"]
        if output_strdef:
            output_strdef = self._get_typed_value(output_strdef, targettype="dict")
        for binding, strdef in output_strdef.iteritems():
            svc_client.assign_stream_definition_to_data_process_definition(self.resource_ids[strdef], res_id, binding)
            
        

    def _load_IngestionConfiguration(self, row):
        if DEBUG:
            return

        xp = row["exchange_point_id"]
        name = row["ic/name"]
        ingest_queue = self._create_object_from_row("IngestionQueue",row,"ingestion_queue/")
        svc_client = self._get_service_client("ingestion_management")
        ic_id = svc_client.create_ingestion_configuration(name=name, exchange_point_id=xp, queues=[ingest_queue])

    def _load_DataProduct(self, row):
        tdom, sdom = time_series_domain()

        res_obj = self._create_object_from_row("DataProduct", row, "dp/")
        res_obj.spatial_domain = sdom.dump()
        res_obj.temporal_domain = tdom.dump()
        # HACK: cannot parse CSV value directly when field defined as "list"
        # need to evaluate as simplelist instead and add to object explicitly
        res_obj.available_formats = self._get_typed_value(row['available_formats'], targettype="simplelist")

        svc_client = self._get_service_client("data_product_management")
        stream_definition_id = self.resource_ids[row["stream_def_id"]]
        res_id = svc_client.create_data_product(data_product=res_obj, stream_definition_id=stream_definition_id)
        self._register_id(row[self.COL_ID], res_id)

        if not DEBUG:
            svc_client.activate_data_product_persistence(res_id)
        self._resource_advance_lcs(row, res_id, "DataProduct")

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

        res_id = svc_client.activate_data_process(res_id)

    def _load_DataProductLink(self, row):
        log.info("Loading DataProductLink")

        dp_id = self.resource_ids[row["data_product_id"]]
        res_id = self.resource_ids[row["input_resource_id"]]

        svc_client = self._get_service_client("data_acquisition_management")
        svc_client.assign_data_product(res_id, dp_id)

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

        rr_client = self._get_service_client("resource_registry")
        headers = self._get_op_headers(row)
        att_id = rr_client.create_attachment(res_id, att_obj, headers=headers)
        self._register_id(row[self.COL_ID], att_id)


    # WorkflowDefinition load functions - Added by Raj Singh
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

        #Create it in the resource registry
        workflow_def_id = workflow_client.create_workflow_definition(workflow_def_obj)

        self._register_id(row[self.COL_ID], workflow_def_id)

        return

    # Workflow load functions - Added by Raj Singh
    def _load_Workflow(self,row):
        workflow_obj = self._create_object_from_row("WorkflowDefinition", row, "wf/")
        workflow_client = self._get_service_client("workflow_management")
        workflow_def_id = self.resource_ids[row["wfd_id"]]
        #Create and start the workflow
        workflow_id, workflow_product_id = workflow_client.create_data_process_workflow(workflow_def_id, self.resource_ids[row["in_dp_id"]], timeout=30)

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

        deployment_id = oms.create_deployment(deployment)
        oms.deploy_instrument_site(site_id, deployment_id)
        ims.deploy_instrument_device(device_id, deployment_id)

        activate_str = row['activate'].lower()# if 'activate' in row else None
        activate = activate_str=='true' or activate_str=='yes' or activate_str=='activate'
        if activate:
            oms.activate_deployment(deployment_id)

    def extract_ooi_assets(self):
        if not self.asset_path:
            raise iex.BadRequest("Must provide path for assets: path=dir or assets=dir")
        if self.asset_path.startswith('http'):
            raise iex.BadRequest('Asset path must be local directory, not URL: ' + self.asset_path)

        log.info("Parsing OOI assets from path=%s", self.asset_path)

        categories = [ 'Report2_InstrumentTypes',
                       'Report4_InstrumentsPerSite',
                       'Report1_InstrumentLocations',
                       'Report3_InstrumentTypeByLocation',
                       'Report6_ReferenceDesignatorListWithDepth' ]

        self.obs_sites = {}
        self.sub_sites = {}
        self.platform_models = {}
        self.instrument_models = {}
        self.logical_platforms = {}
        self.logical_instruments = {}

        for category in categories:
            row_do, row_skip = 0, 0

            funcname = "_parse_%s" % category
            catfunc = getattr(self, funcname)
            filename = "%s/%s.csv" % (self.asset_path, category)
            log.debug("Loading category %s from file %s", category, filename)
            try:
                with open(filename, "rb") as csvfile:
                    for i in xrange(9):
                        # Skip the first rows, because they are garbage
                        csvfile.readline()
                    reader = csv.DictReader(csvfile, delimiter=',')
                    for row in reader:
                        row_do += 1

                        catfunc(row)
            except IOError, ioe:
                log.warn("OOI asset file %s error: %s" % (filename, str(ioe)))

            log.debug("Loaded assets %s: %d rows read" % (category, row_do))

    def _parse_Report2_InstrumentTypes(self, row):
        """
        Extract instrument models
        """
        im = dict(name=row["InstrumentTypes"],
                  family=row["Family"],
                  code=row["Class"],
                  instrument_count=row["Count"])

        self.instrument_models[row["Class"]] = im

    def _parse_Report4_InstrumentsPerSite(self, row):
        """
        Extract observatory sites and sub-sites
        """
        observatory = row["Observatory"]
        site_code, site_name = observatory.split(" ", 1)

        # Observatory site
        if site_code not in self.obs_sites:
            site_name = site_name.strip("()")
            site = dict(code=site_code, name=site_name)
            self.obs_sites[site_code] = site

        # Subsite
        subsite = dict(code=row["SubsiteCode"],
            name=row["SubsiteName"],
            instrument_count=row["InstrumentCount"],
            parent_site=site_code)
        self.sub_sites[row["SubsiteCode"]] = subsite

    def _parse_Report1_InstrumentLocations(self, row):
        """
        Extract platform models and logical platforms
        """
        lp_code = row["LocationCode"]
        lp_name = row["SiteName"]
        platform_model = row["NodeType"]

        # Platform model
        pm_code, pm_name = platform_model.split(" ", 1)
        if pm_code not in self.platform_models:
            pm_name = pm_name.strip("()")
            #pm_name = platform_model
            pm = dict(code=pm_code, name=pm_name)
            self.platform_models[pm_code] = pm

        # Logical platform
        site_code,lp_c = lp_code.split("-")
        lp = dict(code=lp_code,
                  name=lp_name,
                  instrument_count=row["InstrumentCount"],
                  platform_model=pm_code,
                  site=site_code)

        if site_code not in self.sub_sites:
            log.warn("Site %s not registered" % site_code)
            if self.sub_sites[site_code]['name'] != site_name:
                log.warn("Registered site %s name %s does not match %s" % (site_code, self.sub_sites[site_code]['name'], site_name))

        assert lp_code not in self.logical_platforms, "Double entry %s" % lp_code
        self.logical_platforms[lp_code] = lp


    def _parse_Report3_InstrumentTypeByLocation(self, row):
        """
        Extracts logical instrument
        """
        lp_code = row["LocationCode"]
        im_code = row["SensorInstrumentClass"]

        li_code = "%s-%s" % (lp_code, im_code)

        # Logical instrument
        li_name = "%s %s" % (row["SubsiteName"], row["SensorInstrumentName"])
        li = dict(code=li_code,
                  name=li_name,
                  sensor_count=row["SensorCount"],
                  instrument_model=im_code,
                  logical_platform=lp_code)

        assert li_code not in self.logical_instruments, "Double entry %s" % li_code
        self.logical_instruments[li_code] = li

    def _parse_Report6_ReferenceDesignatorListWithDepth(self, row):
        """
        Add port information to logical instrument
        """
        rd_code = row["ReferenceDesignator"]
        osite_code, lp_part, port_part, li_part = rd_code.split("-")

        # Logical Instrument
        li_code = "%s-%s-%s" % (osite_code, lp_part, row["InstrumentClass"])
        li = self.logical_instruments[li_code]
        li['port_number'] = row["PortNumber"]
        li['instrument_series'] = row["InstrumentSeries"]
        li['port_min_depth'] = row["PortMinDepth"]
        li['port_max_depth'] = row["PortMaxDepth"]

    def extract_ooi_assets1(self):
        if not self.asset_path:
            raise iex.BadRequest("Must provide path for assets: path=dir or assets=dir")
        if self.asset_path.startswith('http'):
            raise iex.BadRequest('Asset path must be local directory, not URL: ' + self.asset_path)

        log.info("Parsing OOI assets from path=%s", self.asset_path)

        categories = [ 'AttributeReportClass',
                       'AttributeReportDataProducts',
                       'AttributeReportMakeModel',
                       'AttributeReportPorts',
                       'AttributeReportReferenceDesignator',
                       'AttributeReportSubsites',
                       'InstrumentTableDetailed',
                       'InstrumentCatalogFull']

        self.ooi_objects = {}
        self.ooi_obj_attrs = {}
        self.warnings = []

        for category in categories:
            row_do, row_skip = 0, 0

            funcname = "_parse_%s" % category
            catfunc = getattr(self, funcname)
            filename = "%s/%s.csv" % (self.asset_path, category)
            log.debug("Loading category %s from file %s", category, filename)
            try:
                with open(filename, "rb") as csvfile:
                    for i in xrange(9):
                        # Skip the first rows, because they are garbage
                        csvfile.readline()
                    reader = csv.DictReader(csvfile, delimiter=',')
                    for row in reader:
                        row_do += 1

                        catfunc(row)
            except IOError, ioe:
                log.warn("OOI asset file %s error: %s" % (filename, str(ioe)))

            log.debug("Loaded assets %s: %d rows read" % (category, row_do))

        # Post processing
        if self.warnings:
            log.warn("WARNINGS:\n%s", "\n".join(["%s: %s" % (a, b) for a, b in self.warnings]))

        for ot, oo in self.ooi_objects.iteritems():
            log.warn("Type %s has %s entries", ot, len(oo))
            log.warn("Type %s has %s attributes", ot, self.ooi_obj_attrs[ot])
            #print ot
            #print "\n".join(sorted(list(self.ooi_obj_attrs[ot])))

    def _add_object_attribute(self, objtype, objid, key, value, **kwargs):
        if objtype not in self.ooi_objects:
            self.ooi_objects[objtype] = {}
        ot_objects = self.ooi_objects[objtype]
        if objtype not in self.ooi_obj_attrs:
            self.ooi_obj_attrs[objtype] = set()
        ot_obj_attrs = self.ooi_obj_attrs[objtype]

        if objid not in ot_objects:
            ot_objects[objid] = {}
        obj_entry = ot_objects[objid]
        if key:
            if key in obj_entry:
                msg = "duplicate_key: %s.%s has duplicate key: %s (old=%s, new=%s)" % (objtype, objid, key, obj_entry[key], value)
                self.warnings.append((objid, msg))
            else:
                obj_entry[key] = value
            ot_obj_attrs.add(key)
        for okey, oval in kwargs.iteritems():
            if okey in obj_entry and obj_entry[okey] != oval:
                msg = "different_static: %s.%s has different value for key: %s (old=%s, new=%s)" % (objtype, objid, okey, obj_entry[okey], oval)
                self.warnings.append((objid, msg))
            else:
                obj_entry[okey] = oval
            ot_obj_attrs.add(okey)

    def _parse_AttributeReportClass(self, row):
        self._add_object_attribute('class',
            row['Class'], row['Attribute'], row['AttributeValue'],
            Class_Name=row['Class_Name'])

    def _parse_AttributeReportDataProducts(self, row):
        self._add_object_attribute('data_product',
            row['Data_Product_Identifier'], row['Attribute'], row['AttributeValue'],
            Data_Product_Name=row['Data_Product_Name'], Data_Product_Level=row['Data_Product_Level'])

    def _parse_AttributeReportMakeModel(self, row):
        self._add_object_attribute('model',
            row['Make_Model'], row['Attribute'], row['Attribute_Value'],
            Manufacturer=row['Manufacturer'], Make_Model_Description=row['Make_Model_Description'])

    def _parse_AttributeReportPorts(self, row):
        self._add_object_attribute('port',
            row['Port'], row['Attribute'], row['AttributeValue'])

    def _parse_AttributeReportReferenceDesignator(self, row):
        self._add_object_attribute('sensor',
            row['Reference_Designator'], row['Attribute'], row['AttributeValue'], Class=row['Class'])

    def _parse_AttributeReportSubsites(self, row):
        self._add_object_attribute('subsite',
            row['Subsite'], row['Attribute'], row['AttributeValue'], Subsite_Name=row['Subsite_Name'])

    def _parse_InstrumentTableDetailed(self, row):
        refid = row['ReferenceDesignator']
        entry = dict(
            observatory_id=row['LArray_PublicID'],
            subsite_id=row['LSubsite_PublicID'],
            node_type_id=row['NodeType'],
            sensor_class_id=row['SClass_PublicID'],
            model_id=row['MMInstrument_PublicID']
        )
        self._add_object_attribute('sensor',
            refid, None, None, **entry)

    def _parse_InstrumentCatalogFull(self, row):
        #		LSlot_InstrumentSequence			MMInstrument_Manufacturer	MMInstrument_PublicID	MMInstrument_Description	First_Deployment_Date
        refid = row['ReferenceDesignator']
        entry = dict(
            sensor_series=row['SSeries_PublicID'],
            sensor_subseries=row['SSubseries_PublicID']
        )
        self._add_object_attribute('sensor',
            refid, None, None, **entry)

        # Build up the series here
        sid = "%s-%s" % (row['SClass_PublicID'], row['SSeries_PublicID'])
        self._add_object_attribute('series',
            sid, None, None)

        # Build up the subseries here
        ssid = "%s-%s-%s" % (row['SClass_PublicID'], row['SSeries_PublicID'], row['SSubseries_PublicID'])
        ssentry = dict(description=row['SSubseries_Description'])
        self._add_object_attribute('subseries',
            ssid, None, None, **ssentry)

        # Build up the node type here
        ntype_txt = row['Textbox11']
        #re.match('(\w+)\s+\((.+)\)\s*')
        ntype_id = ntype_txt[:2]
        ntype_desc = ntype_txt[3:-1].strip('()')
        self._add_object_attribute('nodetype',
            ntype_id, None, None, description=ntype_desc)

    def _scan_ooi_reference_designators(self):
        pass

class XLSParser(object):

    def extract_csvs(self, file_content):
        sheets = self.extract_worksheets(file_content)
        csv_docs = {}
        for sheet_name, sheet in sheets.iteritems():
            csv_doc = self.dumps_csv(sheet)
            csv_docs[sheet_name] = csv_doc
        return csv_docs

    def extract_worksheets(self, file_content):
        book = xlrd.open_workbook(file_contents=file_content)
        sheets = {}
        formatter = lambda(t,v): self.format_excelval(book,t,v,False)

        for sheet_name in book.sheet_names():
            raw_sheet = book.sheet_by_name(sheet_name)
            data = []
            for row in range(raw_sheet.nrows):
                (types, values) = (raw_sheet.row_types(row), raw_sheet.row_values(row))
                data.append(map(formatter, zip(types, values)))
            sheets[sheet_name] = data
        return sheets

    def dumps_csv(self, sheet):
        stream = StringIO.StringIO()
        csvout = csv.writer(stream, delimiter=',', doublequote=False, escapechar='\\')
        csvout.writerows( map(self.utf8ize, sheet) )
        csv_doc = stream.getvalue()
        stream.close()
        return csv_doc

    def tupledate_to_isodate(self, tupledate):
        (y,m,d, hh,mm,ss) = tupledate
        nonzero = lambda n: n!=0
        date = "%04d-%02d-%02d"  % (y,m,d)    if filter(nonzero, (y,m,d))                else ''
        time = "T%02d:%02d:%02d" % (hh,mm,ss) if filter(nonzero, (hh,mm,ss)) or not date else ''
        return date+time

    def format_excelval(self, book, type, value, wanttupledate):
        if   type == 2: # TEXT
            if value == int(value): value = int(value)
        elif type == 3: # NUMBER
            datetuple = xlrd.xldate_as_tuple(value, book.datemode)
            value = datetuple if wanttupledate else self.tupledate_to_isodate(datetuple)
        elif type == 5: # ERROR
            value = xlrd.error_text_from_code[value]
        return value

    def utf8ize(self, l):
        return [unicode(s).encode("utf-8") if hasattr(s,'encode') else s for s in l]



