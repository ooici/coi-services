#!/usr/bin/env python

"""Process that loads ION resources via service calls based on given definitions"""

__author__ = 'Michael Meisinger, Ian Katz, Thomas Lennan'

import ast
import csv
import uuid
import json

from interface import objects

from pyon.core.bootstrap import get_service_registry
from pyon.datastore.datastore import DatastoreManager
from pyon.ion.resource import get_restype_lcsm
from pyon.public import CFG, log, ImmediateProcess, iex, IonObject, RT, PRED
from pyon.util.containers import named_any, get_ion_ts

DEBUG = True

class IONLoader(ImmediateProcess):
    """
    @see https://confluence.oceanobservatories.org/display/CIDev/R2+System+Preload
    bin/pycc -x ion.processes.bootstrap.ion_loader.IONLoader op=load path=res/preload/r2_ioc scenario=R2_DEMO

    bin/pycc -x ion.processes.bootstrap.ion_loader.IONLoader op=load path=res/preload/lca_demo scenario=LCA_DEMO_PRE
    bin/pycc -x ion.processes.bootstrap.ion_loader.IONLoader op=load path=res/preload/lca_demo scenario=LCA_DEMO_PRE loadooi=True
    bin/pycc -x ion.processes.bootstrap.ion_loader.IONLoader op=load path=res/preload/lca_demo scenario=LCA_DEMO_PRE loadui=True
    bin/pycc -x ion.processes.bootstrap.ion_loader.IONLoader op=loadooi path=res/preload/lca_demo scenario=LCA_DEMO_PRE
    bin/pycc -x ion.processes.bootstrap.ion_loader.IONLoader op=loadui path=res/preload/lca_demo
    """

    COL_SCENARIO = "Scenario"
    COL_ID = "ID"
    COL_OWNER = "owner_id"
    COL_LCSTATE = "lcstate"
    COL_ORGS = "org_ids"

    ID_ORG_ION = "ORG_ION"

    def on_start(self):

        global DEBUG
        op = self.CFG.get("op", None)
        path = self.CFG.get("path", None)
        scenario = self.CFG.get("scenario", None)
        DEBUG = self.CFG.get("debug", False)
        self.loadooi = self.CFG.get("loadooi", False)
        self.loadui = self.CFG.get("loadui", False)

        log.info("IONLoader: {op=%s, path=%s, scenario=%s}" % (op, path, scenario))
        if op:
            if op == "load":
                self.load_ion(path, scenario)
            elif op == "loadooi":
                self.extract_ooi_assets(path)
            elif op == "loadui":
                self.load_ui(path)
            elif op == "deleteui":
                self.delete_ui()
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
        categories = ['User',
                      'Org',
                      'UserRole',
                      'PlatformModel',
                      'InstrumentModel',
                      'Observatory',
                      'Subsite',
                      'PlatformSite',
                      'InstrumentSite',
                      'StreamDefinition',
                      'PlatformDevice',
                      'InstrumentDevice',
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
                      'Deployment',
                      ]

        self.path = path
        self.obj_classes = {}
        self.resource_ids = {}
        self.user_ids = {}

        self._preload_ids()
        if self.loadooi:
            self.extract_ooi_assets(path)

        if self.loadui:
            self.load_ui(path)

        for category in categories:
            row_do, row_skip = 0, 0

            catfunc_ooi = getattr(self, "_load_%s_OOI" % category, None)
            if self.loadooi and catfunc_ooi:
                catfunc_ooi()

            catfunc = getattr(self, "_load_%s" % category)
            filename = "%s/%s.csv" % (path, category)
            log.info("Loading category %s from file %s" % (category, filename))
            try:
                with open(filename, "rb") as csvfile:
                    reader = self._get_csv_reader(csvfile)
                    for row in reader:
                        # Check if scenario applies
                        rowsc = row[self.COL_SCENARIO]
                        if not scenario in rowsc:
                            row_skip += 1
                            continue
                        row_do += 1

                        catfunc(row)
            except IOError, ioe:
                log.warn("Resource category file %s error: %s" % (filename, str(ioe)))

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
                            fieldvalue = self._get_typed_value(value, schema[fieldname])
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

    def _get_typed_value(self, value, schema_entry=None, targettype=None):
        targettype = targettype or schema_entry["type"]
        if schema_entry and 'enum_type' in schema_entry:
            enum_clzz = getattr(objects, schema_entry['enum_type'])
            return enum_clzz._value_map[value]
        elif targettype is 'str':
            return str(value)
        elif targettype is 'bool':
            lvalue = value.lower()
            if lvalue == 'true':
               return True
            elif lvalue == 'false' or lvalue == '':
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
            return ast.literal_eval(value)

    def _get_service_client(self, service):
        return get_service_registry().services[service].client(process=self)

    def _register_id(self, alias, resid):
        if alias in self.resource_ids:
            raise iex.BadRequest("ID alias %s used twice" % alias)
        self.resource_ids[alias] = resid
        log.info("Added resource alias=%s to id=%s" % (alias, resid))

    def _register_user_id(self, name, id):
        self.user_ids[name] = id
        log.info("Added user name|id=%s|%s" % (name, id))

    def _get_op_headers(self, row):
        headers = {}
        owner_id = row.get(self.COL_OWNER, None)
        if owner_id:
            owner_id = self.resource_ids[owner_id]
            headers['ion-actor-id'] = owner_id
        return headers

    def _basic_resource_create(self, row, restype, prefix, svcname, svcop, **kwargs):
        log.info("Loading %s (ID=%s)" % (restype, row[self.COL_ID]))
        res_obj = self._create_object_from_row(restype, row, prefix)
        log.info("%s: %s" % (restype,res_obj))

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

    # --------------------------------------------------------------------------------------------------
    # Add specific types of resources below

    def _load_User(self, row):
        log.info("Loading user")
        subject = row["subject"]
        name = row["name"]
        email = row["email"]

        ims = self._get_service_client("identity_management")

        actor_identity_obj = IonObject("ActorIdentity", {"name": subject})
        user_id = ims.create_actor_identity(actor_identity_obj)
        self._register_user_id(name, user_id)
        self._register_id(row[self.COL_ID], user_id)

        user_credentials_obj = IonObject("UserCredentials", {"name": subject})
        ims.register_user_credentials(user_id, user_credentials_obj)

        user_info_obj = IonObject("UserInfo", {"contact": {"name": name, "email": email}})
        ims.create_user_info(user_id, user_info_obj)

    def _load_Org(self, row):
        log.info("Loading Org (ID=%s)" % (row[self.COL_ID]))
        res_obj = self._create_object_from_row("Org", row, "org/")
        log.info("Org: %s" % (res_obj))

        headers = self._get_op_headers(row)

        org_type = row["org_type"]
        if org_type == "MarineFacility":
            svc_client = self._get_service_client("observatory_management")
            res_id = svc_client.create_marine_facility(res_obj, headers=headers)
        elif org_type == "VirtualObservatory":
            svc_client = self._get_service_client("observatory_management")
            res_id = svc_client.create_virtual_observatory(res_obj, headers=headers)
        else:
            log.warn("Unknown Org type: %s" % org_type)

        self._register_id(row[self.COL_ID], res_id)

    def _load_UserRole(self, row):
        log.info("Loading UserRole")

        rr_client = self._get_service_client("resource_registry")

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
        log.info("Loading OOI PlatformModel assets")
        for pm_def in self.platform_models.values():
            fakerow = {}
            fakerow[self.COL_ID] = pm_def['code']
            fakerow['pm/name'] = "%s (%s)" % (pm_def['code'], pm_def['name'])
            fakerow['pm/description'] = pm_def['name']
            fakerow['pm/OOI_node_type'] = pm_def['code']
            mf_id = 'MF_RSN' if pm_def['code'].startswith("R") else 'MF_CGSN'
            fakerow['mf_ids'] = mf_id

            self._load_PlatformModel(fakerow)

    def _load_InstrumentModel(self, row):
        res_id = self._basic_resource_create(row, "InstrumentModel", "im/",
                                            "instrument_management", "create_instrument_model")

    def _load_InstrumentModel_OOI(self):
        log.info("Loading OOI InstrumentModel assets")
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
        res_id = self._basic_resource_create(row, "Observatory", "obs/",
            "observatory_management", "create_observatory")

    def _load_Subsite(self, row):
        res_id = self._basic_resource_create(row, "Subsite", "site/",
                                            "observatory_management", "create_subsite")

        svc_client = self._get_service_client("observatory_management")
        psite_id = row.get("parent_site_id", None)
        if psite_id:
            svc_client.assign_site_to_site(res_id, self.resource_ids[psite_id])

    def _load_Subsite_OOI(self):
        log.info("Loading OOI Observatory assets")
        for site_def in self.obs_sites.values():
            fakerow = {}
            fakerow[self.COL_ID] = site_def['code']
            fakerow['obs/name'] = site_def['name']
            fakerow['obs/description'] = site_def['name']
            org_id = 'MF_RSN' if site_def['code'].startswith("R") else 'MF_CGSN'
            fakerow['org_ids'] = org_id

            self._load_Observatory(fakerow)

        log.info("Loading OOI Subsite assets")
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
        res_id = self._basic_resource_create(row, "PlatformSite", "ps/",
                                            "observatory_management", "create_platform_site")

        svc_client = self._get_service_client("observatory_management")
        site_id = row["parent_site_id"]
        if site_id:
            svc_client.assign_site_to_site(res_id, self.resource_ids[site_id])

        pm_ids = row["platform_model_ids"]
        if pm_ids:
            pm_ids = self._get_typed_value(pm_ids, targettype="simplelist")
            for pm_id in pm_ids:
                svc_client.assign_platform_model_to_platform_site(self.resource_ids[pm_id], res_id)

    def _load_PlatformSite_OOI(self):
        log.info("Loading OOI PlatformSite assets")
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
        res_id = self._basic_resource_create(row, "InstrumentSite", "is/",
                                            "observatory_management", "create_instrument_site")

        svc_client = self._get_service_client("observatory_management")
        lp_id = row["parent_site_id"]
        if lp_id:
            svc_client.assign_site_to_site(res_id, self.resource_ids[lp_id])

        im_ids = row["instrument_model_ids"]
        if im_ids:
            im_ids = self._get_typed_value(im_ids, targettype="simplelist")
            for im_id in im_ids:
                svc_client.assign_instrument_model_to_instrument_site(self.resource_ids[im_id], res_id)

    def _load_InstrumentSite_OOI(self):
        log.info("Loading OOI InstrumentSite assets")
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
        log.info("Loading StreamDefinition")
        res_obj = self._create_object_from_row("StreamDefinition", row, "sdef/")
        log.info("StreamDefinition: %s" % res_obj)

        sd_module = row["StreamContainer_module"]
        sd_method = row["StreamContainer_method"]
        creator_func = named_any("%s.%s" % (sd_module, sd_method))
        sd_container = creator_func()

        svc_client = self._get_service_client("pubsub_management")
        res_id = svc_client.create_stream_definition(container=sd_container,
                                        name=res_obj.name,
                                        description=res_obj.description)

        self._register_id(row[self.COL_ID], res_id)

    def _load_PlatformDevice(self, row):
        res_id = self._basic_resource_create(row, "PlatformDevice", "pd/",
                                            "instrument_management", "create_platform_device")

#        oms_client = self._get_service_client("observatory_management")
#        ass_ids = row["deployment_lp_ids"]
#        if ass_ids:
#            ass_ids = self._get_typed_value(ass_ids, targettype="simplelist")
#            for ass_id in ass_ids:
#                oms_client.assign_device_to_site(res_id, self.resource_ids[ass_id])

        ims_client = self._get_service_client("instrument_management")
        ass_id = row["platform_model_id"]
        if ass_id:
            ims_client.assign_platform_model_to_platform_device(self.resource_ids[ass_id], res_id)

        self._resource_advance_lcs(row, res_id, "PlatformDevice")

#        ass_id = row["primary_deployment_lp_id"]
        #TODO: we no longer have "primary deployment"
        #if ass_id:
        #    oms_client.deploy_as_primary_platform_device_to_platform_site(res_id, self.resource_ids[ass_id])

    def _load_InstrumentDevice(self, row):
        res_id = self._basic_resource_create(row, "InstrumentDevice", "id/",
                                            "instrument_management", "create_instrument_device")

#        oms_client = self._get_service_client("observatory_management")
#        ass_ids = row["deployment_li_ids"]
#        if ass_ids:
#            ass_ids = self._get_typed_value(ass_ids, targettype="simplelist")
#            for ass_id in ass_ids:
#                oms_client.assign_device_to_site(res_id, self.resource_ids[ass_id])

        ims_client = self._get_service_client("instrument_management")
        ass_id = row["instrument_model_id"]
        if ass_id:
            ims_client.assign_instrument_model_to_instrument_device(self.resource_ids[ass_id], res_id)

#        ass_id = row["platform_device_id"]
#        if ass_id:
#            ims_client.assign_instrument_device_to_platform_device(res_id, self.resource_ids[ass_id])

        self._resource_advance_lcs(row, res_id, "InstrumentDevice")

#        ass_id = row["primary_deployment_li_id"]
        #TODO: we no longer have "primary deployment"
        #if ass_id:
        #    oms_client.deploy_as_primary_instrument_device_to_instrument_site(res_id, self.resource_ids[ass_id])


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
        log.info("Assigning input StreamDefinition to DataProcessDefinition for %s" % input_strdef)
        for insd in input_strdef:
            svc_client.assign_input_stream_definition_to_data_process_definition(self.resource_ids[insd], res_id)

        output_strdef = row["output_stream_defs"]
        if output_strdef:
            output_strdef = self._get_typed_value(output_strdef, targettype="simplelist")
        for outsd in output_strdef:
            svc_client.assign_stream_definition_to_data_process_definition(self.resource_ids[outsd], res_id)

    def _load_IngestionConfiguration(self, row):
        log.info("Loading IngestionConfiguration")
        if DEBUG:
            return

        xp = row["exchange_point_id"]
        name = row["ic/name"]
        ingest_queue = self._create_object_from_row("IngestionQueue",row,"ingestion_queue/")
        #couch_cfg = self._create_object_from_row("CouchStorage", row, "couch_storage/")
        #hdf_cfg = self._create_object_from_row("HdfStorage", row, "hdf_storage/")
        #numw = int(row["number_of_workers"])

        svc_client = self._get_service_client("ingestion_management")
        #ic_id = svc_client.create_ingestion_configuration(xp, couch_cfg, hdf_cfg, numw)
        ic_id = svc_client.create_ingestion_configuration(name=name, exchange_point_id=xp, queues=[ingest_queue])

        #ic_id = svc_client.activate_ingestion_configuration(ic_id)

    def _load_DataProduct(self, row):
        strdef = row["stream_def_id"]

        res_id = self._basic_resource_create(row, "DataProduct", "dp/",
                                            "data_product_management", "create_data_product",
                                            stream_definition_id=self.resource_ids[strdef])

        svc_client = self._get_service_client("data_product_management")
        persist_metadata = self._get_typed_value(row["persist_metadata"], targettype="bool")
        persist_data = self._get_typed_value(row["persist_data"], targettype="bool")
        if persist_metadata or persist_data:
            if not DEBUG:
                svc_client.activate_data_product_persistence(res_id, persist_data, persist_metadata)

        self._resource_advance_lcs(row, res_id, "DataProduct")

    def _load_DataProcess(self, row):
        log.info("Loading DataProcess")

        dpd_id = self.resource_ids[row["data_process_definition_id"]]
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

        create_stream = self._get_typed_value(row["create_stream"], targettype="bool")

        svc_client = self._get_service_client("data_acquisition_management")
        svc_client.assign_data_product(res_id, dp_id, create_stream)

    def _load_Attachment(self, row):
        log.info("Loading Attachment")

        res_id = self.resource_ids[row["resource_id"]]
        att_obj = self._create_object_from_row("Attachment", row, "att/")
        file_path = row["file_path"]
        if file_path:
            file_path = "%s/attachments/%s" % (self.path, file_path)
            try:
                with open(file_path, "rb") as attfile:
                    att_obj.content = attfile.read()
            except IOError, ioe:
                raise iex.BadRequest("Attachment file_path %s error: %s" % (file_path, str(ioe)))

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
            workflow_step_obj = IonObject('DataProcessWorkflowStep', data_process_definition_id=self.resource_ids[step_id],
                persist_process_output_data=False)
            workflow_def_obj.workflow_steps.append(workflow_step_obj)

        #Create it in the resource registry
        workflow_def_id = workflow_client.create_workflow_definition(workflow_def_obj)

        self._register_id(row[self.COL_ID], workflow_def_id)

        return

    # Workflow load functions - Added by Raj Singh
    def _load_Workflow(self,row):
        log.info("Loading Workflow")

        workflow_obj = self._create_object_from_row("WorkflowDefinition", row, "wf/")
        workflow_client = self._get_service_client("workflow_management")

        workflow_def_id = self.resource_ids[row["wfd_id"]]
        #Create and start the workflow
        workflow_id, workflow_product_id = workflow_client.create_data_process_workflow(workflow_def_id, self.resource_ids[row["in_dp_id"]], timeout=30)
        #print " >>>>>>> Workflow_id = ", workflow_id , " workflow_product_id = ", workflow_product_id

    def _load_Deployment(self,row):
        log.info("Loading Deployments")
        deployment = self._create_object_from_row("Deployment", row, "d/")
        device_id = self.resource_ids[row['device_id']]
        site_id = self.resource_ids[row['site_id']]

        oms = self._get_service_client("observatory_management")
        ims = self._get_service_client("instrument_management")

        deployment_id = oms.create_deployment(deployment)
        oms.deploy_instrument_site(site_id, deployment_id)
        ims.deploy_instrument_device(device_id, deployment_id)

    def extract_ooi_assets(self, path):
        if not path:
            raise iex.BadRequest("Must provide path")

        path = path + "/ooi_assets"
        log.info("Start parsing OOI assets from path=%s" % path)
        categories = [
                      'Report2_InstrumentTypes',
                      'Report4_InstrumentsPerSite',
                      'Report1_InstrumentLocations',
                      'Report3_InstrumentTypeByLocation',
                      'Report6_ReferenceDesignatorListWithDepth',
                      ]

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
            filename = "%s/%s.csv" % (path, category)
            log.info("Loading category %s from file %s" % (category, filename))
            try:
                with open(filename, "rb") as csvfile:
                    for i in xrange(9):
                        # Skip the first rows, because they are garbage
                        csvfile.readline()
                    reader = self._get_csv_reader(csvfile)
                    for row in reader:
                        row_do += 1

                        catfunc(row)
            except IOError, ioe:
                log.warn("OOI asset file %s error: %s" % (filename, str(ioe)))

            log.info("Loaded assets %s: %d rows read" % (category, row_do))

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

    # ---------------------------------------------------------------------------

    def delete_ui(self):
        resource_types = [
            'UIInternalResourceType',
            'UIInformationLevel',
            'UIScreenLabel',
            'UIAttribute',
            'UIBlock',
            'UIGroup',
            'UIRepresentation',
            'UIResourceType',
            'UIView',
            'UIBlockAttribute',
            'UIBlockRepresentation',
            'UIGroupBlock',
            'UIViewGroup']

        res_ids = []

        for restype in resource_types:
            res_is_list, _ = self.container.resource_registry.find_resources(restype, id_only=True)
            res_ids.extend(res_is_list)
            log.debug("Found %s resources of type %s" % (len(res_is_list), restype))

        ds = DatastoreManager.get_datastore_instance("resources")
        docs = ds.read_doc_mult(res_ids)

        for doc in docs:
            doc['_deleted'] = True

        ds.create_doc_mult(docs, allow_ids=True)


    def load_ui(self, path):
        """@brief Entry point to the import/generation capabilities from the FileMakerPro database
        CVS files to ION resource objects.
        """
        # Delete old UI objects first
        self.delete_ui()

        if not path:
            raise iex.BadRequest("Must provide path")

        path = path + "/ui_assets"
        log.info("Start parsing UI assets from path=%s" % path)
        categories = [
            ('Internal Resource Type.csv', 'InternalResourceType'),
            ('Resource.csv', 'ResourceType'),
            ('Information Level.csv', 'InformationLevel'),
            ('Attribute.csv', 'Attribute'),
            ('Block.csv', 'Block'),
            ('Group.csv', 'Group'),
            ('Representation.csv', 'Representation'),
            ('View.csv', 'View'),
            ('Screen Label.csv', 'ScreenLabel'),
            ('_jt_Block_Attribute.csv', 'BlockAttribute'),
            ('_jt_Block_Representation.csv', 'BlockRepresentation'),
            ('_jt_Group_Block.csv', 'GroupBlock'),
            ('_jt_View_Group.csv', 'ViewGroup'),
            ]

        self.uiid_prefix = uuid.uuid4().hex[:9] + "_"
        self.ui_objs = {}
        self.ui_obj_by_id = {}
        self.ref_assocs = []
        self.ui_assocs = []

        for filename, category in categories:
            row_do, row_skip = 0, 0

            catfunc = getattr(self, "_loadui_%s" % category)
            filename = "%s/%s" % (path, filename)
            log.info("Loading UI category %s from file %s" % (category, filename))
            try:
                with open(filename, "rb") as csvfile:
                    reader = self._get_csv_reader(csvfile)
                    for row in reader:
                        catfunc(row)
                        row_do += 1
            except IOError, ioe:
                log.warn("UI category file %s error: %s" % (filename, str(ioe)))

            log.info("Loaded UI category %s: %d rows imported, %d rows skipped" % (category, row_do, row_skip))

        try:
            ds = DatastoreManager.get_datastore_instance("resources")
            self._finalize_uirefs(ds)
            res = ds.create_mult(self.ui_obj_by_id.values(), allow_ids=True)
            log.info("Loaded %s UI resource objects into resource registry" % (len(res)))
            res = ds.create_mult(self.ui_assocs)
            log.info("Loaded %s UI resource associations into resource registry" % (len(res)))
        except Exception as ex:
            log.exception("load error err=%s" % (str(ex)))

    def _add_ui_object(self, refid, obj):
        while refid in self.ui_objs:
            log.warn("Object duplicate id=%s, obj=%s" % (refid, obj))
            refid = refid + "!"

        if not refid:
            log.warn("Object has no UI refid: %s" % obj)
        else:
            self.ui_objs[refid] = obj

    def _add_ui_refassoc(self, sub_refid, predicate, obj_refid):
        # Create a pre-association based on UI refids (not object IDs)
        if not sub_refid or not obj_refid:
            log.warn("Association not complete: %s (%s) -> %s" % (sub_refid, predicate, obj_refid))
        else:
            refassoc = (sub_refid, predicate, obj_refid)
            self.ref_assocs.append(refassoc)

    def _get_uiid(self, refid):
        return refid

    def _finalize_uirefs(self, ds):
        # Create real resource IDs
        for obj in self.ui_objs.values():
            oid = self.uiid_prefix + obj.uirefid
            obj._id = oid
            self.ui_obj_by_id[oid] = obj

            # Change references for all known UI objects
            for attr in obj.__dict__:
                if attr != 'uirefid' and getattr(obj, attr) in self.ui_objs:
                    setattr(obj, attr, self.uiid_prefix + getattr(obj, attr))
            try:
                json.dumps(obj.__dict__.copy())
            except Exception as ex:
                log.exception("Object %s problem" % obj)

        # Resolve associations to real resource IDs
        for refassoc in self.ref_assocs:
            sub_refid, pred, obj_refid = refassoc
            try:
                subo = self.ui_objs[sub_refid]
                objo = self.ui_objs[obj_refid]
                assoc = objects.Association(at="",
                    s=subo._id, st=subo._get_type(), srv="",
                    p=pred,
                    o=objo._id, ot=objo._get_type(), orv="",
                    ts=get_ion_ts())

                self.ui_assocs.append(assoc)
            except Exception as ex:
                log.warn("Cannot create association for subject=%s pred=%s object=%s: %s" % (sub_refid, pred, obj_refid, ex))

    def _build_ui_resource(self, row, objtype, mapping, auto_add=True):
        refid = None
        obj_fields = {}
        for obj_attr, row_attr in mapping.iteritems():
            row_val = row[row_attr]
            obj_fields[obj_attr] = row_val
            if obj_attr == "uirefid":
                refid = row_val

        obj = IonObject(objtype, **obj_fields)

        if 'name' in obj_attr and not obj.name:
            log.warn("Ignoring object with no name: %s" % obj)
        else:
            if auto_add:
                self._add_ui_object(refid, obj)

        return refid, obj

    def _loadui_InternalResourceType(self, row):
        refid, obj = self._build_ui_resource(row, "UIInternalResourceType",
                {'uirefid':'__pk_InternalResourceType_ID',
                 'name':'Name',
                 'internal_resource_superclass':'Internal Resource Superclass',
                 'alignment_status':'Alignment Status'})

        self._add_ui_refassoc(refid, "hasUISupertype", obj.internal_resource_superclass)

    def _loadui_ResourceType(self, row):
        refid, obj = self._build_ui_resource(row, "UIResourceType",
                {'uirefid':'__pk_Resource_ID',
                 'name':'Name',
                 'screen_label_id':'_fk_ScreenLabel_ID'})

        self._add_ui_refassoc(refid, "hasUIScreenLabel", obj.screen_label_id)

    def _loadui_InformationLevel(self, row):
        refid, obj = self._build_ui_resource(row, "UIInformationLevel",
                {'uirefid':'__pk_InformationLevel_ID',
                 'level':'Level',
                 'description':'Description'})

    def _loadui_ScreenLabel(self, row):
        refid, obj = self._build_ui_resource(row, "UIScreenLabel",
                {'uirefid':'__pk_ScreenLabel_ID',
                 'text':'Text',
                 'abbreviation':'Abbreviation'})

    def _loadui_Attribute(self, row):
        refid, obj = self._build_ui_resource(row, "UIAttribute",
                {'uirefid':'__pk_Attribute_ID',
                 'name':'Name',
                 'information_level_id':'_fk_InformationLevel_ID',
                 'screen_label_id':'_fk_ScreenLabel_ID',
                 'internal_resource_type_id':'_fk_InternalResourceType_ID',
                 'value_generation':'Value Generation',
                 'alignment_status':'Alignment Status'})

        # TODO: Only accepted attributes, only Value (not Computed)

    def _loadui_View(self, row):
        refid, obj = self._build_ui_resource(row, "UIView",
                {'uirefid':'__pk_View_ID',
                 'name':'Name',
                 'description':'Description',
                 'screen_label_id':'_fk_ScreenLabel_ID'})

        self._add_ui_refassoc(refid, "hasUIScreenLabel", obj.screen_label_id)

    def _loadui_Group(self, row):
        refid, obj = self._build_ui_resource(row, "UIGroup",
                {'uirefid':'__pk_Group_ID',
                 'name':'Name',
                 'description':'Description',
                 'screen_label_id':'_fk_ScreenLabel_ID'})

        self._add_ui_refassoc(refid, "hasUIScreenLabel", obj.screen_label_id)

    def _loadui_Block(self, row):
        refid, obj = self._build_ui_resource(row, "UIBlock",
                {'uirefid':'__pk_Block_ID',
                 'name':'Name',
                 'resource_id':'_fk_Resource_ID',
                 'group_id':'_fk_Group_ID',
                 'screen_label_id':'_fk_ScreenLabel_ID'})

        self._add_ui_refassoc(refid, "hasUIResource", obj.resource_id)
        self._add_ui_refassoc(refid, "hasUIGroup", obj.group_id)
        self._add_ui_refassoc(refid, "hasUIScreenLabel", obj.screen_label_id)

    def _loadui_Representation(self, row):
        refid, obj = self._build_ui_resource(row, "UIRepresentation",
                {'uirefid':'__pk_Representation_ID',
                 'name':'Name',
                 'description':'Description',
                 'screen_label_id':'_fk_ScreenLabel_ID'})

        self._add_ui_refassoc(refid, "hasUIScreenLabel", obj.screen_label_id)

    def _loadui_BlockAttribute(self, row):
        refid, obj = self._build_ui_resource(row, "UIBlockAttribute",
                {'uirefid':'__pk_jt_Block_Attribute',
                 'block_id':'_fk_Block_ID',
                 'attribute_id':'_fk_Attribute_ID',
                 'position':'Position'})

        if not obj.block_id or not obj.attribute_id:
            log.warn("Ignoring UIBlockAttribute: Both FK must be set")
        else:
            self._add_ui_refassoc(obj.attribute_id, "hasUIBlockAttribute", refid)
            self._add_ui_refassoc(obj.block_id, "hasUIBlockAttribute", refid)
            self._add_ui_refassoc(obj.block_id, "hasUIAttribute", obj.attribute_id)

    def _loadui_BlockRepresentation(self, row):
        refid, obj = self._build_ui_resource(row, "UIBlockRepresentation",
                {'uirefid':'__pk_jt_Block_Representation',
                 'block_id':'_fk_Block_ID',
                 'representation_id':'_fk_Representation_ID'})

        if not obj.block_id or not obj.representation_id:
            log.warn("Ignoring UIBlockRepresentation: Both FK must be set")
        else:
            self._add_ui_refassoc(obj.block_id, "hasUIBlockRepresentation", refid)
            self._add_ui_refassoc(obj.representation_id, "hasUIBlockRepresentation", refid)
            self._add_ui_refassoc(obj.block_id, "hasUIRepresentation", obj.representation_id)

    def _loadui_GroupBlock(self, row):
        refid, obj = self._build_ui_resource(row, "UIGroupBlock",
                {'uirefid':'__pk_jt_Group_Block_ID',
                 'block_id':'_fk_Block_ID',
                 'group_id':'_fk_Group_ID'})

        if not obj.block_id or not obj.group_id:
            log.warn("Ignoring UIGroupBlock: Both FK must be set")
        else:
            self._add_ui_refassoc(obj.group_id, "hasUIGroupBlock", refid)
            self._add_ui_refassoc(obj.block_id, "hasUIGroupBlock", refid)
            self._add_ui_refassoc(obj.group_id, "hasUIBlock", obj.block_id)


    def _loadui_ViewGroup(self, row):
        refid, obj = self._build_ui_resource(row, "UIViewGroup",
                {'uirefid':'__pk_jt_View_Group_ID',
                 'view_id':'_fk_View_ID',
                 'group_id':'_fk_Group_ID'})

        if not obj.view_id or not obj.group_id:
            log.warn("Ignoring UIViewGroup: Both FK must be set")
        else:
            self._add_ui_refassoc(obj.view_id, "hasUIViewGroup", refid)
            self._add_ui_refassoc(obj.group_id, "hasUIViewGroup", refid)
            self._add_ui_refassoc(obj.view_id, "hasUIGroup", obj.group_id)
