#!/usr/bin/env python

"""Process that loads ION resources via service calls based on given definitions"""

__author__ = 'Michael Meisinger, Ian Katz, Thomas Lennan'

import ast
import csv
import re

from interface import objects

from pyon.core.bootstrap import service_registry
from pyon.public import CFG, log, ImmediateProcess, iex, IonObject
from pyon.util.containers import named_any

DEBUG = True

class IONLoader(ImmediateProcess):
    """
    @see https://confluence.oceanobservatories.org/display/CIDev/R2+System+Preload
    bin/pycc -x ion.processes.bootstrap.ion_loader.IONLoader op=load path=res/preload/lca_demo scenario=LCA_DEMO_PRE
    """

    COL_SCENARIO = "Scenario"
    COL_ID = "ID"
    COL_OWNER = "owner_id"

    def on_start(self):

        if self.CFG.system.force_clean and not self.CFG.system.testing and not DEBUG:
            text = "system.force_clean=True. ION Preload does not support this"
            log.error(text)
            raise iex.BadRequest(text)
        op = self.CFG.get("op", None)
        path = self.CFG.get("path", None)
        scenario = self.CFG.get("scenario", None)
        global DEBUG
        DEBUG = self.CFG.get("debug", False)

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
        categories = ['User',
                      'PlatformModel',
                      'InstrumentModel',
                      'MarineFacility',
                      'UserRole',
                      'Site',
                      'LogicalPlatform',
                      'LogicalInstrument',
                      'StreamDefinition',
                      'PlatformDevice',
                      'InstrumentDevice',
                      'InstrumentAgent',
                      'InstrumentAgentInstance',
                      'DataProcessDefinition',
                      'IngestionConfiguration',
                      'DataProduct',
                      'DataProcess',
                      'DataProductLink',
                      'Attachment',
                      ]

        self.obj_classes = {}
        self.resource_ids = {}
        self.user_ids = {}

        for category in categories:
            row_do, row_skip = 0, 0

            funcname = "_load_%s" % category
            catfunc = getattr(self, funcname)
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
        if targettype is 'str':
            return str(value)
        elif targettype is 'bool':
            lvalue = value.lower()
            if lvalue == 'true':
               return True
            elif lvalue == 'false' or lvalue == '':
                return False
            else:
                raise iex.BadRequest("Value %s is no bool" % value)
        elif targettype is 'simplelist':
            if value.startswith('[') and value.endswith(']'):
                value = value[1:len(value)-1]
            return list(value.split(','))
        elif schema_entry and 'enum_type' in schema_entry:
            enum_clzz = getattr(objects, schema_entry['enum_type'])
            return enum_clzz._value_map[value]
        else:
            return ast.literal_eval(value)

    def _get_service_client(self, service):
        return service_registry.services[service].client(process=self)

    def _register_id(self, alias, resid):
        if alias in self.resource_ids:
            raise iex.BadRequest("ID alias %s used twice" % alias)
        self.resource_ids[alias] = resid
        log.info("Added resource alias=%s to id=%s" % (alias, resid))

    def _register_user_id(self, name, id):
        self.user_ids[name] = id
        log.info("Added user name|id=%s|%s" % (name, id))

    def _basic_resource_create(self, row, restype, prefix, svcname, svcop, **kwargs):
        log.info("Loading %s (ID=%s)" % (restype, row[self.COL_ID]))
        res_obj = self._create_object_from_row(restype, row, prefix)
        log.info("%s: %s" % (restype,res_obj))

        headers = {}
        owner_id = row.get(self.COL_OWNER, None)
        if owner_id:
            owner_id = self.resource_ids[owner_id]
            headers['ion-actor-id'] = owner_id

        svc_client = self._get_service_client(svcname)
        res_id = getattr(svc_client, svcop)(res_obj, headers=headers, **kwargs)
        self._register_id(row[self.COL_ID], res_id)
        return res_id

    # --------------------------------------------------------------------------------------------------
    # Add specific types of resources below

    def _load_User(self, row):
        log.info("Loading user")
        subject = row["subject"]
        name = row["name"]
        email = row["email"]

        ims = self._get_service_client("identity_management")

        user_identity_obj = IonObject("UserIdentity", {"name": subject})
        user_id = ims.create_user_identity(user_identity_obj)
        self._register_user_id(name, user_id)
        self._register_id(row[self.COL_ID], user_id)

        user_credentials_obj = IonObject("UserCredentials", {"name": subject})
        ims.register_user_credentials(user_id, user_credentials_obj)

        user_info_obj = IonObject("UserInfo", {"contact": {"name": name, "email": email}})
        ims.create_user_info(user_id, user_info_obj)

    def _load_PlatformModel(self, row):
        res_id = self._basic_resource_create(row, "PlatformModel", "pm/",
                                            "instrument_management", "create_platform_model")

    def _load_InstrumentModel(self, row):
        res_id = self._basic_resource_create(row, "InstrumentModel", "im/",
                                            "instrument_management", "create_instrument_model")

    def _load_MarineFacility(self, row):
        res_id = self._basic_resource_create(row, "MarineFacility", "mf/",
                                            "marine_facility_management", "create_marine_facility")

    def _load_UserRole(self, row):
        log.info("Loading UserRole")

    def _load_Site(self, row):
        res_id = self._basic_resource_create(row, "Site", "site/",
                                            "marine_facility_management", "create_site")

        svc_client = self._get_service_client("marine_facility_management")
        mf_id = row["marine_facility_id"]
        psite_id = row["parent_site_id"]
        if mf_id:
            svc_client.assign_site_to_marine_facility(res_id, self.resource_ids[mf_id])
        elif psite_id:
            svc_client.assign_site_to_site(res_id, self.resource_ids[psite_id])

    def _load_LogicalPlatform(self, row):
        res_id = self._basic_resource_create(row, "LogicalPlatform", "lp/",
                                            "marine_facility_management", "create_logical_platform")

        svc_client = self._get_service_client("marine_facility_management")
        site_id = row["site_id"]
        svc_client.assign_logical_platform_to_site(res_id, self.resource_ids[site_id])

        #ims_client = self._get_service_client("instrument_management")
        pm_ids = row["platform_model_ids"]
        if pm_ids:
            pm_ids = self._get_typed_value(pm_ids, targettype="simplelist")
            for pm_id in pm_ids:
                svc_client.assign_platform_model_to_logical_platform(self.resource_ids[pm_id], res_id)

    def _load_LogicalInstrument(self, row):
        res_id = self._basic_resource_create(row, "LogicalInstrument", "li/",
                                            "marine_facility_management", "create_logical_instrument")

        svc_client = self._get_service_client("marine_facility_management")
        lp_id = row["logical_platform_id"]
        svc_client.assign_logical_instrument_to_logical_platform(res_id, self.resource_ids[lp_id])

        #ims_client = self._get_service_client("instrument_management")
        im_ids = row["instrument_model_ids"]
        if im_ids:
            im_ids = self._get_typed_value(im_ids, targettype="simplelist")
            for im_id in im_ids:
                svc_client.assign_instrument_model_to_logical_instrument(self.resource_ids[im_id], res_id)

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

        ims_client = self._get_service_client("instrument_management")
        ass_ids = row["deployment_lp_ids"]
        if ass_ids:
            ass_ids = self._get_typed_value(ass_ids, targettype="simplelist")
            for ass_id in ass_ids:
                ims_client.deploy_platform_device_to_logical_platform(res_id, self.resource_ids[ass_id])

        ass_id = row["primary_deployment_lp_id"]
        if ass_id:
            ims_client.deploy_as_primary_platform_device_to_logical_platform(res_id, self.resource_ids[ass_id])

        ass_id = row["platform_model_id"]
        if ass_id:
            ims_client.assign_platform_model_to_platform_device(self.resource_ids[ass_id], res_id)

    def _load_InstrumentDevice(self, row):
        res_id = self._basic_resource_create(row, "InstrumentDevice", "id/",
                                            "instrument_management", "create_instrument_device")

        ims_client = self._get_service_client("instrument_management")
        ass_ids = row["deployment_li_ids"]
        if ass_ids:
            ass_ids = self._get_typed_value(ass_ids, targettype="simplelist")
            for ass_id in ass_ids:
                ims_client.deploy_instrument_device_to_logical_instrument(res_id, self.resource_ids[ass_id])

        ass_id = row["primary_deployment_li_id"]
        if ass_id:
            ims_client.deploy_as_primary_instrument_device_to_logical_instrument(res_id, self.resource_ids[ass_id])

        ass_id = row["instrument_model_id"]
        if ass_id:
            ims_client.assign_instrument_model_to_instrument_device(self.resource_ids[ass_id], res_id)

    def _load_InstrumentAgent(self, row):
        res_id = self._basic_resource_create(row, "InstrumentAgent", "ia/",
                                            "instrument_management", "create_instrument_agent")

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
        couch_cfg = self._create_object_from_row("CouchStorage", row, "couch_storage/")
        hdf_cfg = self._create_object_from_row("HdfStorage", row, "hdf_storage/")
        numw = int(row["number_of_workers"])

        svc_client = self._get_service_client("ingestion_management")
        ic_id = svc_client.create_ingestion_configuration(xp, couch_cfg, hdf_cfg, numw)

        ic_id = svc_client.activate_ingestion_configuration(ic_id)

    def _load_DataProduct(self, row):
        strdef = row["stream_def_id"]

        res_id = self._basic_resource_create(row, "DataProduct", "dp/",
                                            "data_product_management", "create_data_product",
                                            stream_definition_id=self.resource_ids[strdef])

        svc_client = self._get_service_client("data_product_management")
        persist_metadata = self._get_typed_value(row["persist_metadata"], targettype="bool")
        persist_data = self._get_typed_value(row["persist_data"], targettype="bool")
        if DEBUG:
            return
        if persist_metadata or persist_data:
            svc_client.activate_data_product_persistence(res_id, persist_data, persist_metadata)
            pass

    def _load_DataProcess(self, row):
        log.info("Loading DataProcess")

        dpd_id = self.resource_ids[row["data_process_definition_id"]]
        in_data_product_id = self.resource_ids[row["in_data_product_id"]]
        out_data_products = row["out_data_products"]
        if out_data_products:
            out_data_products = self._get_typed_value(out_data_products, targettype="dict")
            for name, dp_id in out_data_products.iteritems():
                out_data_products[name] = self.resource_ids[dp_id]

        svc_client = self._get_service_client("data_process_management")

        res_id = svc_client.create_data_process(dpd_id, in_data_product_id, out_data_products)
        self._register_id(row[self.COL_ID], res_id)

    def _load_DataProductLink(self, row):
        log.info("Loading DataProductLink")

        dp_id = self.resource_ids[row["data_product_id"]]
        res_id = self.resource_ids[row["input_resource_id"]]

        svc_client = self._get_service_client("data_acquisition_management")
        svc_client.assign_data_product(res_id, dp_id, False)

    def _load_Attachment(self, row):
        pass
