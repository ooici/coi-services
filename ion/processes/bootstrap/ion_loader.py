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
        categories = ['User',
                      'MarineFacility',
                      'Site',
                      'LogicalPlatform',
                      'LogicalInstrument',
                      'PlatformModel',
                      'InstrumentModel',
                      'StreamDefinition',
                      'DataProcessDefinition',
                      'DataProduct',
                      'DataProcess',
                      ]

        self.obj_classes = {}
        self.resource_ids = {}
        self.user_ids = {}

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
                    elif category == "StreamDefinition":
                        self._load_stream_definition(row)
                    elif category == "DataProcessDefinition":
                        self._load_data_process_definition(row)
                    elif category == "DataProduct":
                        self._load_data_product(row)
                    elif category == "DataProcess":
                        self._load_data_process(row)
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
        if value.lower() == 'false':
            return False
        elif value.lower() == 'true':
            return True
        elif targettype is 'simplelist':
            if value.startswith('[') and value.endswith(']'):
                value = value[1:len(value)-1]
            return list(value.split(','))
        elif schema_entry and 'enum_type' in schema_entry:
            enum_clzz = getattr(objects, schema_entry['enum_type'])
            return enum_clzz._value_map[value]
#        elif targettype is 'dicteval':
#            return eval(value)
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

    # --------------------------------------------------------------------------------------------------
    # Add specific types of resources below

    def _load_user(self, row):
        log.info("Loading user")
        subject = row["subject"]
        name = row["name"]
        email = row["email"]

        ims = self._get_service_client("identity_management")

        user_identity_obj = IonObject("UserIdentity", {"name": subject})
        user_id = ims.create_user_identity(user_identity_obj)
        self._register_user_id(name, user_id)

        user_credentials_obj = IonObject("UserCredentials", {"name": subject})
        ims.register_user_credentials(user_id, user_credentials_obj)

        user_info_obj = IonObject("UserInfo", {"contact": {"name": name, "email": email}})
        ims.create_user_info(user_id, user_info_obj)

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
        psite_id = row["parent_site_id"]
        if mf_id:
            mfms.assign_site_to_marine_facility(site_id, self.resource_ids[mf_id])
        elif psite_id:
            mfms.assign_site_to_site(site_id, self.resource_ids[psite_id])

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

    def _load_stream_definition(self, row):
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

    def _load_data_process_definition(self, row):
        log.info("Loading DataProcessDefinition")
        res_obj = self._create_object_from_row("DataProcessDefinition", row, "dpd/")
        log.info("DataProcessDefinition: %s" % res_obj)

        svc_client = self._get_service_client("data_process_management")
        res_id = svc_client.create_data_process_definition(res_obj)
        self._register_id(row[self.COL_ID], res_id)

        input_strdef = row["input_stream_defs"]
        if input_strdef:
            input_strdef = self._get_typed_value(input_strdef, targettype="simplelist")

        output_strdef = row["output_stream_defs"]
        if output_strdef:
            output_strdef = self._get_typed_value(output_strdef, targettype="simplelist")

        # TODO: How to assign stream defs?

    def _load_data_product(self, row):
        log.info("Loading DataProduct")
        res_obj = self._create_object_from_row("DataProduct", row, "dp/")
        log.info("DataProduct: %s" % res_obj)

        svc_client = self._get_service_client("data_product_management")
        res_id = svc_client.create_data_product(data_product=res_obj)
        self._register_id(row[self.COL_ID], res_id)

        # TODO: What to do with streamdef?
        strdef = row["stream_def_id"]


    def _load_data_process(self, row):
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
