#!/usr/bin/env python

"""Parses OOI SAF Instrument Application assets from CSV reports."""

__author__ = 'Michael Meisinger'

import csv
import datetime
import os.path
import re

from pyon.public import log, iex
from ion.core.ooiref import OOIReferenceDesignator
from pyon.datastore.datastore import DatastoreManager, DataStore
from ion.util.geo_utils import GeoUtils
from ion.util.xlsparser import XLSParser

class OOILoader(object):
    def __init__(self, process, container=None, asset_path=None):
        self.process = process
        self.container = container or self.process.container
        self.asset_path = asset_path
        self._extracted = False

    def extract_ooi_assets(self):
        """
        Parses SAF Instrument Application export CSV files into intermediate memory structures.
        This information can later be loaded in to actual load_ion() function.
        """
        if self._extracted:
            return

        if not self.asset_path:
            raise iex.BadRequest("Must provide path for assets: path=dir or assets=dir")
        if self.asset_path.startswith('http'):
            raise iex.BadRequest('Asset path must be local directory, not URL: ' + self.asset_path)

        log.info("Parsing OOI assets from path=%s", self.asset_path)

        categories = [ # Mapping spreadsheet early
                       'NodeTypes',
                       # Core concept attributes
                       'AttributeReportArrays',
                       'AttributeReportClass',
                       'AttributeReportDataProducts',
                       'AttributeReportFamilies',
                       'AttributeReportMakeModel',
                       'AttributeReportNodes',
                       'AttributeReportPorts',
                       'AttributeReportReferenceDesignator',
                       'AttributeReportSeries',
                       'AttributeReportSites',
                       'AttributeReportSubseries',
                       'AttributeReportSubsites',
                       # Additional attributes and links taken from aggregate reports
                       'NodeTypes',
                       'InstrumentCatalogFull',
                       'DataQCLookupTables',
                       'DataProductSpreadsheet',
                       'AllSensorTypeCounts',
                       # Tabs from the mapping spreadsheet
                       'Arrays',
                       'Sites',
                       'Subsites',
                       'NTypes',
                       'Nodes',
                       'PlatformAgents',
                       'Series',
                       'InstAgents',
                       'InstAvail',
        ]

        # Holds the object representations of parsed OOI assets by type
        self.ooi_objects = {}
        # Holds a list of attribute names of OOI assets by type
        self.ooi_obj_attrs = {}
        self.warnings = []
        self.csv_files = None

        # Load OOIResourceMappings.xlsx
        mapping_file = self.asset_path + "/OOIResourceMappings.xlsx"
        if os.path.exists(mapping_file):
            with open(mapping_file, "rb") as f:
                preload_doc_str = f.read()
                log.debug("Loaded %s mapping file, size=%s", mapping_file, len(preload_doc_str))
                xls_parser = XLSParser()
                self.csv_files = xls_parser.extract_csvs(preload_doc_str)

        for category in categories:
            row_do, row_skip = 0, 0

            catfunc = getattr(self, "_parse_%s" % category)
            filename = "%s/%s.csv" % (self.asset_path, category)
            log.debug("Loading category %s from file %s", category, filename)
            try:
                if category in self.csv_files:
                    csv_doc = self.csv_files[category]
                    reader = csv.DictReader(csv_doc, delimiter=',')
                    filename = mapping_file + ":" + category
                else:
                    csvfile = open(filename, "rb")
                    for i in xrange(9):
                        # Skip the first rows, because they are garbage
                        csvfile.readline()
                    reader = csv.DictReader(csvfile, delimiter=',')

                for row in reader:
                    row_do += 1
                    catfunc(row)
            except IOError as ioe:
                log.warn("OOI asset file %s error: %s" % (filename, str(ioe)))

            log.debug("Loaded assets %s: %d rows read" % (category, row_do))

        # Post processing
        self._post_process()

        # Do some validation checking
        self._perform_ooi_checks()

        if self.warnings:
            log.warn("WARNINGS:\n%s", "\n".join(["%s: %s" % (a, b) for a, b in self.warnings]))

        for ot, oo in self.ooi_objects.iteritems():
            log.info("Type %s has %s entries", ot, len(oo))
            #import pprint
            #pprint.pprint(oo)
            #log.debug("Type %s has %s attributes", ot, self.ooi_obj_attrs[ot])
            #print ot
            #print "\n".join(sorted(list(self.ooi_obj_attrs[ot])))

        self._extracted = True

    def get_type_assets(self, objtype):
        return self.ooi_objects.get(objtype, None)

    def _add_object_attribute(self, objtype, objid, key, value, value_is_list=False, list_dup_ok=False, change_ok=False, mapping=None, **kwargs):
        """
        Add a single attribute to an identified object of given type. Create object/type on first occurrence.
        The kwargs are static attributes"""
        if objtype not in self.ooi_objects:
            self.ooi_objects[objtype] = {}
        ot_objects = self.ooi_objects[objtype]
        if objtype not in self.ooi_obj_attrs:
            self.ooi_obj_attrs[objtype] = set()
        ot_obj_attrs = self.ooi_obj_attrs[objtype]

        if objid not in ot_objects:
            ot_objects[objid] = dict(id=objid)
        obj_entry = ot_objects[objid]
        if key:
            key = key if mapping is None else mapping.get(key, key)
            if value_is_list:
                if key in obj_entry:
                    if value in obj_entry[key]:
                        if not list_dup_ok:
                            msg = "duplicate_attr_list_value: %s.%s has attribute '%s' with duplicate list value: %s" % (objtype, objid, key, value)
                            self.warnings.append((objid, msg))
                    else:
                        obj_entry[key].append(value)
                        obj_entry[key].sort()
                else:
                    obj_entry[key] = [value]
            elif key in obj_entry and not change_ok:
                msg = "duplicate_attr: %s.%s has duplicate attribute '%s' def: (old=%s, new=%s)" % (objtype, objid, key, obj_entry[key], value)
                self.warnings.append((objid, msg))
            else:
                obj_entry[key] = value
            ot_obj_attrs.add(key)
        for okey, oval in kwargs.iteritems():
            okey = okey if mapping is None else mapping.get(okey, okey)
            if okey in obj_entry and obj_entry[okey] != oval and not change_ok:
                msg = "different_static_attr: %s.%s has different attribute '%s' value: (old=%s, new=%s)" % (objtype, objid, okey, obj_entry[okey], oval)
                self.warnings.append((objid, msg))
            else:
                obj_entry[okey] = oval
            ot_obj_attrs.add(okey)

    # ---- Parse SAF export CSV files ----
    # Note: The following _parse_AttributeReport* function parse decomposed CSV files. Every attribute is in
    # its own row. There are, however, "static" attributes that are repeated with each attribute row.

    def _parse_AttributeReportArrays(self, row):
        ooi_rd = OOIReferenceDesignator(row['Array'])
        if ooi_rd.error or not ooi_rd.rd_type == "asset" or not ooi_rd.rd_subtype == "array":
            msg = "invalid_rd: %s is not an array reference designator" % (ooi_rd.rd)
            self.warnings.append((ooi_rd.rd, msg))
            return
        self._add_object_attribute('array',
            ooi_rd.rd, row['Attribute'], row['AttributeValue'],
            mapping={'Array_Name':'name'},
            Array_Name=row['Array_Name'])

    def _parse_AttributeReportClass(self, row):
        ooi_rd = OOIReferenceDesignator(row['Class'])
        if ooi_rd.error or not ooi_rd.rd_type == "inst_class":
            msg = "invalid_rd: %s is not an instrument class reference designator" % (ooi_rd.rd)
            self.warnings.append((ooi_rd.rd, msg))
            return
        self._add_object_attribute('class',
            ooi_rd.rd, row['Attribute'], row['AttributeValue'],
            mapping={'Description':'description'},
            name=row['Class_Name'])

    def _parse_AttributeReportDataProducts(self, row):
        key = row['Data_Product_Identifier'].strip() + "_L" + row['Data_Product_Level'].strip()
        ooi_rd = OOIReferenceDesignator(key)
        if ooi_rd.error or not ooi_rd.rd_type == "dataproduct" or not ooi_rd.rd_subtype == "level":
            msg = "invalid_rd: %s is not a data product reference designator" % (ooi_rd.rd)
            self.warnings.append((ooi_rd.rd, msg))
            return
        self._add_object_attribute('data_product_type',
            row['Data_Product_Identifier'], row['Attribute'], row['AttributeValue'],
            mapping={'Regime(s)':'regime'},
            Data_Product_Name=row['Data_Product_Name'], Data_Product_Level=row['Data_Product_Level'])

    def _parse_AttributeReportFamilies(self, row):
        self._add_object_attribute('family',
            row['Family'], row['Attribute'], row['AttributeValue'],
            mapping={},
            name=row['Family_Name'])

    def _parse_AttributeReportMakeModel(self, row):
        self._add_object_attribute('makemodel',
            row['Make_Model'], row['Attribute'], row['Attribute_Value'],
            mapping={},
            name=row['Make_Model'],
            Manufacturer=row['Manufacturer'], Make_Model_Description=row['Make_Model_Description'])

    def _parse_AttributeReportNodes(self, row):
        ooi_rd = OOIReferenceDesignator(row['Node'])
        if ooi_rd.error or not ooi_rd.rd_type == "asset" or not ooi_rd.rd_subtype == "node":
            msg = "invalid_rd: %s is not a node designator" % (ooi_rd.rd)
            self.warnings.append((ooi_rd.rd, msg))
            return
        # TODO: Create a default name by structure (subsite name + node type name)
        nodetypes = self.get_type_assets('nodetype')
        if row['Attribute'] == "longitude" and row['AttributeValue']:
            row['AttributeValue'] = str(-1 * float(row['AttributeValue']))
        self._add_object_attribute('node',
            ooi_rd.rd, row['Attribute'], row['AttributeValue'],
            mapping={},
            Node_Type=row['Node_Type'], Node_Site_Sequence=row['Node_Site_Sequence'])

    def _parse_NodeTypes(self, row):
        self._add_object_attribute('nodetype',
                                   row['LNodeType'], None, None,
                                   mapping={'Name':'name'},
                                   Name=row['Name'])

    def _parse_AttributeReportPorts(self, row):
        ooi_rd = OOIReferenceDesignator(row['Port'])
        if ooi_rd.error or not ooi_rd.rd_type == "asset" or not ooi_rd.rd_subtype == "port":
            msg = "invalid_rd: %s is not a port designator" % (ooi_rd.rd)
            self.warnings.append((ooi_rd.rd, msg))
            return
        self._add_object_attribute('port',
            ooi_rd.rd, row['Attribute'], row['AttributeValue'],
            mapping={})

    def _parse_AttributeReportReferenceDesignator(self, row):
        ooi_rd = OOIReferenceDesignator(row['Reference_Designator'])
        if ooi_rd.error or not ooi_rd.rd_type == "asset" or not ooi_rd.rd_subtype == "instrument":
            msg = "invalid_rd: %s is not an instrument designator" % (ooi_rd.rd)
            self.warnings.append((ooi_rd.rd, msg))
            return
        if row['Attribute'] == "longitude" and row['AttributeValue']:
            row['AttributeValue'] = str(-1 * float(row['AttributeValue']))
        self._add_object_attribute('instrument',
            ooi_rd.rd, row['Attribute'], row['AttributeValue'],
            mapping={},
            Class=row['Class'])

    def _parse_AttributeReportSeries(self, row):
        key = row['Class'] + row['Series']
        self._add_object_attribute('series',
            key, row['Attribute'], row['AttributeValue'],
            mapping={'Description':'description'},
            Series=row['Series'], name=row['Series_Name'], Class=row['Class'])

    def _parse_AttributeReportSites(self, row):
        ooi_rd = OOIReferenceDesignator(row['Site'])
        if ooi_rd.error or not ooi_rd.rd_type == "asset" or not ooi_rd.rd_subtype == "site":
            msg = "invalid_rd: %s is not a site designator" % (ooi_rd.rd)
            self.warnings.append((ooi_rd.rd, msg))
            return
        self._add_object_attribute('site',
            ooi_rd.rd, row['Attribute'], row['AttributeValue'],
            name=row['Site_Name'])

    def _parse_AttributeReportSubseries(self, row):
        key = row['Class'] + row['Series'] + row['Subseries']
        self._add_object_attribute('subseries',
            key, row['Attribute'], row['AttributeValue'],
            mapping={'Description':'description'},
            Subseries=row['Subseries'], name=row['Subseries_Name'], Class=row['Class'])

    def _parse_AttributeReportSubsites(self, row):
        ooi_rd = OOIReferenceDesignator(row['Subsite'])
        if ooi_rd.error or not ooi_rd.rd_type == "asset" or not ooi_rd.rd_subtype == "subsite":
            msg = "invalid_rd: %s is not a subsite designator" % (ooi_rd.rd)
            self.warnings.append((ooi_rd.rd, msg))
            return
        if row['Attribute'] == "longitude" and row['AttributeValue']:
            row['AttributeValue'] = str(-1 * float(row['AttributeValue']))
        self._add_object_attribute('subsite',
            ooi_rd.rd, row['Attribute'], row['AttributeValue'],
            name=row['Subsite_Name'])

    def _parse_InstrumentCatalogFull(self, row):
        # This adds the subseries to current sensors and make/model.
        # Also used to infer node types and names
        refid = row['ReferenceDesignator']
        series_id = row['SClass_PublicID']+row['SSeries_PublicID']
        subseries_id = series_id+row['SSubseries_PublicID']
        makemodel = row['MMInstrument_PublicID']
        entry = dict(
            instrument_class=row['SClass_PublicID'],
            instrument_series=row['SSeries_PublicID'],
            instrument_subseries=row['SSubseries_PublicID'],
            instrument_model1=row['SClass_PublicID'],
            instrument_model=series_id,
            makemodel=makemodel,
            ready_for_2013=row['Ready_For_2013_']
        )
        self._add_object_attribute('instrument',
            refid, None, None, **entry)

        if makemodel:
            self._add_object_attribute('class',
                                       row['SClass_PublicID'], 'makemodel', makemodel, value_is_list=True, list_dup_ok=True)
            self._add_object_attribute('series',
                                       series_id, None, None, makemodel=makemodel)
            self._add_object_attribute('subseries',
                subseries_id, None, None, makemodel=makemodel)

        # Build up the node type here
        ntype_txt = row['Textbox11']
        ntype_id = ntype_txt[:2]
        #ntype_name = ntype_txt[3:-1].strip('()')
        #self._add_object_attribute('nodetype',
        #    ntype_id, None, None, name=ntype_name)

        # Determine on which arrays the nodetype is used
        self._add_object_attribute('nodetype',
            ntype_id, 'array_list', refid[:2], value_is_list=True, list_dup_ok=True)

        # Determine on which arrays the instrument class is used
        self._add_object_attribute('class',
            row['SClass_PublicID'], 'array_list', refid[:2], value_is_list=True, list_dup_ok=True)

        self._add_object_attribute('series',
                                   series_id, 'array_list', refid[:2], value_is_list=True, list_dup_ok=True)

    def _parse_DataQCLookupTables(self, row):
        # Adds a list of data products with level to instruments
        refid = row['ReferenceDesignator']
        self._add_object_attribute('instrument',
            refid, None, None, Class=row['SClass_PublicID'])

        dpl = row['Data_Product_With_Level']
        m = re.match('^([A-Z0-9_]{7})\s+\((L\d)\)$', dpl)
        if not m:
            msg = "invalid_rd: %s is not a data product designator" % (dpl)
            self.warnings.append((refid, msg))
            return
        dp_type, dp_level = m.groups()
        dpl = dp_type + "_" + dp_level

        self._add_object_attribute('instrument',
            refid, 'data_product_list', dpl, value_is_list=True)

    def _parse_DataProductSpreadsheet(self, row):
        dp_types = self.ooi_objects['data_product_type']
        dp_type = row['Data_Product_Identifier'].strip()
        dpt_obj = dp_types.get(dp_type, {})
        key = dp_type + "_" + row['Data_Product_Level1'].strip()
        entry = dpt_obj.copy()
        entry.pop("id", None)
        entry.update(dict(
            name=row['Data_Product_Name'].strip(),
            code=dp_type,
            level=row['Data_Product_Level1'].strip(),
            units=row['Units'].strip(),
            dps=row['DPS_DCN_s_'].strip(),
            diagrams=row['Processing_Flow_Diagram_DCN_s_'].strip(),
        ))
        self._add_object_attribute('data_product',
            key, None, None, **entry)
        self._add_object_attribute('data_product',
            key, 'instrument_class_list', row['Instrument_Class'].strip(), value_is_list=True)

    def _parse_AllSensorTypeCounts(self, row):
        # Adds family to instrument class
        self._add_object_attribute('class',
            row['Class'].strip(), 'family', row['Family'].strip())

    # ---- Parse mapping spreadsheet tab ----

    def _parse_Arrays(self, row):
        ooi_rd = row['Reference ID']
        name=row['Name']
        self._add_object_attribute('array',
            ooi_rd, 'name', name, change_ok=True)

    def _parse_Sites(self, row):
        ooi_rd = row['Reference ID']
        name = row['Full Name']
        local_name = row['Name Extension']

        self._add_object_attribute('site',
            ooi_rd, 'name', name, change_ok=True)

        # Aggregated site level entries
        self._add_object_attribute('site',
                                   ooi_rd, 'osite', name)

        self._add_object_attribute('osite',
                                   name, None, None, name=name, local_name=local_name)
        self._add_object_attribute('osite',
                                   name, 'site_rd_list', ooi_rd, value_is_list=True)

    def _parse_Subsites(self, row):
        ooi_rd = row['Reference ID']
        name = row['Full Name']
        local_name = row['Local Name']
        geo_area = row['Site Name']

        coord_dict = dict(
            lat_north = float(row['lat_north']) if row['lat_north'] else None,
            lat_south = float(row['lat_south']) if row['lat_south'] else None,
            lon_east = float(row['lon_east']) if row['lon_east'] else None,
            lon_west = float(row['lon_west']) if row['lon_west'] else None,
            depth_min = float(row['depth_min']) if row['depth_min'] else None,
            depth_max = float(row['depth_max']) if row['depth_max'] else None,
        )
        # Aggregated subsite level entries
        self._add_object_attribute('subsite',
            ooi_rd, 'ssite', name)

        self._add_object_attribute('ssite',
                                   name, None, None, name=name, local_name=local_name, geo_area=geo_area)
        self._add_object_attribute('ssite',
                                   name, 'subsite_rd_list', ooi_rd, value_is_list=True)
        if row['lat_north']:
            self._add_object_attribute('ssite',
                                   name, None, None, **coord_dict)

    def _parse_Nodes(self, row):
        ooi_rd = row['Reference ID']
        name=row['Full Name']
        local_name = row['Name Extension']
        node_entry = dict(
            local_name=local_name,
            parent_id=row['Parent Reference ID'],
            platform_id=row['Platform Reference ID'],
            platform_config_type=row['Platform Configuration Type'],
            platform_agent_type=row['Platform Agent Type'],
            is_platform=row['Platform Reference ID'] == ooi_rd,
            self_port=row['Self Port'],
            uplink_node=row['Uplink Node'],
            uplink_port=row['Uplink Port'],
            deployment_start=row['Start Deployment Cruise'],
        )
        self._add_object_attribute('node',
            ooi_rd, None, None, **node_entry)
        self._add_object_attribute('node',
            ooi_rd, 'name', name, change_ok=True)

        # Determine on which arrays the nodetype is used
        self._add_object_attribute('nodetype',
            ooi_rd[9:11], 'array_list', ooi_rd[:2], value_is_list=True, list_dup_ok=True)

    def _parse_NTypes(self, row):
        code = row['Code']
        name = row['Name']

        # Only add new stuff from spreadsheet
        if code not in self.ooi_objects['nodetype']:
            self._add_object_attribute('nodetype',
                code, None, None, name=name)

    def _parse_PlatformAgents(self, row):
        code = row['Code']
        entry = dict(
            name=row['Name'],
            agent_type=row['Agent Type'],
            node_types=row['Node Types'],
            rt_control_path=row['RT Control Path'],
            rt_data_path=row['RT Data Path'],
            rt_data_acquisition=row['RT Data Acquisition'],
            full_data_acquisition=row['Full Data Acquisition'],
            ci_interface_location=row['Marine-CI Interface Location'],
        )
        self._add_object_attribute('platformagent',
            code, None, None, **entry)

    def _parse_Series(self, row):
        code = row['Class Code']
        series = row['Series']
        series_rd = code + series
        agent_name = row['Agent Code']

        entry = dict(
            agent_name=agent_name,
            connection=row['Connection'],
            driver=row['Driver'] == "Yes",
            tier1=row['Tier 1'] == "Yes"
            )
        self._add_object_attribute('series',
                                   series_rd, None, None, **entry)
        if agent_name and agent_name != "NA":
            self._add_object_attribute('instagent',
                                       agent_name, None, None,
                                       inst_class=code,
                                       tier1=row['Tier 1'] == "Yes")
            self._add_object_attribute('instagent',
                                       agent_name, 'series_list', series_rd, value_is_list=True, list_dup_ok=True)

    def _parse_InstAgents(self, row):
        agent_code = row['Agent Code']
        self._add_object_attribute('instagent',
                                   agent_code, None, None, active=row['Active'] == "Yes")

    def _parse_InstAvail(self, row):
        node_code = row['Node Code']
        inst_class = row['Instrument Class']
        inst_series = row['Instrument Series']
        inst_deploy = row['First Deployment Date']

        code = node_code + "-" + inst_class + inst_series
        deploy_value = (inst_class + inst_series, inst_deploy)

        if deploy_value and re.match(r'\d+-\d+-\d+', inst_deploy):
            self._add_object_attribute('node',
                                   node_code, 'model_deploy_list', deploy_value, value_is_list=True, list_dup_ok=True)


    # ---- Post-processing and validation ----

    def _perform_ooi_checks(self):
        # Perform some consistency checking on imported objects
        ui_checks = [
            ('ref_exists', ['instrument', 'data_product_list', 'data_product'], None),
            ('ref_exists', ['data_product', 'instrument_class_list', 'class'], None),
            ]
        for check, ckargs, ckkwargs in ui_checks:
            ckargs = [] if ckargs is None else ckargs
            ckkwargs = {} if ckkwargs is None else ckkwargs
            checkfunc = getattr(self, "_checkooi_%s" % check)
            checkfunc(*ckargs, **ckkwargs)

    def _checkooi_ref_exists(self, objtype, attr, target_type, **kwargs):
        if objtype not in self.ooi_objects:
            msg = "ref_exists: %s not a valid object type" % (objtype)
            self.warnings.append(("GENERAL", msg))
            return
        ot_objects = self.ooi_objects[objtype]
        if target_type not in self.ooi_objects:
            msg = "ref_exists: %s not a valid target object type" % (target_type)
            self.warnings.append(("GENERAL", msg))
            return
        ottarg_objects = self.ooi_objects[target_type]

        refattrset = set()
        total_ref = 0

        for obj_key,obj in ot_objects.iteritems():
            ref_attr = obj.get(attr, None)
            if ref_attr is None:
                #msg = "ref_exists: %s.%s attribute is None" % (objtype, attr)
                #self.warnings.append((obj_key, msg))
                continue
            elif type(ref_attr) is list:
                for rattval in ref_attr:
                    refattrset.add(rattval)
                    total_ref += 1
                    if rattval not in ottarg_objects:
                        msg = "ref_exists: %s.%s (list) contains a non-existing object reference (value=%s)" % (objtype, attr, rattval)
                        self.warnings.append((obj_key, msg))
            else:
                refattrset.add(ref_attr)
                total_ref += 1
                if ref_attr not in ottarg_objects:
                    msg = "ref_exists: %s.%s not an existing object reference (value=%s)" % (objtype, attr, ref_attr)
                    self.warnings.append((obj_key, msg))

        log.debug("_checkooi_ref_exists: Checked %s objects type %s against type %s" % (len(ot_objects), objtype, target_type))
        log.debug("_checkooi_ref_exists: Different references=%s (of total=%s) vs target objects=%s" % (len(refattrset), total_ref, len(ottarg_objects)))

    def _post_process(self):
        nodes = self.get_type_assets('node')
        nodetypes = self.get_type_assets('nodetype')
        subsites = self.get_type_assets('subsite')
        osites = self.get_type_assets('osite')
        sites = self.get_type_assets('site')
        ssites = self.get_type_assets('ssite')

        # Make sure all node types have a name
        for code, obj in nodetypes.iteritems():
            if not obj.get('name', None):
                obj['name'] = "(" + code + ")"

        # Add rd and parents to ssites
        for key, ssite in ssites.iteritems():
            subsite_rd_list = ssite['subsite_rd_list']
            if not 'lat_north' in ssite or not ssite['lat_north']:
                subsite_objs = [subsites[subsite_id] for subsite_id in subsite_rd_list]
                bbox = GeoUtils.calc_bounding_box_for_points(subsite_objs, key_mapping=dict(depth="depth_subsite"))
                ssite.update(bbox)
            ssite['rd'] = subsite_rd_list[0]
            ooi_rd = OOIReferenceDesignator(subsite_rd_list[0])
            site = sites[ooi_rd.site_rd]
            osite = osites[site['osite']]
            if 'ssite_list' not in osite:
                osite['ssite_list'] = []
            osite['ssite_list'].append(key)
            ssite['parent_id'] = osite['site_rd_list'][0]

        # Add rd to osites
        for key, osite in osites.iteritems():
            site_rd_list = osite['site_rd_list']
            ssite_list = osite.get('ssite_list', [])

            ssite_objs = [ssites[ss_id] for ss_id in ssite_list]
            bbox = GeoUtils.calc_bounding_box_for_boxes(ssite_objs)

            osite.update(bbox)
            osite['rd'] = site_rd_list[0]

        # Make sure all nodes have a name and geospatial coordinates
        for ooi_rd, obj in nodes.iteritems():
            if not obj.get('name', None):
                name = subsites[ooi_rd[:8]]['name'] + " - " + nodetypes[ooi_rd[9:11]]['name']
                obj['name'] = name
            if not obj.get('latitude', None):
                pass


    def get_marine_io(self, ooi_rd_str):
        ooi_rd = OOIReferenceDesignator(ooi_rd_str)
        if ooi_rd.error:
            return None
        else:
            return ooi_rd.marine_io

    def get_org_ids(self, ooi_rd_list):
        if not ooi_rd_list:
            return ""
        marine_ios = set()
        for ooi_rd in ooi_rd_list:
            marine_io = self.get_marine_io(ooi_rd)
            if marine_io == "CG":
                marine_ios.add("MF_CGSN")
            elif marine_io == "RSN":
                marine_ios.add("MF_RSN")
            elif marine_io == "EA":
                marine_ios.add("MF_EA")
        return ",".join(marine_ios)

    def delete_ooi_assets(self):
        res_ids = []

        ooi_asset_types = ['InstrumentModel',
                           'PlatformModel',
                           'Observatory',
                           'Subsite',
                           'PlatformSite',
                           'InstrumentSite',
                           'InstrumentAgent',
                           'InstrumentAgentInstance',
                           'InstrumentDevice',
                           'PlatformAgent',
                           'PlatformAgentInstance',
                           'PlatformDevice',
                           'Deployment',
                           'DataProduct'
        ]

        self.resource_ds = DatastoreManager.get_datastore_instance(DataStore.DS_RESOURCES, DataStore.DS_PROFILE.RESOURCES)

        del_objs = {}
        del_assocs = {}
        all_objs = self.resource_ds.find_by_view("_all_docs", None, id_only=False, convert_doc=False)
        for obj_id, key, obj in all_objs:
            if obj_id.startswith("_design") or not isinstance(obj, dict):
                continue
            obj_type = obj.get("type_", None)
            if obj_type and obj_type in ooi_asset_types:
                del_objs[obj_id] = obj
        for obj_id, key, obj in all_objs:
            if obj_id.startswith("_design") or not isinstance(obj, dict):
                continue
            obj_type = obj.get("type_", None)
            if obj_type == "Association":
                if obj['o'] in del_objs or obj['s'] in del_objs:
                    del_assocs[obj_id] = obj
        for doc in del_objs.values():
            doc_id, doc_rev = doc['_id'], doc['_rev']
            doc.clear()
            doc.update(dict(_id=doc_id, _rev=doc_rev, _deleted=True))
        for doc in del_assocs.values():
            doc_id, doc_rev = doc['_id'], doc['_rev']
            doc.clear()
            doc.update(dict(_id=doc_id, _rev=doc_rev, _deleted=True))

        self.resource_ds.update_doc_mult(del_objs.values())
        self.resource_ds.update_doc_mult(del_assocs.values())

        log.info("Deleted %s OOI resources and %s associations", len(del_objs), len(del_assocs))

    def analyze_ooi_assets(self, end_date):
        """
        Iterates through OOI assets and determines relevant ones by a cutoff data.
        Prepares a report and export for easier development.
        """
        report_lines = []
        node_objs = self.get_type_assets("node")
        nodetype_objs = self.get_type_assets("nodetype")
        inst_objs = self.get_type_assets("instrument")
        series_objs = self.get_type_assets("series")

        deploy_platforms = {}
        platform_children = {}

        # Pass: Propagate first deployment date to
        for ooi_id, ooi_obj in node_objs.iteritems():
            platform_node = node_objs[ooi_obj['platform_id']]
            if ooi_obj.get('First Deployment Date', None):
                platform_node['First Deployment Date'] = min(platform_node.get('First Deployment Date', '2020-01'), ooi_obj['First Deployment Date'])

        # Pass: Determine node first deployment dates
        for ooi_id, ooi_obj in node_objs.iteritems():
            if ooi_obj.get('parent_id', None):
                parent_id = ooi_obj.get('parent_id')
                if parent_id not in platform_children:
                    platform_children[parent_id] = []
                platform_children[parent_id].append(ooi_id)
            deploy_date_col = ooi_obj['deployment_start']
            if not deploy_date_col:
                try:
                    deploy_date = datetime.datetime.strptime(ooi_obj['First Deployment Date'], "%Y-%m")
                    ooi_obj['deploy_date'] = deploy_date
                    deploy_date_col = deploy_date.strftime('%Y-%m-%d')
                    ooi_obj['deployment_start'] = deploy_date_col
                except Exception as ex:
                    deploy_date_col = "2020-01-01"
                    ooi_obj['deployment_start'] = deploy_date_col
            try:
                deploy_date = datetime.datetime.strptime(deploy_date_col, "%Y-%m-%d")
                ooi_obj['deploy_date'] = deploy_date
                if not end_date or deploy_date <= end_date:
                    if ooi_obj.get('is_platform', False):
                        deploy_platforms[ooi_id] = ooi_obj
            except Exception as ex:
                ooi_obj['deploy_date'] = None
                print "Date parse error", ex

            nodetype_obj = nodetype_objs[ooi_id[9:11]]
            nodetype_obj["deploy_date"] = min(ooi_obj['deploy_date'], nodetype_obj.get("deploy_date", None) or datetime.datetime(2020, 1, 1))

        # TODO: Adjust incorrect child node or instrument deployment dates to minimum of parent platform date

        deploy_platform_list = deploy_platforms.values()
        deploy_platform_list.sort(key=lambda obj: [obj['deploy_date'], obj['name']])

        # Pass: Find instruments by node, set first deployment date
        inst_by_node = {}
        isite_by_node = {}
        pagent_objs = self.get_type_assets("platformagent")
        for inst_id, inst_obj in inst_objs.iteritems():
            ooi_rd = OOIReferenceDesignator(inst_id)
            node_id = ooi_rd.node_rd
            if node_id not in inst_by_node:
                inst_by_node[node_id] = []
            inst_by_node[node_id].append(ooi_rd.series_rd)
            if node_id not in isite_by_node:
                isite_by_node[node_id] = []
            isite_by_node[node_id].append(inst_id)

            # Find possible override instrument deploy date from InstAvail tab
            node_obj = node_objs[node_id]
            node_model_dates = node_obj.get("model_deploy_list", None)
            inst_deploy = None
            if node_model_dates:
                for iseries, idate in node_model_dates:
                    if iseries ==ooi_rd.series_rd:
                        inst_deploy = idate
                        break
            #if not inst_deploy:
            #    inst_deploy = inst_obj.get("First Deployment Date", None)
            node_deploy = node_objs[ooi_rd.node_rd].get("deploy_date", None)
            deploy_date = None
            if inst_deploy:
                try:
                    deploy_date = datetime.datetime.strptime(inst_deploy, "%Y-%m-%d")
                except Exception as ex:
                    try:
                        deploy_date = datetime.datetime.strptime(inst_deploy, "%Y-%m")
                    except Exception as ex:
                        try:
                            deploy_date = datetime.datetime.strptime(inst_deploy, "%Y")
                        except Exception as ex:
                            pass
                if deploy_date and node_deploy:
                    deploy_date = max(deploy_date, node_deploy)
            #elif node_deploy:
            #    deploy_date = node_deploy
            inst_obj['deploy_date'] = deploy_date or datetime.datetime(2020, 1, 1)

            series_obj = series_objs[ooi_rd.series_rd]
            series_obj["deploy_date"] = min(inst_obj['deploy_date'], series_obj.get("deploy_date", None) or datetime.datetime(2020, 1, 1))

        # Set data recovery mode etc in nodes and instruments
        for ooi_id, ooi_obj in node_objs.iteritems():
            pagent_type = ooi_obj.get('platform_agent_type', "")
            pagent_obj = pagent_objs.get(pagent_type, None)
            if pagent_obj:
                instrument_agent_rt = pagent_obj['rt_data_path'] == "Direct"
                data_agent_rt = pagent_obj['rt_data_path'] == "File Transfer"
                data_agent_recovery = pagent_obj['rt_data_acquisition'] == "Partial"
                ooi_obj['instrument_agent_rt'] = instrument_agent_rt
                ooi_obj['data_agent_rt'] = data_agent_rt
                ooi_obj['data_agent_recovery'] = data_agent_recovery

                for inst in isite_by_node.get(ooi_id, []):
                    inst_obj = inst_objs[inst]
                    inst_obj['instrument_agent_rt'] = instrument_agent_rt
                    inst_obj['data_agent_rt'] = data_agent_rt
                    inst_obj['data_agent_recovery'] = data_agent_recovery

        # Compose the report
        report_lines.append((0, "OOI ASSET REPORT - DEPLOYMENT UNTIL %s" % end_date.strftime('%Y-%m-%d') if end_date else "PROGRAM END"))
        report_lines.append((0, "Platforms by deployment date:"))
        inst_class_all = set()
        for ooi_obj in deploy_platform_list:
            inst_class_top = set()
            platform_deploy_date = ooi_obj['deploy_date']

            def follow_node_inst(node_id, level):
                inst_lines = []
                for inst_id in isite_by_node.get(node_id, []):
                    inst_obj = inst_objs[inst_id]
                    deploy_date = inst_obj.get('deploy_date', datetime.datetime(2020, 1, 1))
                    time_delta = platform_deploy_date - deploy_date
                    if abs(time_delta.days) < 40:
                        inst_lines.append((2, "%s               +-%s %s %s" % (
                            "  " * level, inst_id, "IA" if inst_obj['instrument_agent_rt'] else "", "DA" if inst_obj['data_agent_rt'] else "")))
                    else:
                        inst_lines.append((2, "%s               +-%s IGNORED" % ("  " * level, inst_id)))
                return sorted(inst_lines)

            def follow_child_nodes(level, child_nodes=None):
                if not child_nodes:
                    return
                for ch_id in child_nodes:
                    ch_obj = node_objs[ch_id]
                    inst_class_node = set((inst, ch_obj.get('platform_agent_type', "")) for inst in inst_by_node.get(ch_id, []))
                    inst_class_top.update(inst_class_node)
                    report_lines.append((1, "%s             +-%s %s %s: %s" % ("  "*level, ch_obj['id'], ch_obj['name'], ch_obj.get('platform_agent_type', ""), ", ".join([i for i,p in sorted(list(inst_class_node))]))))
                    inst_lines = follow_node_inst(ch_id, level)
                    report_lines.extend(inst_lines)
                    follow_child_nodes(level+1, platform_children.get(ch_id,None))

            inst_class_node = set((inst, ooi_obj.get('platform_agent_type', "")) for inst in inst_by_node.get(ooi_obj['id'], []))
            inst_class_top.update(inst_class_node)
            report_lines.append((0, "  %s %s %s %s: %s" % (ooi_obj['deployment_start'], ooi_obj['id'], ooi_obj['name'], ooi_obj.get('platform_agent_type', ""), ", ".join([i for i,p in sorted(list(inst_class_node))]))))

            inst_lines = follow_node_inst(ooi_obj['id'], 0)
            report_lines.extend(inst_lines)

            follow_child_nodes(0, platform_children.get(ooi_obj['id'], None))
            inst_class_all.update(inst_class_top)

        report_lines.append((0, "Instrument Models:"))
        inst_by_conn = {}
        for clss, conn in inst_class_all:
            if conn not in inst_by_conn:
                inst_by_conn[conn] = []
            inst_by_conn[conn].append(clss)
        for conn, inst in inst_by_conn.iteritems():
            report_lines.append((0, "  %s: %s" % (conn, ",".join(sorted(inst)))))

        self.asset_report = report_lines

    def report_ooi_assets(self, report_level=5, dump_assets=True, print_report=True):
        if print_report:
            print "\n".join(line for level, line in self.asset_report if level < report_level)

        if dump_assets:
            from ion.util.datastore.resources import ResourceRegistryHelper
            rrh = ResourceRegistryHelper()
            rrh.dump_dicts_as_xlsx(self.ooi_objects)
