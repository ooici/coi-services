#!/usr/bin/env python

"""Parses OOI SAF Instrument Application assets from CSV reports."""

__author__ = 'Michael Meisinger'

import csv
import os.path
import re

from pyon.public import log, iex
from ion.core.ooiref import OOIReferenceDesignator
from ion.util.xlsparser import XLSParser

class OOILoader(object):
    def __init__(self, process, container=None, asset_path=None):
        self.process = process
        self.container = container or self.process.container
        self.asset_path = asset_path

    def extract_ooi_assets(self):
        """
        Parses SAF Instrument Application export CSV files into intermediate memory structures.
        This information can later be loaded in to actual load_ion() function.
        """
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
                       'InstrumentCatalogFull',
                       'DataQCLookupTables',
                       'DataProductSpreadsheet',
                       'AllSensorTypeCounts',
                       # Tabs from the mapping spreadsheet
                       'Arrays',
                       'Sites',
                       'Subsites',
                       'Nodes',
                       #'Platforms',
                       'NodeTypes',
                       'PlatformAgentTypes'
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
            ot_objects[objid] = {}
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
            mapping={},
            Class_Name=row['Class_Name'])

    def _parse_AttributeReportDataProducts(self, row):
        key = row['Data_Product_Identifier'] + "_L" + row['Data_Product_Level']
        ooi_rd = OOIReferenceDesignator(key)
        if ooi_rd.error or not ooi_rd.rd_type == "dataproduct" or not ooi_rd.rd_subtype == "level":
            msg = "invalid_rd: %s is not a data product reference designator" % (ooi_rd.rd)
            self.warnings.append((ooi_rd.rd, msg))
            return
        self._add_object_attribute('data_product_type',
            row['Data_Product_Identifier'], row['Attribute'], row['AttributeValue'],
            mapping={},
            Data_Product_Name=row['Data_Product_Name'], Data_Product_Level=row['Data_Product_Level'])

    def _parse_AttributeReportFamilies(self, row):
        self._add_object_attribute('family',
            row['Family'], row['Attribute'], row['AttributeValue'],
            mapping={},
            Family_Name=row['Family_Name'])

    def _parse_AttributeReportMakeModel(self, row):
        self._add_object_attribute('makemodel',
            row['Make_Model'], row['Attribute'], row['Attribute_Value'],
            mapping={},
            Manufacturer=row['Manufacturer'], Make_Model_Description=row['Make_Model_Description'])

    def _parse_AttributeReportNodes(self, row):
        ooi_rd = OOIReferenceDesignator(row['Node'])
        if ooi_rd.error or not ooi_rd.rd_type == "asset" or not ooi_rd.rd_subtype == "node":
            msg = "invalid_rd: %s is not a node designator" % (ooi_rd.rd)
            self.warnings.append((ooi_rd.rd, msg))
            return
        # TODO: Create a default name by structure (subsite name + node type name)
        nodetypes = self.get_type_assets('nodetype')
        self._add_object_attribute('node',
            ooi_rd.rd, row['Attribute'], row['AttributeValue'],
            mapping={},
            Node_Type=row['Node_Type'], Node_Site_Sequence=row['Node_Site_Sequence'])

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
        self._add_object_attribute('instrument',
            ooi_rd.rd, row['Attribute'], row['AttributeValue'],
            mapping={},
            Class=row['Class'])

    def _parse_AttributeReportSeries(self, row):
        key = row['Class'] + row['Series']
        self._add_object_attribute('series',
            key, row['Attribute'], row['AttributeValue'],
            mapping={},
            Series=row['Series'], Series_Name=row['Series_Name'], Class=row['Class'])

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
        self._add_object_attribute('subsite',
            ooi_rd.rd, row['Attribute'], row['AttributeValue'],
            name=row['Subsite_Name'])

    def _parse_InstrumentCatalogFull(self, row):
        # This adds the subseries to current sensors and make/model.
        # Also used to infer node types and names
        refid = row['ReferenceDesignator']
        subseries_id = row['SClass_PublicID']+row['SSeries_PublicID']+row['SSubseries_PublicID']
        entry = dict(
            instrument_class=row['SClass_PublicID'],
            instrument_series=row['SSeries_PublicID'],
            instrument_subseries=row['SSubseries_PublicID'],
            instrument_model=subseries_id,
            makemodel=row['MMInstrument_PublicID'],
            ready_for_2013=row['Textbox16']
        )
        self._add_object_attribute('instrument',
            refid, None, None, **entry)

        self._add_object_attribute('subseries',
            subseries_id, 'makemodel', row['MMInstrument_PublicID'], value_is_list=True, list_dup_ok=True)

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

    def _parse_DataQCLookupTables(self, row):
        # Adds a list of data products with level to instruments
        refid = row['ReferenceDesignator']
        self._add_object_attribute('instrument',
            refid, None, None, Class=row['SClass_PublicID'])

        dpl = row['Data_Product_With_Level']
        m = re.match('^([A-Z0-9_]{7}) \((L\d)\)$', dpl)
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
        dp_type = row['Data_Product_Identifier']
        dpt_obj = dp_types.get(dp_type, {})
        key = dp_type + "_" + row['Data_Product_Level1']
        entry = dpt_obj.copy()
        entry.update(dict(
            name=row['Data_Product_Name'],
            level=row['Data_Product_Level1'],
            units=row['Units'],
            dps=row['DPS_DCN_s_'],
            diagrams=row['Processing_Flow_Diagram_DCN_s_'],
        ))
        self._add_object_attribute('data_product',
            key, None, None, **entry)
        self._add_object_attribute('data_product',
            key, 'instrument_class_list', row['Instrument_Class'], value_is_list=True)

    def _parse_AllSensorTypeCounts(self, row):
        # Adds family to instrument class
        self._add_object_attribute('class',
            row['Class'], 'family', row['Family'])

    def _parse_Arrays(self, row):
        ooi_rd = row['Reference ID']
        name=row['Name']
        self._add_object_attribute('array',
            ooi_rd, 'name', name, change_ok=True)

    def _parse_Sites(self, row):
        ooi_rd = row['Reference ID']
        name = row['Full Name']
        self._add_object_attribute('site',
            ooi_rd, 'name', name, change_ok=True)

    def _parse_Subsites(self, row):
        ooi_rd = row['Reference ID']
        name = row['Full Name']
        self._add_object_attribute('subsite',
            ooi_rd, 'name', name, change_ok=True)

    def _parse_Nodes(self, row):
        ooi_rd = row['Reference ID']
        name=row['Full Name']
        node_entry = dict(
            parent_id=row['Parent Reference ID'],
            platform_id=row['Platform Reference ID'],
            platform_config_type=row['Platform Configuration Type'],
            platform_agent_type=row['Platform Agent Type'],
            is_platform=row['Platform Reference ID'] == ooi_rd,
        )
        self._add_object_attribute('node',
            ooi_rd, None, None, **node_entry)
        self._add_object_attribute('node',
            ooi_rd, 'name', name, change_ok=True)

        # Determine on which arrays the nodetype is used
        self._add_object_attribute('nodetype',
            ooi_rd[9:11], 'array_list', ooi_rd[:2], value_is_list=True, list_dup_ok=True)

    def _parse_NodeTypes(self, row):
        code = row['Code']
        name = row['Name']
        self._add_object_attribute('nodetype',
            code, None, None, name=name)

    def _parse_PlatformAgentTypes(self, row):
        #
        code = row['Code']
        entry = dict(
            name=row['Name'],
            rt_control_path=row['RT Control Path'],
            rt_data_path=row['RT Data Path'],
            rt_data_acquisition=row['RT Data Acquisition'],
            full_data_acquisition=row['Full Data Acquisition'],
            ci_interface_location=row['Marine-CI Interface Location'],
        )
        self._add_object_attribute('platformagent',
            code, None, None, **entry)

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

        # Make sure all node types have a name
        for code, obj in nodetypes.iteritems():
            if not obj.get('name', None):
                obj['name'] = "(" + code + ")"

        # Make sure all nodes have a name
        for ooi_rd, obj in nodes.iteritems():
            if not obj.get('name', None):
                name = subsites[ooi_rd[:8]]['name'] + " - " + nodetypes[ooi_rd[9:11]]['name']
                obj['name'] = name

    def get_marine_io(self, ooi_rd_str):
        ooi_rd = OOIReferenceDesignator(ooi_rd_str)
        if ooi_rd.error:
            return None
        else:
            return ooi_rd.marine_io
