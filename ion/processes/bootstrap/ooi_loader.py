#!/usr/bin/env python

"""Parses OOI SAF Instrument Application assets from CSV reports."""

__author__ = 'Michael Meisinger'

import csv
import re

from pyon.public import log, iex
from ion.core.ooiref import OOIReferenceDesignator


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

        categories = [ # Core concept attributes
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
                       'AllSensorTypeCounts']

        # Holds the object representations of parsed OOI assets by type
        self.ooi_objects = {}
        # Holds a list of attribute names of OOI assets by type
        self.ooi_obj_attrs = {}
        self.warnings = []

        for category in categories:
            row_do, row_skip = 0, 0

            catfunc = getattr(self, "_parse_%s" % category)
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
            except IOError as ioe:
                log.warn("OOI asset file %s error: %s" % (filename, str(ioe)))

            log.debug("Loaded assets %s: %d rows read" % (category, row_do))

        # Do some validation checking
        self._perform_ooi_checks()

        # Post processing
        if self.warnings:
            log.warn("WARNINGS:\n%s", "\n".join(["%s: %s" % (a, b) for a, b in self.warnings]))

        for ot, oo in self.ooi_objects.iteritems():
            log.warn("Type %s has %s entries", ot, len(oo))
            log.warn("Type %s has %s attributes", ot, self.ooi_obj_attrs[ot])
            #print ot
            #print "\n".join(sorted(list(self.ooi_obj_attrs[ot])))

    def get_type_assets(self, objtype):
        return self.ooi_objects.get(objtype, None)

    def _add_object_attribute(self, objtype, objid, key, value, value_is_list=False, mapping=None, **kwargs):
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
                        msg = "duplicate_attr_list_value: %s.%s has attribute '%s' with duplicate list value: %s" % (objtype, objid, key, value)
                        self.warnings.append((objid, msg))
                    obj_entry[key].append(value)
                else:
                    obj_entry[key] = [value]
            elif key in obj_entry:
                msg = "duplicate_attr: %s.%s has duplicate attribute '%s' def: (old=%s, new=%s)" % (objtype, objid, key, obj_entry[key], value)
                self.warnings.append((objid, msg))
            else:
                obj_entry[key] = value
            ot_obj_attrs.add(key)
        for okey, oval in kwargs.iteritems():
            okey = okey if mapping is None else mapping.get(okey, okey)
            if okey in obj_entry and obj_entry[okey] != oval:
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
        self._add_object_attribute('data_product',
            ooi_rd.rd, row['Attribute'], row['AttributeValue'],
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
            mapping={'Site_Name':'name'},
            Site_Name=row['Site_Name'])

    def _parse_AttributeReportSubseries(self, row):
        key = row['Class'] + row['Series'] + row['Subseries']
        self._add_object_attribute('subseries',
            key, row['Attribute'], row['AttributeValue'],
            mapping={'Subseries_Name':'name',
                     'Description':'description'},
            Subseries=row['Subseries'], Subseries_Name=row['Subseries_Name'], Class=row['Class'])

    def _parse_AttributeReportSubsites(self, row):
        ooi_rd = OOIReferenceDesignator(row['Subsite'])
        if ooi_rd.error or not ooi_rd.rd_type == "asset" or not ooi_rd.rd_subtype == "subsite":
            msg = "invalid_rd: %s is not a subsite designator" % (ooi_rd.rd)
            self.warnings.append((ooi_rd.rd, msg))
            return
        self._add_object_attribute('subsite',
            ooi_rd.rd, row['Attribute'], row['AttributeValue'],
            mapping={'Subsite_Name':'name'},
            Subsite_Name=row['Subsite_Name'])

    def _parse_InstrumentCatalogFull(self, row):
        # This adds the subseries to current sensors and make/model.
        # Also used to infer node types and names
        refid = row['ReferenceDesignator']
        entry = dict(
            instrument_class=row['SClass_PublicID'],
            instrument_series=row['SSeries_PublicID'],
            instrument_subseries=row['SSubseries_PublicID'],
            instrument_model=row['SClass_PublicID']+row['SSeries_PublicID']+row['SSubseries_PublicID'],
            makemodel=row['MMInstrument_PublicID'],
            ready_for_2013=row['Textbox16']
        )
        self._add_object_attribute('instrument',
            refid, None, None, **entry)

        # Build up the node type here
        ntype_txt = row['Textbox11']
        ntype_id = ntype_txt[:2]
        ntype_name = ntype_txt[3:-1].strip('()')
        self._add_object_attribute('nodetype',
            ntype_id, None, None, name=ntype_name)

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
        # Adds a list of instrument class to data product
        key = row['Data_Product_Identifier'] + "_" + row['Data_Product_Level1']
        entry = dict(
            data_product_name=row['Data_Product_Name'],
            units=row['Units'],
            dps=row['DPS_DCN_s_'],
            diagrams=row['Processing_Flow_Diagram_DCN_s_'],
        )
        self._add_object_attribute('data_product',
            key, None, None, **entry)
        self._add_object_attribute('data_product',
            key, 'instrument_class_list', row['Instrument_Class'], value_is_list=True)

    def _parse_AllSensorTypeCounts(self, row):
        # Adds family to instrument class
        self._add_object_attribute('class',
            row['Class'], 'family', row['Family'])

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


    # -------------------------------------------------------------------------------------------
    # OLD STUFF

    def extract_ooi_assets_old(self):
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

