#!/usr/bin/env python

"""Class that loads ION UI definitions from UI database exports into the resource registry"""

__author__ = 'Michael Meisinger'

import uuid
import json
import re
import os
import requests
import urllib

from pyon.datastore.datastore import DatastoreManager
from pyon.public import log, iex, IonObject
from pyon.util.containers import get_ion_ts

from interface import objects

class UILoader(object):

    DEFAULT_UISPEC_LOCATION = "https://userexperience.oceanobservatories.org/database-exports/"

    def __init__(self, process, container=None):
        self.process = process
        self.container = container or self.process.container
        self.files = {}
        self.read_from_local = True

    # ---------------------------------------------------------------------------

    def delete_ui(self):
        resource_types = [
            'UIGraphicType',
            'UIGraphic',
            'UIInformationLevel',
            'UIResourceType',
            'UIObjectType',
            'UIResourceAttribute',
            'UIObjectField',
            'UIScreenElement',
            'UIScreenLabel',
            'UIHelpTag',
            'UIMessageString',
            'UIState',
            'UIWidget',
            'UIScreenElementInformationElement',
            'UIEmbeddedScreenElement']

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

        if path.startswith("http"):
            self.get_ui_files(path)
            self.read_from_local = False
        else:
            path = path + "/ui_assets"

        log.info("Start parsing UI assets from path=%s" % path)

        categories = [
            ('Graphic Type.csv', 'GraphicType'),
            ('Graphic.csv', 'Graphic'),
            ('Information Level.csv', 'InformationLevel'),
            ('Resource Type.csv', 'ResourceType'),
            ('Object Type.csv', 'ObjectType'),
            ('Resource Attribute.csv', 'ResourceAttribute'),
            ('Object Field.csv', 'ObjectField'),
            ('Screen Element.csv', 'ScreenElement'),
            ('Screen Label.csv', 'ScreenLabel'),
            ('State.csv', 'State'),
            ('Widget.csv', 'Widget'),
            ('_jt_ScreenElement_InformationElement.csv', 'ScreenElementInformationElement'),
            ('_jt_ScreenElement_ScreenElement.csv', 'EmbeddedScreenElement'),
        ]

        self.uiid_prefix = uuid.uuid4().hex[:9] + "_"
        self.ui_objs = {}
        self.ui_obj_by_id = {}
        self.ref_assocs = []
        self.ui_assocs = []
        self.warnings = []

        for fname, category in categories:
            row_do, row_skip = 0, 0

            catfunc = getattr(self, "_loadui_%s" % category)
            try:
                if self.read_from_local:
                    # Read from file handle
                    filename = "%s/%s" % (path, fname)
                    log.info("Loading UI category %s from file %s" % (category, filename))
                    try:
                        with open(filename, "rb") as csvfile:
                            reader = self.process._get_csv_reader(csvfile)
                            for row in reader:
                                catfunc(row)
                                row_do += 1
                    except IOError, ioe:
                        log.warn("UI category file %s error: %s" % (filename, str(ioe)))
                else:
                    # Read from string
                    if fname in self.files:
                        log.info("Loading UI category %s from retrieved file %s" % (category, fname))
                        csvfile = self.files[fname]
                        # This is a hack to be able to read from string
                        csvfile = csvfile.split(os.linesep)
                        reader = self.process._get_csv_reader(csvfile)
                        for row in reader:
                            catfunc(row)
                            row_do += 1
                    else:
                        log.warn("UI category %s has no file %s" % (category, fname))

            except Exception as ex:
                log.warn("UI category %s error: %s" % (category, str(ex)))

            log.info("Loaded UI category %s: %d rows valid, %d rows skipped" % (category, row_do, row_skip))

        # Do some checking
        self._perform_ui_checks()

        try:
            ds = DatastoreManager.get_datastore_instance("resources")
            self._finalize_uirefs(ds)
            res = ds.create_mult(self.ui_obj_by_id.values(), allow_ids=True)
            log.info("Stored %s UI resource objects into resource registry" % (len(res)))
            res = ds.create_mult(self.ui_assocs)
            log.info("Stored %s UI resource associations into resource registry" % (len(res)))
        except Exception as ex:
            log.exception("load error err=%s" % (str(ex)))

    def get_ui_files(self, path):
        dirurl = path or self.DEFAULT_UISPEC_LOCATION
        log.info("Accessing UI specs URL: %s", dirurl)
        dirpage = requests.get(dirurl).text
        csvfiles = re.findall('href="(.+?\.csv)"', dirpage)
        log.debug("Found %s csvfiles: %s", len(csvfiles), csvfiles)
        for file in csvfiles:
            csvurl = self.DEFAULT_UISPEC_LOCATION + file
            content = requests.get(csvurl).content
            self.files[urllib.unquote(file)] = content
            log.info("Downloaded %s, size=%s", file, len(content))

    def _perform_ui_checks(self):
        # Perform some consistency checking on imported objects
        ui_checks = [
            ('illegal_char', None, {'allow_linebreak':['description']}),

            ('required', ['*', 'name'], {'ignore_types': ['UIScreenElementInformationElement', 'UIEmbeddedScreenElement','UIScreenLabel']}),
            ('required', ['UIScreenElementInformationElement', 'information_element_type'], None),

            ('ref_exists', ['UIGraphic', 'graphic_type_id', ['UIGraphicType']], None),
            ('ref_exists', ['UIScreenElement', 'screen_label_id', ['UIScreenLabel']], {'required':False}),
            ('ref_exists', ['UIResourceType', 'resource_supertype_id', ['UIResourceType']], {'required':False}),
            ('ref_exists', ['UIObjectType', 'object_supertype_id', ['UIObjectType']], {'required':False}),
            ('ref_exists', ['UIResourceAttribute', 'resource_type_id', ['UIResourceType']], None),
            ('ref_exists', ['UIResourceAttribute', 'default_widget_id', ['UIWidget']], None),
            ('ref_exists', ['UIResourceAttribute', 'base_attribute_id', ['UIResourceAttribute']], {'required':False}),
            ('ref_exists', ['UIResourceAttribute', 'object_type_id', ['UIObjectType']], {'required':False}),
            ('ref_exists', ['UIObjectField', 'object_type_id', ['UIObjectType']], None),
            ('ref_exists', ['UIObjectField', 'object_field_type_id', ['UIObjectType']], {'required':False}),
            ('ref_exists', ['UIObjectField', 'base_field_id', ['UIObjectField']], {'required':False}),
            ('ref_exists', ['UIScreenElement','widget_id', ['UIWidget']], None),
            ('ref_exists', ['UIScreenElement','screen_label_id', ['UIScreenLabel']], {'required':False}),
            ('ref_exists', ['UIScreenElement','state_id', ['UIState']], {'required':False}),
            ('ref_exists', ['UIScreenElement','help_tag_id', ['UIHelpTag']], {'required':False}),
            ('ref_exists', ['UIScreenElement','message_string_id', ['UIMessageString']], {'required':False}),
            ('ref_exists', ['UIScreenElementInformationElement', 'screen_element_id', ['UIScreenElement']], None),
            ('ref_exists', ['UIScreenElementInformationElement', 'information_element_id', ['UIResourceAttribute','UIResourceType','UIObjectField','UIObjectType']], None),
            ('ref_exists', ['UIEmbeddedScreenElement', 'embedding_screen_element_id', ['UIScreenElement']], None),
            ('ref_exists', ['UIEmbeddedScreenElement', 'embedded_screen_element_id', ['UIScreenElement']], None),
            ('ref_exists', ['UIEmbeddedScreenElement', 'override_graphic_id', ['UIGraphic']], {'required':False}),
            ('ref_exists', ['UIEmbeddedScreenElement', 'override_screen_label_id', ['UIScreenLabel']], {'required':False}),

        ]
        for check, ckargs, ckkwargs in ui_checks:
            ckargs = [] if ckargs is None else ckargs
            ckkwargs = {} if ckkwargs is None else ckkwargs
            checkfunc = getattr(self, "_checkui_%s" % check)
            checkfunc(*ckargs, **ckkwargs)

        if self.warnings:
            log.warn("WARNINGS:\n%s", "\n".join(["%s: %s" % (a, b) for a, b in self.warnings]))

    def _checkui_illegal_char(self, **kwargs):
        allow_linebreak = kwargs.get('allow_linebreak', [])
        for obj in self.ui_objs.values():
            for attr_name, attr_value in obj.__dict__.iteritems():
                match_char = 'a-zA-Z0-9_:&/,"@%=;<>+()*#?{}!$.^ \-\[\]\''
                if attr_name in allow_linebreak:
                    match_char += '\s'
                if attr_value and not re.match('^[' + match_char + ']*$', str(attr_value)):
                    new_attr = re.sub('[^' + match_char + ']', '', attr_value)
                    illegal = re.sub('[' + match_char + ']', '', attr_value)
                    setattr(obj, attr_name, new_attr)
                    msg = "illegal_char: %s.%s contains invalid chars: %s" % (obj._get_type(), attr_name, "")
                    self.warnings.append((obj.uirefid, msg))

    def _checkui_required(self, type_name, *args, **kwargs):
        drop = kwargs.get('drop', True)
        ignore_types = kwargs.get('ignore_types', [])
        droplist = []
        for obj in self.ui_objs.values():
            if (type_name == "*" and obj._get_type() not in ignore_types) or obj._get_type() == type_name:
                for attr in args:
                    ref_attr = getattr(obj, attr)
                    if not ref_attr:
                        msg = "required: %s.%s is empty. Drop=%s" % (obj._get_type(), attr, drop)
                        if drop:
                            droplist.append(obj.uirefid)
                        self.warnings.append((obj.uirefid, msg))
        for dropobj in droplist:
            del self.ui_objs[dropobj]

    def _checkui_ref_exists(self, type_name, attr, target_types, **kwargs):
        required = kwargs.get('required', True)
        for obj in self.ui_objs.values():
            if obj._get_type() == type_name:
                ref_attr = getattr(obj, attr)
                if not ref_attr and not required:
                    # Empty is ok if not required
                    pass
                elif not ref_attr in self.ui_objs:
                    msg = "ref_exists: %s.%s not an existing object reference (value=%s)" % (obj._get_type(), attr, ref_attr)
                    self.warnings.append((obj.uirefid, msg))
                else:
                    target_obj = self.ui_objs[ref_attr]
                    if target_obj._get_type() not in target_types:
                        msg = "ref_exists: %s.%s not a valid object type (id=%s, type=%s)" % (obj._get_type(), attr, ref_attr, target_obj._get_type())
                        self.warnings.append((obj.uirefid, msg))


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

        if auto_add:
            self._add_ui_object(refid, obj)

        return refid, obj

    def _loadui_GraphicType(self, row):
        refid, obj = self._build_ui_resource(row, "UIGraphicType",
                {'uirefid':'__pk_GraphicType_ID',
                 'name':'Name',
                 'description':'Description'})

    def _loadui_Graphic(self, row):
        refid, obj = self._build_ui_resource(row, "UIGraphic",
                {'uirefid':'__pk_Graphic_ID',
                 'graphic_type_id':'_fk_GraphicType_ID',
                 'name':'Name',
                 'description':'Description',
                 'filename':'Filename'})

    def _loadui_InformationLevel(self, row):
        refid, obj = self._build_ui_resource(row, "UIInformationLevel",
                {'uirefid':'__pk_InformationLevel_ID',
                 'level':'Level',
                 'name':'Level',
                 'description':'Description'})

    def _loadui_ResourceType(self, row):
        refid, obj = self._build_ui_resource(row, "UIResourceType",
                {'uirefid':'__pk_ResourceType_ID',
                 'name':'Name',
                 'ci_id':'CI ID',
                 'resource_supertype_id':'Supertype'})

    def _loadui_ObjectType(self, row):
        refid, obj = self._build_ui_resource(row, "UIObjectType",
                {'uirefid':'__pk_ObjectType_ID',
                 'object_supertype_id':'Object Supertype',
                 'name':'Name',
                 'description':'Description',
                 'ci_id':'CI ID'})

    def _loadui_ResourceAttribute(self, row):
        refid, obj = self._build_ui_resource(row, "UIResourceAttribute",
                {'uirefid':'__pk_ResourceAttribute_ID',
                 'name':'Name',
                 'information_level':'Information Level',
                 'object_type_id':'_fk_ObjectType_ID',
                 'resource_type_id':'_fk_ResourceType_ID',
                 'base_attribute_id':'Base Attribute',
                 'ci_id':'CI ID',
                 'default_widget_id':'Default Widget ID',
                 'description':'Description'})

    def _loadui_ObjectField(self, row):
        refid, obj = self._build_ui_resource(row, "UIObjectField",
                {'uirefid':'__pk_ObjectField_ID',
                 'ci_id':'CI ID',
                 'name':'Name',
                 'object_type_id':'Object Type',
                 'information_level':'Information Level',
                 'object_field_type_id':'Object Field Type',
                 'base_field_id':'Base Field',
                 'description':'Description'})

    def _loadui_ScreenElement(self, row):
        refid, obj = self._build_ui_resource(row, "UIScreenElement",
                {'uirefid':'__pk_ScreenElement_ID',
                 'graphic_id':'_fk_Graphic_ID',
                 'help_tag_id':'_fk_HelpTag_ID',
                 'message_string_id':'_fk_MessageString_ID',
                 'screen_label_id':'_fk_ScreenLabel_ID',
                 'state_id':'_fk_State_ID',
                 'widget_id':'_fk_Widget_ID',
                 'default_value':'Default Value',
                 'name':'Name',
                 'description':'Description'})

    def _loadui_ScreenLabel(self, row):
        refid, obj = self._build_ui_resource(row, "UIScreenLabel",
                {'uirefid':'__pk_ScreenLabel_ID',
                 'name':'Name',
                 'description':'Description',
                 'text':'Text',
                 'abbreviation':'Abbreviation'})

    def _loadui_State(self, row):
        refid, obj = self._build_ui_resource(row, "UIState",
                {'uirefid':'__pk_State_ID',
                 'name':'Name',
                 'description':'Description'})

    def _loadui_Widget(self, row):
        refid, obj = self._build_ui_resource(row, "UIWidget",
                {'uirefid':'__pk_Widget_ID',
                 'name':'Name',
                 'description':'Description',
                 'visual_sample_url':'Visual Sample URL'})

    def _loadui_ScreenElementInformationElement(self, row):
        refid, obj = self._build_ui_resource(row, "UIScreenElementInformationElement",
                {'uirefid':'__pk_jt_ScreenElement_InformationElement_ID',
                 'information_element_id':'_fk_InformationElement_ID',
                 'screen_element_id':'_fk_ScreenElement_ID',
                 'information_element_type':'Information Element Type'})

    def _loadui_EmbeddedScreenElement(self, row):
        refid, obj = self._build_ui_resource(row, "UIEmbeddedScreenElement",
                {'uirefid':'__pk_jt_ScreenElement_ScreenElement_ID',
                 'embedded_screen_element_id':'_fk_ScreenElement_ID #embedded',
                 'embedding_screen_element_id':'_fk_ScreenElement_ID #embedding',
                 'override_graphic_id':'_fk_Graphic_ID #override',
                 'override_information_level':'Embedding Information Level',
                 'override_screen_label_id':'_fk_ScreenLabel_ID #override',
                 'position':'Position',
                 'data_path':'Data Path',
                 'data_path_description':'Data Path Description'})
