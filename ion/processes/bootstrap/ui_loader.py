#!/usr/bin/env python

"""Class that loads ION UI definitions from UI database exports into the resource registry"""

__author__ = 'Michael Meisinger'

import csv
import uuid
import json
import re
import os
import requests
import urllib
import time

from pyon.datastore.datastore import DatastoreManager
from pyon.ion.identifier import create_unique_resource_id
from pyon.public import log, iex, IonObject, RT
from pyon.util.containers import get_ion_ts

from interface import objects

class UILoader(object):

    DEFAULT_UISPEC_LOCATION = "https://userexperience.oceanobservatories.org/database-exports/"

    UI_RESOURCE_TYPES = [
        'UISpec',
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

    UI_LANGUAGES = {
        '': 'text',       # Default (English)
        'en-US': 'text',  # English
    }

    def __init__(self, process, container=None):
        self.process = process
        self.container = container or self.process.container
        self.files = {}
        self.read_from_local = True

    # ---------------------------------------------------------------------------

    def delete_ui(self):
        res_ids = []
        for restype in self.UI_RESOURCE_TYPES:
            res_is_list, _ = self.container.resource_registry.find_resources(restype, id_only=True)
            res_ids.extend(res_is_list)
            #log.debug("Found %s resources of type %s" % (len(res_is_list), restype))

        ds = DatastoreManager.get_datastore_instance("resources")
        docs = ds.read_doc_mult(res_ids)

        for doc in docs:
            doc['_deleted'] = True

        # TODO: Also delete associations

        ds.create_doc_mult(docs, allow_ids=True)
        log.info("Deleted %s UI resources and associations", len(docs))


    def load_ui(self, path, replace=True, specs_only=True, specs_path=None):
        """
        @brief Import UI definitions from the FileMakerPro database exported CVS files
                to ION resource objects.
        """
        if not path:
            raise iex.BadRequest("Must provide path")

        if path.startswith("http"):
            self._get_ui_files(path)
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
            ('Help Tag.csv', 'HelpTag'),
            ('Message String.csv', 'MessageString'),
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
        self.abort = False

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
                            reader = csv.DictReader(csvfile, delimiter=',')
                            for row in reader:
                                catfunc(row)
                                row_do += 1
                    except IOError, ioe:
                        log.warn("UI category file %s error: %s" % (filename, str(ioe)))
                        self.abort = True
                else:
                    # Read from string
                    if fname in self.files:
                        log.info("Loading UI category %s from retrieved file %s" % (category, fname))
                        csvfile = self.files[fname]
                        # This is a hack to be able to read from string
<<<<<<< HEAD
                        csvfile = csvfile.splitlines()
=======
                        csvfile.split(os.linesep)
>>>>>>> Improved AMS agent interface
                        reader = csv.DictReader(csvfile, delimiter=',')
                        for row in reader:
                            catfunc(row)
                            row_do += 1
                    else:
                        log.warn("UI category %s has no file %s" % (category, fname))
                        self.abort = True

            except Exception as ex:
                log.warn("UI category %s error: %s" % (category, str(ex)))
                self.abort = True

            log.info("Loaded UI category %s: %d rows valid, %d rows skipped" % (category, row_do, row_skip))

        # Do some validation checking
        self._perform_ui_checks()

        # This is the latest point to bail
        if self.abort:
            raise iex.Inconsistent("UI specs load error and/or inconsistent")

        if replace:
            # Delete old UI objects first
            self.delete_ui()

        try:
            if specs_only:
                # Only write one UISpec resource, not a multitude of UIResource objects for each entry
                specs = self.get_ui_specs(ui_objs=self.ui_objs)
                obj_list,_ = self.container.resource_registry.find_resources(restype=RT.UISpec, name="ION UI Specs", id_only=False)
                if obj_list:
                    spec_obj = obj_list[0]
                    spec_obj.spec = specs
                    self.container.resource_registry.update(spec_obj)
                else:
                    spec_obj = IonObject('UISpec', name="ION UI Specs", spec=specs)
                    res_id = self.container.resource_registry.create(spec_obj)
                spec_size = len(json.dumps(spec_obj.spec))
                log.info("Wrote UISpec object, size=%s", spec_size)

                if specs_path:
                    self.export_ui_specs(specs_path, specs=specs)
            else:
                # Write the full set of UIResource objects
                self._finalize_uirefs()
                ds = DatastoreManager.get_datastore_instance("resources")
                res = ds.create_mult(self.ui_obj_by_id.values(), allow_ids=True)
                log.info("Stored %s UI resource objects into resource registry" % (len(res)))
                res = ds.create_mult(self.ui_assocs)
                log.info("Stored %s UI resource associations into resource registry" % (len(res)))
        except Exception as ex:
            log.exception("Store in resource registry error err=%s" % (str(ex)))


    def _get_ui_files(self, path):
        dirurl = path or self.DEFAULT_UISPEC_LOCATION
        if not dirurl.endswith("/"):
            dirurl += "/"
        log.info("Accessing UI specs URL: %s", dirurl)
        dirpage = requests.get(dirurl).text
        csvfiles = re.findall('(?:href|HREF)="([-%/\w]+\.csv)"', dirpage)
        log.debug("Found %s csvfiles: %s", len(csvfiles), csvfiles)

        #tmp_dir = "./ui_export/%s" % int(time.time())
        #if not os.path.exists(tmp_dir):
        #    os.makedirs(tmp_dir)

        s = requests.session()
        for file in csvfiles:
            file = file.rsplit('/', 1)[-1]
            csvurl = dirurl + file
            #log.info("Trying download %s", csvurl)
            content = s.get(csvurl).content
            self.files[urllib.unquote(file)] = content
            log.info("Downloaded %s, size=%s", csvurl, len(content))
        #    with open("%s/%s" % (tmp_dir, urllib.unquote(file)), "wb") as f:
        #        f.write(content)

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

            ('persists', None, None),
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

    def _checkui_persists(self, **kwargs):
        for obj in self.ui_objs.values():
            # Check object
            try:
                json.dumps(obj.__dict__.copy())
            except Exception as ex:
                #log.exception("Object %s problem" % obj)
                msg = "persists: object cannot be persisted: %s" % (repr(ex))
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

    def _finalize_uirefs(self):
        # Create real resource IDs
        for obj in self.ui_objs.values():
            #oid = self.uiid_prefix + obj.uirefid
            oid = create_unique_resource_id()
            obj._id = oid
            self.ui_obj_by_id[oid] = obj

    def _build_ui_resource(self, row, objtype, mapping, auto_add=True):
        refid = None
        obj_fields = {}
        for obj_attr, row_attr in mapping.iteritems():
            row_val = row[row_attr]

            obj_fields[obj_attr] = row_val
            if obj_attr == "uirefid":
                refid = row_val

        obj = IonObject(objtype, **obj_fields)
        if not obj.name:
            obj.name = 'TBD'

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

    def _loadui_HelpTag(self, row):
        refid, obj = self._build_ui_resource(row, "UIHelpTag",
                {'uirefid':'__pk_HelpTag_ID',
                 'name':'Name',
                 'description':'Description',
                 'text':'Text'})

    def _loadui_MessageString(self, row):
        refid, obj = self._build_ui_resource(row, "UIMessageString",
                {'uirefid':'__pk_MessageString_ID',
                 'name':'Name',
                 'description':'Description',
                 'text':'Text'})

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

    # -------------------------------------------------------------------------

    def export_ui_specs(self, filename, specs=None):
        """
        @brief Retrieve UI specs from resource registry, extract UI specs
                and save as JSON file
        """
        try:
            if not specs:
                ui_specs = self.get_ui_specs()
            else:
                ui_specs = specs
            json_specs = json.dumps(ui_specs)
            log.info("Generated JSON UI specs, len=%s", len(json_specs))
            with open(filename, 'w') as f:
                f.write(json_specs)
        except IOError as ioe:
            log.exception("Cannot save UIspecs json")
            raise BadRequest('IOError: %s' % repr(ioe))

    def get_ui_specs(self, user_id='', language='', verbose=False, strip=False, ui_objs=None):
        rr = self.container.resource_registry

        warnings = []       # Warning strings
        widgets = {}        # Dict of widget types containing widgets
        widgets_names = {}  # Mapping of widget name to id
        elements = {}       # Dict of screen elements
        graphics = {}       # Dict of graphics
        helptags = {}       # Dict of help tags
        msgstrs = {}        # Dict of message strings
        restypes = {}       # Dict of resource types

        # Pass 1: Load UI resources by type from resource registry
        if ui_objs is None:
            ui_objs = {}
            for rt in self.UI_RESOURCE_TYPES:
                res_list,_ = rr.find_resources(rt, id_only=False)
                log.info("Found %s UI resources of type %s", len(res_list), rt)
                for obj in res_list:
                    ui_objs[obj.uirefid] = obj
        else:
            log.info("get_ui_specs() working with %s UI objects", len(ui_objs))

        # Pass 2: Perform initial binning of UI resources objects by type
        for obj in ui_objs.values():
            if obj._get_type() == "UIWidget":
                widget_dict = dict(
                    name=obj.name,
                    #wid=obj.uirefid,
                    elements=[])
                if verbose:
                    widget_dict['desc'] = obj.description
                widgets[obj.uirefid] = widget_dict
                # Create a reverse mapping for widget names
                widgets_names[obj.name] = obj.uirefid
            elif obj._get_type() == "UIGraphic":
                graphic_dict = dict(
                    name=obj.name,
                    gtype=obj.graphic_type_id,
                    file=obj.filename)
                if verbose:
                    graphic_dict['desc'] = obj.description
                graphics[obj.uirefid] = graphic_dict
            elif obj._get_type() == "UIHelpTag":
                elem_dict = dict(
                    name=obj.name,
                    text=obj.text)
                if verbose:
                    elem_dict['desc'] = obj.description
                helptags[obj.uirefid] = elem_dict
            elif obj._get_type() == "UIMessageString":
                elem_dict = dict(
                    name=obj.name,
                    text=obj.text)
                if verbose:
                    elem_dict['desc'] = obj.description
                msgstrs[obj.uirefid] = elem_dict
            elif obj._get_type() == "UIResourceType":
                elem_dict = dict(
                    name=obj.name,
                    super=obj.resource_supertype_id)
                if verbose:
                    elem_dict['desc'] = obj.description
                restypes[obj.uirefid] = elem_dict
            elif obj._get_type() == "UIScreenElement":
                # Add object to elements dict
                element_dict = dict(
                    #elid=obj.uirefid,
                    name=obj.name,
                    wid=obj.widget_id,
                    gfx=obj.graphic_id,
                    tag=obj.help_tag_id,
                    msg=obj.message_string_id,
                    state=obj.state_id,
                    value=obj.default_value,
                    embed=[])
                label_text = ''
                if obj.screen_label_id:
                    label = ui_objs.get(obj.screen_label_id, None)
                    if not label:
                        msg = "UIScreenElement label %s not in screen labels" % (obj.screen_label_id)
                        warnings.append((obj.uirefid, msg))
                    else:
                        label_text = label.text
                element_dict['label'] = label_text

                if verbose:
                    element_dict['desc'] = obj.description
                elements[obj.uirefid] = element_dict

        # Pass 3: Build embedded screen elements and associate information elements
        for obj in ui_objs.values():
            if obj._get_type() == "UIEmbeddedScreenElement":
                parent = elements.get(obj.embedding_screen_element_id, None)
                if not parent:
                    msg = "UIEmbeddedScreenElement parent %s not in screen elements" % (obj.embedding_screen_element_id)
                    warnings.append((obj.uirefid, msg))
                    continue
                child = elements.get(obj.embedded_screen_element_id, None)
                if not child:
                    msg = "UIEmbeddedScreenElement child %s not in screen elements" % (obj.embedded_screen_element_id)
                    warnings.append((obj.uirefid, msg))
                    continue

                embed_dict = dict(
                    elid=obj.embedded_screen_element_id,
                    wid=child['wid'],
                    ogfx=obj.override_graphic_id,
                    olevel=obj.override_information_level,
                    pos=obj.position,
                    dpath=obj.data_path)
                if obj.override_screen_label_id:
                    label = ui_objs.get(obj.override_screen_label_id, None)
                    if not label:
                        msg = "UIEmbeddedScreenElement override label %s not in screen labels" % (obj.override_screen_label_id)
                        warnings.append((obj.uirefid, msg))
                    else:
                        label_text = label.text
                element_dict['olabel'] = label_text

                if verbose:
                    embed_dict['dpath_desc'] = obj.data_path_description
                parent['embed'].append(embed_dict)

            elif obj._get_type() == "UIScreenElementInformationElement":
                element = elements.get(obj.screen_element_id, None)
                if not element:
                    msg = "UIScreenElementInformationElement screen element %s not in elements" % (obj.screen_element_id)
                    warnings.append((obj.uirefid, msg))
                    continue
                info = ui_objs.get(obj.information_element_id, None)
                if not info:
                    msg = "UIScreenElementInformationElement information element %s not found" % (obj.information_element_id)
                    warnings.append((obj.uirefid, msg))
                    continue
                if 'info' in element:
                    msg = "UIScreenElementInformationElement screen element %s already has information element %s" % (obj.screen_element_id, obj.information_element_id)
                    warnings.append((obj.uirefid, msg))
                    continue
                info_dict = dict(ie_name=info.name)
                if info._get_type() == 'UIResourceType':
                    info_dict['ie_type'] = 'RT'

                elif info._get_type() == 'UIResourceAttribute':
                    info_dict['ie_type'] = 'RA'
                    info_dict['ie_level'] = info.information_level
                    info_dict['ie_wid'] = info.default_widget_id
                    ot_name = ''
                    if info.object_type_id:
                        ot = ui_objs.get(info.object_type_id, None)
                        if not ot:
                            msg = "UIScreenElementInformationElement object type %s for resource attribute %s not found" % (info.object_type_id, info.uirefid)
                            warnings.append((obj.uirefid, msg))
                        else:
                            ot_name = ot.name
                    info_dict['ie_ot'] = ot_name
                    rt_name = ''
                    if info.resource_type_id:
                        rt = ui_objs.get(info.resource_type_id, None)
                        if not rt:
                            msg = "UIScreenElementInformationElement resource type %s for resource attribute %s not found" % (info.object_type_id, info.uirefid)
                            warnings.append((obj.uirefid, msg))
                        else:
                            rt_name = rt.name
                    info_dict['ie_rt'] = rt_name
                elif info._get_type() == 'UIObjectType':
                    info_dict['ie_type'] = 'OT'
                elif info._get_type() == 'UIObjectField':
                    info_dict['ie_type'] = 'OF'
                    info_dict['ie_level'] = info.information_level
                    ot_name = ''
                    if info.object_type_id:
                        ot = ui_objs.get(info.object_type_id, None)
                        if not ot:
                            msg = "UIScreenElementInformationElement object type %s for object field %s not found" % (info.object_type_id, info.uirefid)
                            warnings.append((obj.uirefid, msg))
                        else:
                            ot_name = ot.name
                    info_dict['ie_ot'] = ot_name

                if verbose:
                    info_dict['ie_desc'] = obj.description
                element['ie'] = info_dict
                #element.update(info_dict)

            elif obj._get_type() == "UIScreenElement":
                widgets[obj.widget_id]['elements'].append(obj.uirefid)


        # Pass 4: Sort of embeds and removal of unnecessary attributes (elements)
        for element in elements.values():
            if element['embed']:
                element['embed'] = sorted(element['embed'], key=lambda obj: obj['pos'])
            else:
                if strip:
                    # Strip unreferenced in embedding
                    del element['embed']

        # Build the resulting structure
        ui_specs = {}   # The resulting data structure
        ui_specs['widgets'] = widgets
        ui_specs['elements'] = elements
        ui_specs['graphics'] = graphics
        ui_specs['helptags'] = helptags
        ui_specs['msgstrings'] = msgstrs
        ui_specs['restypes'] = restypes
        if verbose:
            ui_specs['objects'] = ui_objs

        if warnings:
            log.warn("UI SPEC GENERATION WARNINGS:\n%s", "\n".join(["%s: %s" % (a, b) for a, b in warnings]))

        return ui_specs
