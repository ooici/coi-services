#!/usr/bin/env python

"""Helper functions to work with extended resources"""

__author__ = 'Michael Meisinger'

from pyon.core.object import IonObjectBase
from pyon.public import OT, RT, log, CFG

from interface.objects import Resource, Event, ResourceContainer, ComputedValue, EventComputedAttributes, \
    ServiceRequest, Attachment, NegotiationRequest, NotificationRequest, UserInfo, Org

IGNORE_EXT_FIELDS = {"type_", "_id", "ts_created", "resource", "computed",
                     "availability_transitions", "lcstate_transitions", "ext_associations"}

CORE_RES_ATTRIBUTES = {"type_", "_id", "name", "description", "ts_updated",
                   "lcstate", "availability", "alt_resource_type"}

CORE_EVENT_ATTRIBUTES = {"type_", "ts_created"}

CORE_OBJ_ATTRIBUTES = {"type_"}


def get_matchers(matchers=None):
    basic_matchers = [matcher_special, matcher_resource, matcher_event]
    if matchers:
        return matchers + basic_matchers
    else:
        return basic_matchers

def strip_resource_extension(extres, matchers=None):
    if not CFG.get_safe("container.extended_resources.strip_results", True):
        return

    obj_matchers = get_matchers(matchers)
    if not extres or not isinstance(extres, ResourceContainer):
        return
    try:
        log.info("Stripping extended resource: %s (id=%s)", extres.type_, extres.resource._id)
        for k, v in extres.__dict__.iteritems():
            if k not in IGNORE_EXT_FIELDS:
                setattr(extres, k, strip_attribute(v, keep=True, matchers=obj_matchers))

        if extres.computed:
            for k, v in extres.computed.__dict__.iteritems():
                if k not in CORE_OBJ_ATTRIBUTES:
                    setattr(extres.computed, k, strip_attribute(v, keep=True, matchers=obj_matchers))

    except Exception as ex:
        log.exception("Could not strip extended resource")

# -----------------------------------------------------------------------------

def strip_attribute(attr, keep=False, matchers=None):
    res_attr = attr
    if type(attr) is str:
        res_attr = attr if keep or len(attr) <= 2 else "__"
    elif isinstance(attr, IonObjectBase):
        res_attr = None
        for matcher in matchers:
            res_attr = matcher(attr, matchers=matchers)
            if res_attr:
                break
    elif isinstance(attr, dict):
        if keep:
            res_attr = {k: strip_attribute(v, keep=True, matchers=matchers) for k, v in attr.iteritems()}
        else:
            res_attr = {}
    elif isinstance(attr, list):
        if keep:
            res_attr = [strip_attribute(v, keep=True, matchers=matchers) for v in attr]
        else:
            res_attr = []
    return res_attr


def matcher_special(obj, matchers=None):
    if not obj:
        return
    if isinstance(obj, ComputedValue):
        setattr(obj, "value", strip_attribute(obj.value, keep=True, matchers=matchers))
        return obj
    elif isinstance(obj, EventComputedAttributes):
        return obj
    elif isinstance(obj, ServiceRequest):
        return obj
    elif isinstance(obj, Attachment):
        return obj
    elif isinstance(obj, NegotiationRequest):
        return obj
    elif isinstance(obj, NotificationRequest):
        return obj
    elif isinstance(obj, Org):
        return obj


def matcher_resource(obj, matchers=None):
    if not obj or not isinstance(obj, Resource):
        return

    res_attr = {k:v for k, v in obj.__dict__.iteritems() if k in CORE_RES_ATTRIBUTES}
    res_attr["__noion__"] = True
    return res_attr

def matcher_event(obj, matchers=None):
    if not obj or not isinstance(obj, Event):
        return

    res_attr = {k:v for k, v in obj.__dict__.iteritems() if k in CORE_EVENT_ATTRIBUTES}
    res_attr["__noion__"] = True
    return res_attr

def matcher_DataProduct(obj, matchers=None):
    if not obj or not isinstance(obj, Resource) or obj.type_ != RT.DataProduct:
        return
    res_attr = {k: v for k, v in obj.__dict__.iteritems() if k in CORE_RES_ATTRIBUTES or k in {
        "processing_level_code", "quality_control_level"}}
    res_attr["__noion__"] = True
    return res_attr

def matcher_DeviceModel(obj, matchers=None):
    if not obj or not isinstance(obj, Resource) or (obj.type_ != RT.InstrumentModel and obj.type_ != RT.PlatformModel):
        return
    res_attr = {k: v for k, v in obj.__dict__.iteritems() if k in CORE_RES_ATTRIBUTES or k in {
        "baud_rate_default", "hotel_current", "manufacturer", "manufacturer_url", "power_source",
        "family_name", "instrument_family", "platform_family"}}
    res_attr["__noion__"] = True
    return res_attr

def matcher_Device(obj, matchers=None):
    if not obj or not isinstance(obj, Resource) or (obj.type_ != RT.InstrumentDevice and obj.type_ != RT.PlatformDevice):
        return
    res_attr = {k: v for k, v in obj.__dict__.iteritems() if k in CORE_RES_ATTRIBUTES}
    res_attr["__noion__"] = True
    return res_attr

def matcher_UserRole(obj, matchers=None):
    if not obj or not isinstance(obj, Resource) or obj.type_ != RT.UserRole:
        return
    res_attr = {k: v for k, v in obj.__dict__.iteritems() if k in CORE_RES_ATTRIBUTES or k in {
        "org_governance_name"}}
    res_attr["__noion__"] = True
    return res_attr

def matcher_UserInfo(obj, matchers=None):
    if not obj or not isinstance(obj, Resource) or obj.type_ != RT.UserInfo:
        return
    res_attr = {k: v for k, v in obj.__dict__.iteritems() if k in CORE_RES_ATTRIBUTES or k in {
        "contact"}}
    res_attr["__noion__"] = True
    return res_attr

def matcher_Site(obj, matchers=None):
    if not obj or not isinstance(obj, Resource) or (obj.type_ != RT.InstrumentSite and obj.type_ != RT.PlatformSite):
        return
    res_attr = {k: v for k, v in obj.__dict__.iteritems() if k in CORE_RES_ATTRIBUTES or k in {
        "constraint_list", "geospatial_point_center", "coordinate_reference_system"}}
    res_attr["__noion__"] = True
    return res_attr

def matcher_ParameterContext(obj, matchers=None):
    if not obj or not isinstance(obj, Resource) or obj.type_ != RT.ParameterContext:
        return
    res_attr = {k: v for k, v in obj.__dict__.iteritems() if k in CORE_RES_ATTRIBUTES or k in {
        "display_name", "units", "value_encoding"}}
    res_attr["__noion__"] = True
    return res_attr
