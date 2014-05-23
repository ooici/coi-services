#!/usr/bin/env python

"""Utilities for unit and integration testing"""

from pyon.core import bootstrap
from pyon.ion.resource import get_restype_lcsm
from pyon.public import BadRequest, PRED, RT


def create_dummy_resources(res_list, assoc_list=None, container=None):
    """
    Creates all resources of res_list. The names of the resources will be the keys in the returned dict
    The elements of the list are tuples with resource object as first element and optional actor name as second element.
    Can also create associations linking resources by name.
    Assoc_list is a list of 3-tuples subject name, predicate, object name.
    """
    container = container or bootstrap.container_instance
    rr = container.resource_registry
    res_by_name = {}
    for res_entry in res_list:
        if isinstance(res_entry, dict):
            res_obj = res_entry["res"]
            actor_id = res_by_name[res_entry["act"]] if "act" in res_entry else None
        elif type(res_entry) in (list, tuple):
            res_obj = res_entry[0]
            actor_id = res_by_name[res_entry[1]] if len(res_entry) > 1 else None
        else:
            raise BadRequest("Unknown resource entry format")

        res_name = res_obj.name
        res_obj.alt_ids.append("TEST:%s" % res_name)
        res_lcstate = res_obj.lcstate
        rid, _ = rr.create(res_obj, actor_id=actor_id)
        res_by_name[res_name] = rid
        lcsm = get_restype_lcsm(res_obj.type_)
        if lcsm and res_lcstate != lcsm.initial_state:
            rr.set_lifecycle_state(rid, res_lcstate)

        if isinstance(res_entry, dict):
            if "org" in res_entry:
                rr.create_association(res_by_name[res_entry["org"]], PRED.hasResource, rid)

    if assoc_list:
        for assoc in assoc_list:
            sname, p, oname = assoc
            if type(sname) in (list, tuple) and type(oname) in (list, tuple):
                for sname1 in sname:
                    for oname1 in oname:
                        s, o = res_by_name[sname1], res_by_name[oname1]
                        rr.create_association(s, p, o)
            elif type(oname) in (list, tuple):
                for oname1 in oname:
                    s, o = res_by_name[sname], res_by_name[oname1]
                    rr.create_association(s, p, o)
            elif type(sname) in (list, tuple):
                for sname1 in sname:
                    s, o = res_by_name[sname1], res_by_name[oname]
                    rr.create_association(s, p, o)
            else:
                s, o = res_by_name[sname], res_by_name[oname]
                rr.create_association(s, p, o)

    return res_by_name

def create_dummy_events(event_list, container=None):
    container = container or bootstrap.container_instance
    ev_by_alias = {}
    for (alias, event) in event_list:
        evid, _ = container.event_repository.put_event(event)
        ev_by_alias[alias] = evid
    return ev_by_alias