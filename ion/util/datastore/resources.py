#!/usr/bin/env python

"""Helpers to work with the resource registry"""

__author__ = 'Michael Meisinger'


import datetime
import json
import yaml
import os
try:
    import xlrd
    import xlwt
except ImportError:
    print "Excel imports failed"

from ooi.timer import get_accumulators

from pyon.core import bootstrap
from pyon.core.bootstrap import CFG, get_sys_name
from pyon.datastore.datastore import DatastoreManager, DataStore
from pyon.public import log, RT, PRED, BadRequest, NotFound, Inconsistent


class ResourceRegistryUtil(object):
    def __init__(self, container = None):
        self.container = container or bootstrap.container_instance
        self.sysname = get_sys_name()
        self.rr = self.container.resource_registry

    def get_actor_users(self, actors):
        actor_ids = {a if isinstance(a, basestring) else a._id for a in actors}
        # TODO: Restrict to subjects in actor_ids only
        uinfo_assocs = self.rr.find_associations(predicate=PRED.hasInfo, id_only=False)
        uinfo_assocs = [uia for uia in uinfo_assocs if uia.ot == RT.UserInfo and uia.s in actor_ids]
        uinfo_ids = list({uia.o for uia in uinfo_assocs})
        uinfo_objs = self.rr.read_mult(uinfo_ids)
        return uinfo_objs

    def get_actor_user_map(self, actors):
        actor_ids = {a if isinstance(a, basestring) else a._id for a in actors}
        # TODO: Restrict to subjects in actor_ids only
        uinfo_assocs = self.rr.find_associations(predicate=PRED.hasInfo, id_only=False)
        uinfo_assocs = [uia for uia in uinfo_assocs if uia.ot == RT.UserInfo and uia.s in actor_ids]
        uinfo_ids = list({uia.o for uia in uinfo_assocs})
        uinfo_objs = self.rr.read_mult(uinfo_ids)
        uinfo_map = {rid: robj for rid, robj in zip(uinfo_ids, uinfo_objs)}
        actor_ui_map = {uia.s: uinfo_map[uia.o] for uia in uinfo_assocs}
        return actor_ui_map

"""
from ion.util.datastore.resources import ResourceRegistryHelper
rrh = ResourceRegistryHelper()
rrh.dump_resources_as_xlsx()
rrh.revert_to_snapshot(filename="interface/rrsnapshot_20130530_144619.json")
"""

class ResourceRegistryHelper(object):
    def __init__(self, container = None):
        self.container = container or bootstrap.container_instance
        self.sysname = get_sys_name()

        self._clear()

    def _clear(self):
        self._resources = {}
        self._associations = {}
        self._assoc_by_sub = {}
        self._directory = {}
        self._res_by_type = {}
        self._resobj_by_type = {}
        self._attr_by_type = {}

    def dump_resources_as_xlsx(self, filename=None):
        self._clear()
        ds = DatastoreManager.get_datastore_instance(DataStore.DS_RESOURCES, DataStore.DS_PROFILE.RESOURCES)
        all_objs = ds.find_docs_by_view("_all_docs", None, id_only=False)

        log.info("Found %s objects in datastore resources", len(all_objs))

        self._analyze_objects(all_objs)

        self._wb = xlwt.Workbook()
        self._worksheets = {}

        self._dump_observatories()

        self._dump_network()

        for restype in sorted(self._res_by_type.keys()):
            self._dump_resource_type(restype)

        dtstr = datetime.datetime.today().strftime('%Y%m%d_%H%M%S')
        path = filename or "interface/resources_%s.xls" % dtstr
        self._wb.save(path)

    def dump_dicts_as_xlsx(self, objects, filename=None):
        self._clear()
        """Dumps a dict of dicts. Tab names will be the names of keys in the objects dict"""
        self._wb = xlwt.Workbook()
        self._worksheets = {}

        for cat_name, cat_objects in objects.iteritems():
            for obj_id, obj in cat_objects.iteritems():
                if not isinstance(obj, dict):
                    raise Inconsistent("Object of bad type found: %s" % type(obj))
                self._resources[obj_id] = obj
                self._res_by_type.setdefault(cat_name, []).append(obj_id)
                self._resobj_by_type.setdefault(cat_name, {})[obj_id] = obj
                self._attr_by_type.setdefault(cat_name, set()).update(obj.keys())

        for restype in sorted(self._res_by_type.keys()):
            self._dump_resource_type(restype)

        dtstr = datetime.datetime.today().strftime('%Y%m%d_%H%M%S')
        path = filename or "interface/objects_%s.xls" % dtstr
        self._wb.save(path)

    def _analyze_objects(self, resources_objs):
        for obj_id, key, obj in resources_objs:
            if obj_id.startswith("_design"):
                continue
            if not isinstance(obj, dict):
                raise Inconsistent("Object of bad type found: %s" % type(obj))
            obj_type = obj.get("type_", None)
            if obj_type == "Association":
                self._associations[obj_id] = obj
            elif obj_type == "DirEntry":
                self._directory[obj_id] = obj
            elif obj_type:
                self._resources[obj_id] = obj
                if obj_type not in self._res_by_type:
                    self._res_by_type[obj_type] = []
                self._res_by_type[obj_type].append(obj_id)
                for attr, value in obj.iteritems():
                    if obj_type not in self._attr_by_type:
                        self._attr_by_type[obj_type] = set()
                    self._attr_by_type[obj_type].add(attr)
            else:
                raise Inconsistent("Object with no type_ found: %s" % obj)

        for assoc in self._associations.values():
            key = (assoc['s'], assoc['p'])
            if key not in self._assoc_by_sub:
                self._assoc_by_sub[key] = []
            self._assoc_by_sub[key].append(assoc['o'])

    def _dump_resource_type(self, restype):
        """Dumpe one spreadsheet tab"""
        ws = self._wb.add_sheet(restype)
        self._worksheets[restype] = ws
        for j, attr in enumerate(sorted(self._attr_by_type[restype])):
            ws.write(0, j, attr)

        res_objs = [self._resobj_by_type.get(restype, {}).get(res_id, None) or self._resources[res_id] for res_id in self._res_by_type[restype]]
        res_objs.sort(key=lambda doc: doc.get('name', ""))
        for i, res_obj in enumerate(res_objs):
            for j, attr in enumerate(sorted(list(self._attr_by_type[restype]))):
                value = res_obj.get(attr, "")
                if type(value) is str:
                    value = unicode(value, "latin1")
                    wvalue = value.encode("ascii", "replace")[:32760]
                    ws.write(i+1, j, wvalue)
                elif type(value) is unicode:
                    wvalue = value.encode("ascii", "replace")[:32760]
                    ws.write(i+1, j, wvalue)
                elif type(value) in (bool, int, None, float):
                    ws.write(i+1, j, value)
                elif isinstance(value, dict):
                    if value.get("type_", None):
                        obj_type = value.pop("type_")
                        wvalue = json.dumps(value)[:32760]
                        ws.write(i+1, j, obj_type + ":" + wvalue)
                    else:
                        wvalue = json.dumps(value)[:32760]
                        ws.write(i+1, j, wvalue)
                elif isinstance(value, list):
                    wvalue = json.dumps(value)[:32760]
                    ws.write(i+1, j, wvalue)
                else:
                    wvalue = str(value)[:32760]
                    ws.write(i+1, j, wvalue)

    def _dump_observatories(self):
        ws = self._wb.add_sheet("OBS")
        [ws.write(0, col, hdr) for (col, hdr) in enumerate(["Type", "Reference Designator", "Facility", "Geo Area", "Site", "Station", "Component", "Instrument"])]
        self._row = 1

        def follow_site(parent_id, level):
            site_list = [self._resources[site_id] for site_id in self._assoc_by_sub.get((parent_id, "hasSite"), [])]
            site_list.sort(key=lambda obj: obj['name'])
            for site in site_list:
                ws.write(self._row, 0, site['type_'])
                ws.write(self._row, 1, ",".join([i[4:] for i in site['alt_ids'] if i.startswith("OOI:")]))
                if site['type_'] == "InstrumentSite":
                    ilevel = max(7, level)
                    ws.write(self._row, ilevel, site['name'])
                else:
                    ws.write(self._row, level, site['name'])
                self._row += 1
                follow_site(site['_id'], level+1)

        org_list = [self._resources[org_id] for org_id in self._res_by_type.get("Org", [])]
        org_list.sort(key=lambda obj: obj['name'])
        for org in org_list:
            ws.write(self._row, 0, org['type_'])
            ws.write(self._row, 2, org['name'])
            self._row += 1

            obs_list = [self._resources[obs_id] for obs_id in self._assoc_by_sub.get((org["_id"], "hasResource"), [])]
            obs_list.sort(key=lambda obj: (obj.get('spatial_area_name', ""), obj['name']))
            prior_area = ""
            for obs in obs_list:
                if obs["type_"] == "Observatory":
                    if obs['spatial_area_name'] != prior_area:
                        prior_area = obs['spatial_area_name']
                        ws.write(self._row, 0, "(none)")
                        ws.write(self._row, 3, obs['spatial_area_name'])
                        self._row += 1
                    ws.write(self._row, 0, obs['type_'])
                    ws.write(self._row, 1, ",".join([i[4:] for i in obs['alt_ids'] if i.startswith("OOI:")]))
                    ws.write(self._row, 4, obs['name'])
                    self._row += 1
                    follow_site(obs['_id'], 5)

    def _dump_network(self):
        ws = self._wb.add_sheet("Network")
        [ws.write(0, col, hdr) for (col, hdr) in enumerate(["Network"])]
        self._row = 1

        parents = {}
        child_set = set()

        for assoc in self._associations.values():
            if assoc['p'] == "hasNetworkParent":
                parent_id = assoc['o']
                if parent_id not in parents:
                    parents[parent_id] = []
                parents[parent_id].append(assoc['s'])
                child_set.add(assoc['s'])

        roots = set(parents.keys()) - child_set

        def follow_dev(dev_id, level):
            dev_obj = self._resources[dev_id]
            ws.write(self._row, level, dev_obj['name'])
            self._row += 1
            if dev_id in parents:
                for ch_id in parents[dev_id]:
                    follow_dev(ch_id, level+1)

        for dev_id in roots:
            follow_dev(dev_id, 0)

    def dump_accumulators_as_xlsx(self, filename=None):
        dtstr = datetime.datetime.today().strftime('%Y%m%d_%H%M%S')

        all_acc_dict = {}
        for acc_name, acc in get_accumulators().iteritems():
            acc_dict = {}
            acc_name = acc_name.split(".")[-1]
            acc_name = acc_name[:30]
            all_acc_dict[acc_name] = acc_dict
            for key in acc.keys():
                count = acc.get_count(key)
                if count:
                    acc_dict[key] = dict(
                        _key=key,
                        count=count,
                        sum=acc.get_average(key) * count,
                        min=acc.get_min(key),
                        avg=acc.get_average(key),
                        max=acc.get_max(key),
                        sdev=acc.get_standard_deviation(key)
                    )

        path = filename or "interface/accumulators_%s.xls" % (dtstr)
        self.dump_dicts_as_xlsx(all_acc_dict, path)

    def create_resources_snapshot(self, persist=False, filename=None):
        ds = DatastoreManager.get_datastore_instance(DataStore.DS_RESOURCES, DataStore.DS_PROFILE.RESOURCES)
        all_objs = ds.find_docs_by_view("_all_docs", None, id_only=False)

        log.info("Found %s objects in datastore resources", len(all_objs))

        resources = {}
        associations = {}
        snapshot = dict(resources=resources, associations=associations)

        for obj_id, key, obj in all_objs:
            if obj_id.startswith("_design"):
                continue
            if not isinstance(obj, dict):
                raise Inconsistent("Object of bad type found: %s" % type(obj))
            obj_type = obj.get("type_", None)
            if obj_type == "Association":
                associations[obj_id] = obj.get("ts", None)
            elif obj_type:
                resources[obj_id] = obj.get("ts_updated", None)
            else:
                raise Inconsistent("Object with no type_ found: %s" % obj)

        if persist:
            dtstr = datetime.datetime.today().strftime('%Y%m%d_%H%M%S')
            path = filename or "interface/rrsnapshot_%s.json" % dtstr
            snapshot_json = json.dumps(snapshot)
            with open(path, "w") as f:
                #yaml.dump(snapshot, f, default_flow_style=False)
                f.write(snapshot_json)

        log.debug("Created resource registry snapshot. %s resources, %s associations", len(resources), len(associations))

        return snapshot

    def revert_to_snapshot(self, snapshot=None, filename=None):
        current_snapshot = self.create_resources_snapshot()

        if filename:
            if not os.path.exists(filename):
                raise BadRequest("Snapshot file not existing: %s" % filename)
            with open(filename, "r") as f:
                content = f.read()
                snapshot = json.loads(content)

        delta_snapshot = self._compare_snapshots(snapshot, current_snapshot)
        if delta_snapshot["resources"] or delta_snapshot["associations"]:
            res_ids = delta_snapshot["resources"].keys()
            assoc_ids = delta_snapshot["associations"].keys()

            log.debug("Reverting to old snapshot. Deleting %s resources and %s associations", len(res_ids), len(assoc_ids))
            self.container.resource_registry.rr_store.delete_mult(res_ids)
            self.container.resource_registry.rr_store.delete_mult(assoc_ids)

    def _compare_snapshots(self, old_snapshot, new_snapshot):
        delta_snapshot = {}
        for key in new_snapshot:
            key_delta = {}
            delta_snapshot[key] = key_delta
            old_key_snapshot = old_snapshot[key]
            for obj_id, ts in new_snapshot[key].iteritems():
                if obj_id not in old_key_snapshot or ts != old_key_snapshot[obj_id]:
                    key_delta[obj_id] = ts

        return delta_snapshot

    def dump_container_stats_as_xlsx(self, filename=None):
        self._wb = xlwt.Workbook()
        self._worksheets = {}

        cc_objs, _ = self.container.resource_registry.find_resources(restype=RT.CapabilityContainer, id_only=False)
        for cc in cc_objs:
            name_parts = cc.name.split("_")
            if len(name_parts) >= 2:
                tab_name = "%s_%s" % (name_parts[0], name_parts[-1])
            else:
                tab_name = cc.name
            ws = self._wb.add_sheet(tab_name[:31])
            [ws.write(0, col, hdr) for (col, hdr) in enumerate(["RNum", "LNum", "Type"])]
            self._row = 1

            if not cc.status_log:
                continue
            cc_status = cc.status_log[0]

            # Basic
            basic_stats = cc_status.get("basic", {})
            basic_stats.update({"snap."+k:v for k,v in cc_status.iteritems() if isinstance(v, str)})
            basic_stats.update({"cc.id": cc.name})
            [ws.write(self._row, col, hdr) for (col, hdr) in enumerate([self._row, 0, "Basic", "Key", "Value"])]
            self._row += 1
            for lnum, sn in enumerate(sorted(basic_stats.keys())):
                stat = basic_stats[sn]
                ws.write(self._row, 0, self._row)
                ws.write(self._row, 1, lnum+1)
                ws.write(self._row, 2, "Basic")
                ws.write(self._row, 3, sn)
                ws.write(self._row, 4, str(stat))
                self._row += 1

            # Processes
            ws.write(self._row, 0, self._row)
            self._row += 1
            process_stats = cc_status.get("processes", {}).get("procs", {})
            [ws.write(self._row, col, hdr) for (col, hdr) in enumerate([self._row, 0, "Process", "ID"])]
            if process_stats:
                proc = process_stats[process_stats.keys()[0]]
                for i, key in enumerate(sorted(proc.keys())):
                    ws.write(self._row, i+4, key)
            self._row += 1

            for lnum, pid in enumerate(sorted(process_stats.keys())):
                proc = process_stats[pid]
                ws.write(self._row, 0, self._row)
                ws.write(self._row, 1, lnum+1)
                ws.write(self._row, 2, "Process")
                ws.write(self._row, 3, pid)
                for i, key in enumerate(sorted(proc.keys())):
                    ws.write(self._row, i+4, str(proc[key]))
                self._row += 1

            # Policy
            ws.write(self._row, 0, self._row)
            self._row += 1
            policy_stats = cc_status.get("policy", {})
            [ws.write(self._row, col, hdr) for (col, hdr) in enumerate([self._row, 0, "Policy", "PType", "Service/Resource", "Name", "Effect", "Description"])]
            self._row += 1
            lnum = 1
            rpol = policy_stats.get("common_pdp", [])
            for i, prule in enumerate(rpol):
                ws.write(self._row, 0, self._row)
                ws.write(self._row, 1, lnum)
                ws.write(self._row, 2, "Policy")
                ws.write(self._row, 3, "Common Policy")
                ws.write(self._row, 5, prule.get("id", ""))
                ws.write(self._row, 6, prule.get("effect", ""))
                ws.write(self._row, 7, prule.get("description", ""))
                self._row += 1
                lnum += 1

            for rname in sorted(policy_stats.get("service_pdp", {}).keys()):
                rpol = policy_stats.get("service_pdp", {})[rname]

                for i, prule in enumerate(rpol):
                    ws.write(self._row, 0, self._row)
                    ws.write(self._row, 1, lnum)
                    ws.write(self._row, 2, "Policy")
                    ws.write(self._row, 3, "Service Policy")
                    ws.write(self._row, 4, rname)
                    ws.write(self._row, 5, prule.get("id", ""))
                    ws.write(self._row, 6, prule.get("effect", ""))
                    ws.write(self._row, 7, prule.get("description", ""))
                    self._row += 1
                    lnum += 1

            for rname in sorted(policy_stats.get("resource_pdp", {}).keys()):
                rpol = policy_stats.get("resource_pdp", {})[rname]

                for i, prule in enumerate(rpol):
                    ws.write(self._row, 0, self._row)
                    ws.write(self._row, 1, lnum)
                    ws.write(self._row, 2, "Policy")
                    ws.write(self._row, 3, "Resource Policy")
                    ws.write(self._row, 4, rname)
                    ws.write(self._row, 5, prule.get("id", ""))
                    ws.write(self._row, 6, prule.get("effect", ""))
                    ws.write(self._row, 7, prule.get("description", ""))
                    self._row += 1
                    lnum += 1

            for rname in sorted(policy_stats.get("service_precondition", {}).keys()):
                rpol = policy_stats.get("service_precondition", {})[rname]

                for pname in sorted(rpol):
                    ws.write(self._row, 0, self._row)
                    ws.write(self._row, 1, lnum)
                    ws.write(self._row, 2, "Policy")
                    ws.write(self._row, 3, "Service Precond")
                    ws.write(self._row, 4, rname)
                    ws.write(self._row, 5, pname)
                    self._row += 1
                    lnum += 1

            # Policy Update Log
            ws.write(self._row, 0, self._row)
            self._row += 1
            policy_stats = cc_status.get("policy", {}).get("update_log", [])
            [ws.write(self._row, col, hdr) for (col, hdr) in enumerate([self._row, 0, "PolicyUpdate", "PType", "TS", "Message"])]
            self._row += 1
            for lnum, plog in enumerate(policy_stats):
                ws.write(self._row, 0, self._row)
                ws.write(self._row, 1, lnum+1)
                ws.write(self._row, 2, "PolicyUpdate")
                ws.write(self._row, 3, plog["update_type"])
                ws.write(self._row, 4, plog["update_ts"])
                ws.write(self._row, 5, plog["message"])
                cnum = 6
                for key in sorted(plog.keys()):
                    if key in ("update_type", "update_ts", "message"):
                        continue
                    puval = plog[key]
                    ws.write(self._row, cnum, "%s=%s" % (key, puval))
                    cnum += 1
                self._row += 1

            # Accumulators
            ws.write(self._row, 0, self._row)
            self._row += 1
            [ws.write(self._row, col, hdr) for (col, hdr) in enumerate([self._row, 0, "Accumulator", "Name", "Count", "Min", "Avg", "Max", "Sum", "SDev"])]
            self._row += 1
            acc_stats = cc_status.get("accumulators", {})
            lnum = 1
            for akey in sorted(acc_stats.keys()):
                acc = acc_stats[akey]
                for akey1 in sorted(acc.keys()):
                    acc1 = acc[akey1]
                    ws.write(self._row, 0, self._row)
                    ws.write(self._row, 1, lnum+1)
                    ws.write(self._row, 2, "Accumulator")
                    ws.write(self._row, 3, "%s.%s" % (akey, akey1))
                    ws.write(self._row, 4, str(acc1.get("count", "")))
                    ws.write(self._row, 5, str(acc1.get("min", "")))
                    ws.write(self._row, 6, str(acc1.get("avg", "")))
                    ws.write(self._row, 7, str(acc1.get("max", "")))
                    ws.write(self._row, 8, str(acc1.get("sum", "")))
                    ws.write(self._row, 9, str(acc1.get("sdev", "")))
                    self._row += 1
                    lnum += 1

            # Path
            ws.write(self._row, 0, self._row)
            self._row += 1
            [ws.write(self._row, col, hdr) for (col, hdr) in enumerate([self._row, 0, "Path", "Dir"])]
            self._row += 1
            config_stats = cc_status.get("config", {}).get("sys.path", [])
            for i, p in enumerate(config_stats):
                ws.write(self._row, 0, self._row)
                ws.write(self._row, 1, i+1)
                ws.write(self._row, 2, "Path")
                ws.write(self._row, 3, p)
                self._row += 1

            # Env
            ws.write(self._row, 0, self._row)
            self._row += 1
            [ws.write(self._row, col, hdr) for (col, hdr) in enumerate([self._row, 0, "ENV", "Key", "Value"])]
            self._row += 1
            config_stats = cc_status.get("config", {}).get("os.environ", [])
            for i, ek in enumerate(sorted(config_stats.keys())):
                ev = config_stats[ek]
                ws.write(self._row, 0, self._row)
                ws.write(self._row, 1, i+1)
                ws.write(self._row, 2, "ENV")
                ws.write(self._row, 3, ek)
                ws.write(self._row, 4, ev)
                self._row += 1

            # Config
            ws.write(self._row, 0, self._row)
            self._row += 1
            [ws.write(self._row, col, hdr) for (col, hdr) in enumerate([self._row, 0, "CFG", "Key", "Value", "Type"])]
            self._row += 1
            config_stats = cc_status.get("config", {}).get("CFG", {})

            self.lnum = 1

            def write_cfg_dict(prefix, cfg_dict):
                for ckey in sorted(cfg_dict.keys()):
                    cval = cfg_dict[ckey]
                    if isinstance(cval, dict):
                        write_cfg_dict("%s.%s" % (prefix, ckey), cval)
                    else:
                        ws.write(self._row, 0, self._row)
                        ws.write(self._row, 1, self.lnum+1)
                        ws.write(self._row, 2, "CFG")
                        ws.write(self._row, 3, "%s.%s" % (prefix, ckey))
                        ws.write(self._row, 4, str(cval))
                        ws.write(self._row, 5, type(cval).__name__)
                        self._row += 1
                        self.lnum += 1

            write_cfg_dict("CFG", config_stats)

            # Greenlet
            ws.write(self._row, 0, self._row)
            self._row += 1
            [ws.write(self._row, col, hdr) for (col, hdr) in enumerate([self._row, 0, "Greenlet", "Name"])]
            self._row += 1
            gl_stats = cc_status.get("gevent", {}).get("greenlets", [])
            for i, p in enumerate(gl_stats):
                ws.write(self._row, 0, self._row)
                ws.write(self._row, 1, i+1)
                ws.write(self._row, 2, "Greenlet")
                ws.write(self._row, 3, p[0])
                self._row += 1

            # Gevent Block
            ws.write(self._row, 0, self._row)
            self._row += 1
            [ws.write(self._row, col, hdr) for (col, hdr) in enumerate([self._row, 0, "Gevent Block", "Trace 1", "Trace 2", "Trace 3"])]
            self._row += 1
            gl_block = cc_status.get("gevent_block", {}).get("gevent_block", {})
            for gl_id, msgs in gl_block.items():
                ws.write(self._row, 0, self._row)
                ws.write(self._row, 1, i+1)
                ws.write(self._row, 2, gl_id)
                for index, msg in enumerate(msgs):
                    ws.write(self._row, 3+index, msg)
                self._row += 1

        dtstr = datetime.datetime.today().strftime('%Y%m%d_%H%M%S')
        path = filename or "interface/containers_%s.xls" % dtstr
        self._wb.save(path)

"""
from pyon.container.snapshot import ContainerSnapshot
cs = ContainerSnapshot(cc)
cs.take_snapshot()
cs.persist_snapshot()

from ion.util.datastore.resources import ResourceRegistryHelper
rrh = ResourceRegistryHelper()
rrh.dump_container_stats_as_xlsx()
"""
