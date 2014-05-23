#!/usr/bin/env python

"""Discovery query execution via a queryable datastore"""

__author__ = 'Michael Meisinger'

import calendar
import dateutil
import dateutil.parser
import pprint

from pyon.datastore.datastore import DataStore
from pyon.datastore.datastore_query import DatastoreQueryBuilder, DQ
from pyon.ion.resource import create_access_args
from pyon.public import PRED, CFG, RT, log, BadRequest, get_ion_actor_id, ResourceQuery, EventQuery

from interface.objects import View

DATASTORE_MAP = {"resources_index": DataStore.DS_RESOURCES,
                 "data_products_index": DataStore.DS_RESOURCES,
                 "events_index": DataStore.DS_EVENTS,
                 }

PROFILE_MAP = {DataStore.DS_RESOURCES: DataStore.DS_PROFILE.RESOURCES,
               DataStore.DS_EVENTS: DataStore.DS_PROFILE.EVENTS,
              }

COL_MAP = {"_all": DQ.RA_NAME,
           "name": DQ.RA_NAME,
           "_id": DQ.ATT_ID,
           "type_": DQ.ATT_TYPE,
           "lcstate": DQ.RA_LCSTATE,
           "availability": DQ.RA_AVAILABILITY,
           "ts_created": DQ.RA_TS_CREATED,
           "ts_updated": DQ.RA_TS_UPDATED,
           "origin": DQ.EA_ORIGIN,
           "origin_type": DQ.EA_ORIGIN_TYPE,
           "sub_type": DQ.EA_SUB_TYPE,
           "actor_id": DQ.EA_ACTOR_ID,
           "geospatial_point_center": DQ.RA_GEOM,
           "geospatial_bounds": DQ.RA_GEOM_LOC,
           }


class DatastoreDiscovery(object):
    def __init__(self, process):
        self.process = process
        self.container = self.process.container

        # Query matchers
        self._qmatchers = [self._qmatcher_andor,
                           self._qmatcher_allmatch,
                           self._qmatcher_field_time,
                           self._qmatcher_fieldeq,
                           self._qmatcher_geo_loc,
                           self._qmatcher_geo_wkt,
                           self._qmatcher_geo_vert,
                          ]

    def execute_query(self, discovery_query, id_only=True, query_args=None, query_params=None):
        try:
            if "QUERYEXP" in discovery_query:
                ds_query, ds_name = discovery_query, discovery_query["query_args"].get("datastore", DataStore.DS_RESOURCES)
            else:
                log.info("DatastoreDiscovery.execute_query(): discovery_query=\n%s", pprint.pformat(discovery_query))
                ds_query, ds_name = self._build_ds_query(discovery_query, id_only=id_only)

            current_actor_id=get_ion_actor_id(self.process)
            ds_query.setdefault("query_params", {})
            if query_params:
                ds_query["query_params"].update(query_params)
            ds_query["query_params"]["current_actor"] = current_actor_id

            log.debug("DatastoreDiscovery.execute_query(): ds_query=\n%s", pprint.pformat(ds_query))

            ds = self._get_datastore(ds_name)
            access_args = create_access_args(current_actor_id=current_actor_id,
                                             superuser_actor_ids=self.container.resource_registry.get_superuser_actors())
            query_results = ds.find_by_query(ds_query, access_args=access_args)
            log.info("Datastore discovery query resulted in %s rows", len(query_results))

            if query_args and query_args.get("query_info", False):
                query_info = dict(_query_info=True, query=ds_query, access_args=access_args, ds_name=ds_name)
                query_info.update(ds_query.get("_result", {}))
                query_results.append(query_info)

            return query_results
        except Exception as ex:
            log.exception("DatastoreDiscovery.execute_query() failed")
        return []


    def _build_ds_query(self, discovery_query, id_only=True):
        query_exp = discovery_query["query"] or {}
        index = query_exp.get("index", "resources_index")
        ds_name = DATASTORE_MAP.get(index, None)
        ds_profile = PROFILE_MAP.get(ds_name, None)
        # TODO: Enable service defined indexes in addition to standard indexes
        if ds_name is None:
            raise BadRequest("Unknown index: %s" % index)
        limit = discovery_query.get("limit", 0)
        skip = discovery_query.get("skip", 0)
        order = discovery_query.get("order", None)

        qb = DatastoreQueryBuilder(limit=limit, skip=skip, id_only=id_only, profile=ds_profile)
        where = None
        for qm in self._qmatchers:
            where = qm(discovery_query, qb)
            if where:
                break
        if where is None:
            raise BadRequest("Query had no matcher")

        if index == "data_products_index":
            filter_types = ["DataProduct", "DataProcess", "Deployment", "InstrumentDevice", "InstrumentModel",
                            "InstrumentAgentInstance", "InstrumentAgent", "PlatformDevice", "PlatformModel",
                            "PlatformAgentInstance", "PlatformAgent", "PlatformSite", "Observatory", "UserRole",
                            "Org", "Attachment", "ExternalDatasetAgent", "ExternalDatasetAgentInstance"]
            where = qb.and_(where, qb.in_(DQ.ATT_TYPE, *filter_types), qb.neq(DQ.RA_LCSTATE, "DELETED"))

        order_by = None
        if order:
            order_list = []
            if type(order) is dict:
                for col, colsort in order.iteritems():
                    order_list.append((col, colsort))
            elif type(order) in (list, tuple):
                for column in order:
                    if type(column) in (list, tuple):
                        col, colsort = column
                    else:
                        col, colsort = column, "asc"
                    order_list.append((col, colsort))
            order_by = qb.order_by(order_list)

        qb.build_query(where=where, order_by=order_by)
        return qb.get_query(), ds_name

    def _get_datastore(self, ds_name):
        ds = None
        if ds_name == DataStore.DS_RESOURCES:
            ds = self.container.resource_registry.rr_store
        elif ds_name == DataStore.DS_EVENTS:
            ds = self.container.event_repository.event_store
        return ds

    # -------------------------------------------------------------------------

    def _qmatcher_andor(self, query, qb):
        and_exp = query.get("and", None)
        or_exp = query.get("or", None)
        query_exp = query.get("query", None)
        if not (and_exp or or_exp) or not query_exp:
            return

        q_list = [query_exp] + (and_exp if and_exp else or_exp)
        exp_parts = []
        for q in q_list:
            where = None
            for qm in self._qmatchers:
                where = qm(q, qb)
                if where:
                    break
            if where is None:
                raise BadRequest("Query had no matcher")
            exp_parts.append(where)
        if and_exp:
            res_where = qb.and_(*exp_parts)
        else:
            res_where = qb.or_(*exp_parts)
        return res_where

    def _qmatcher_allmatch(self, query, qb):
        query_exp = query.get("query", query)
        field = query_exp.get("field", None)
        match = query_exp.get("match", None)
        if field != "_all" or match is None:
            return

        return qb.all_match(match)

    def _qmatcher_field_time(self, query, qb):
        query_exp = query.get("query", query)
        field = query_exp.get("field", None)
        time = query_exp.get("time", None) or query_exp.get("time_bounds", None)
        if not (field and time):
            return
        from_time = time.get("from", None)
        to_time = time.get("to", None)
        if not (from_time and to_time):
            return
        from_time_val = calendar.timegm(dateutil.parser.parse(from_time).timetuple())
        to_time_val = calendar.timegm(dateutil.parser.parse(to_time).timetuple())

        range_op = query_exp.get("cmpop", None)
        basic_col = COL_MAP.get(field, None)
        if basic_col == DQ.RA_TS_CREATED or basic_col == DQ.RA_TS_UPDATED:
            from_time_val *= 1000
            to_time_val *= 1000
            lower_val, upper_val = min(from_time_val, to_time_val), max(from_time_val, to_time_val)
            return qb.between(basic_col, str(lower_val), str(upper_val))
        else:
            temp_col = DQ.RA_TEMP_RANGE
            lower_val, upper_val = min(from_time_val, to_time_val), max(from_time_val, to_time_val)

            if range_op == "contains":
                return qb.contains_range(temp_col, lower_val, upper_val)
            elif range_op == "within":
                return qb.within_range(temp_col, lower_val, upper_val)
            else:
                return qb.overlaps_range(temp_col, lower_val, upper_val)

    def _qmatcher_fieldeq(self, query, qb):
        query_exp = query.get("query", query)
        field = query_exp.get("field", None)
        value = query_exp.get("value", None)
        match = query_exp.get("match", None)
        fuzzy = query_exp.get("fuzzy", None)
        range = query_exp.get("range", None)
        if not field:
            return
        if value is None and match is None and fuzzy is None and range is None:
            return

        basic_col = COL_MAP.get(field, None)
        if basic_col:
            if value is not None and "*" in value:
                match = value.replace("*", "%")
                where = qb.like(basic_col, match, case_sensitive=False)
            elif match is not None:
                where = qb.like(basic_col, "%" + str(match) + "%", case_sensitive=False)
            elif value is not None:
                where = qb.eq(basic_col, value)
            elif range is not None:
                rng_from, rng_to = range["from"], range["to"]
                if basic_col == DQ.RA_TS_CREATED:
                    rng_from, rng_to = str(int(rng_from)), str(int(rng_to))
                    where = qb.and_(qb.gte(basic_col, rng_from), qb.lte(basic_col, rng_to))
                else:
                    rng_from, rng_to = int(rng_from), int(rng_to)
                    where = qb.between(basic_col, rng_from, rng_to)
            else:
                where = qb.fuzzy(basic_col, fuzzy)
        elif range:
            rng_from, rng_to = int(range["from"]), int(range["to"])
            where = qb.between(field, rng_from, rng_to)
        else:
            match = match or value
            match = match.replace("*", "%")
            where = qb.attr_like(field, match, case_sensitive=False)

        return where

    def _qmatcher_geo_loc(self, query, qb):
        query_exp = query.get("query", query)
        field = query_exp.get("field", None)
        bottom_right = query_exp.get("bottom_right", None)
        top_left = query_exp.get("top_left", None)
        if not (field and bottom_right and top_left):
            return

        geom_col = COL_MAP.get(field, DQ.RA_GEOM)
        range_op = query_exp.get("cmpop", None)
        if range_op == "contains":
            return qb.contains_bbox(geom_col, top_left[0], bottom_right[1], bottom_right[0], top_left[1])
        elif range_op == "within":
            return qb.within_bbox(geom_col, top_left[0], bottom_right[1], bottom_right[0], top_left[1])
        else:
            return qb.overlaps_bbox(geom_col, top_left[0], bottom_right[1], bottom_right[0], top_left[1])

    def _qmatcher_geo_wkt(self, query, qb):
        query_exp = query.get("query", query)
        field = query_exp.get("field", None)
        wkt = query_exp.get("wkt", None)
        if not (field and wkt):
            return

        geom_col = COL_MAP.get(field, DQ.RA_GEOM)
        buf = query_exp.get("buffer", None)
        range_op = query_exp.get("cmpop", None)
        if range_op == "contains":
            return qb.contains_geom(geom_col, wkt, buf)
        elif range_op == "within":
            return qb.within_geom(geom_col, wkt, buf)
        else:
            return qb.overlaps_geom(geom_col, wkt, buf)

    def _qmatcher_geo_vert(self, query, qb):
        query_exp = query.get("query", query)
        field = query_exp.get("field", None)
        vertical_bounds = query_exp.get("vertical_bounds", None)
        if not (field and vertical_bounds):
            return

        geom_col = DQ.RA_VERT_RANGE

        range_op = query_exp.get("cmpop", None)
        if range_op == "contains":
            return qb.contains_range(geom_col, vertical_bounds.get("from", 0), vertical_bounds.get("to", 0))
        elif range_op == "within":
            return qb.within_range(geom_col, vertical_bounds.get("from", 0), vertical_bounds.get("to", 0))
        else:
            return qb.overlaps_range(geom_col, vertical_bounds.get("from", 0), vertical_bounds.get("to", 0))

    def get_builtin_view(self, view_name):
        view_obj = View(name=view_name)
        if view_name == "resources_index":
            rq = ResourceQuery()
            view_obj.view_definition = rq.get_query()
            return view_obj
        elif view_name == "data_products_index":
            rq = ResourceQuery()
            rq.set_filter(rq.filter_type(["DataProduct", "DataProcess", "Deployment", "InstrumentDevice", "InstrumentModel",
                            "InstrumentAgentInstance", "InstrumentAgent", "PlatformDevice", "PlatformModel",
                            "PlatformAgentInstance", "PlatformAgent", "PlatformSite", "Observatory", "UserRole",
                            "Org", "Attachment", "ExternalDatasetAgent", "ExternalDatasetAgentInstance"]))
            view_obj.view_definition = rq.get_query()
            return view_obj
        elif view_name == "events_index":
            eq = EventQuery()
            view_obj.view_definition = eq.get_query()
            return view_obj
