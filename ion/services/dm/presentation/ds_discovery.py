#!/usr/bin/env python

"""Discovery query execution via a queryable datastore"""

__author__ = 'Michael Meisinger'

import calendar
import dateutil
import dateutil.parser
import pprint

from pyon.datastore.datastore import DataStore
from pyon.datastore.datastore_query import DatastoreQueryBuilder, DQ
from pyon.public import PRED, CFG, RT, log, BadRequest

DATASTORE_MAP = {"resources_index": DataStore.DS_RESOURCES,
                 "data_products_index": DataStore.DS_RESOURCES,
                 "events_index": DataStore.DS_EVENTS,
                 }

COL_MAP = {"_all": DQ.RA_NAME,
           "name": DQ.RA_NAME,
           "_id": DQ.ATT_ID,
           "type_": DQ.ATT_TYPE,
           "lcstate": DQ.RA_LCSTATE,
           "ts_created": DQ.RA_TS_CREATED,
           "ts_updated": DQ.RA_TS_UPDATED,
           "geospatial_point_center": DQ.RA_GEOM,
           "geospatial_bounds": DQ.RA_GEOM_VERT,
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
                           self._qmatcher_geo_vert,
                          ]

    def execute_query(self, discovery_query, id_only=True):
        try:

            if "QUERYEXP" in discovery_query:
                ds_query, ds_name = discovery_query, discovery_query["query_exp"].get("datastore", DataStore.DS_RESOURCES)
            else:
                log.info("DatastoreDiscovery.execute_query(). discovery_query=\n%s", pprint.pformat(discovery_query))
                ds_query, ds_name = self._build_ds_query(discovery_query, id_only=id_only)
            log.debug("DatastoreDiscovery.execute_query(). ds_query=\n%s", pprint.pformat(ds_query))

            ds = self._get_datastore(ds_name)
            res = ds.find_resources_mult(ds_query)
            log.info("Datastore discovery query resulted in %s rows", len(res))

            return res
        except Exception as ex:
            log.exception("DatastoreDiscovery.execute_query() failed")
        return []

    def _build_ds_query(self, discovery_query, id_only=True):
        query_exp = discovery_query["query"] or {}
        index = query_exp.get("index", "resources_index")
        ds_name = DATASTORE_MAP.get(index, None)
        if ds_name is None:
            raise BadRequest("Unknown index: %s" % index)
        limit = discovery_query.get("limit", 0)

        qb = DatastoreQueryBuilder(limit=limit, id_only=id_only)
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
            where = qb.and_(where, qb.in_(DQ.ATT_TYPE, *filter_types))

        qb.build_query(where=where)
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
        log.info("Query matcher AND/OR")

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
        time = query_exp.get("time", None)
        if not (field and time):
            return
        from_time = time.get("from", None)
        to_time = time.get("to", None)
        if not (from_time and to_time):
            return
        from_time_val = calendar.timegm(dateutil.parser.parse(from_time).timetuple())
        to_time_val = calendar.timegm(dateutil.parser.parse(to_time).timetuple())

        basic_col = COL_MAP.get(field, None)
        if basic_col == DQ.RA_TS_CREATED or basic_col == DQ.RA_TS_UPDATED:
            from_time_val *= 1000
            to_time_val *= 1000
            return qb.between(basic_col, str(min(from_time_val, to_time_val)), str(max(from_time_val, to_time_val)))

        else:
            geom_col = DQ.RA_GEOM_TEMP
            return qb.containedby_bbox(geom_col, min(from_time_val, to_time_val), 0, max(from_time_val, to_time_val), 0)

    def _qmatcher_fieldeq(self, query, qb):
        log.info("In matcher field")
        query_exp = query.get("query", query)
        field = query_exp.get("field", None)
        value = query_exp.get("value", None)
        match = query_exp.get("match", None)
        fuzzy = query_exp.get("fuzzy", None)
        if not field:
            return
        if value is None and match is None and fuzzy is None:
            return

        log.info("Query matcher FIELD")

        basic_col = COL_MAP.get(field, None)
        if basic_col:
            if value is not None and "*" in value:
                match = value.replace("*", "%")
                where = qb.like(basic_col, match)
            elif match is not None:
                where = qb.like(basic_col, "%" + str(match) + "%")
            elif value is not None:
                where = qb.eq(basic_col, value)
            else:
                where = qb.fuzzy(basic_col, fuzzy)
        else:
            match = match or value
            match = match.replace("*", "%")
            where = qb.attr_like(field, match)

        return where

    def _qmatcher_geo_loc(self, query, qb):
        query_exp = query.get("query", query)
        field = query_exp.get("field", None)
        bottom_right = query_exp.get("bottom_right", None)
        top_left = query_exp.get("top_left", None)
        if not (field and bottom_right and top_left):
            return

        geom_col = COL_MAP.get(field, DQ.RA_GEOM)

        return qb.overlaps_bbox(geom_col, top_left[0], top_left[1], bottom_right[0], bottom_right[1])

    def _qmatcher_geo_vert(self, query, qb):
        query_exp = query.get("query", query)
        field = query_exp.get("field", None)
        vertical_bounds = query_exp.get("vertical_bounds", None)
        if not (field and vertical_bounds):
            return

        geom_col = DQ.RA_GEOM_VERT

        return qb.overlaps_bbox(geom_col, vertical_bounds.get("from", 0), 0,  vertical_bounds.get("to", 0), 0)

    # TODO
    # Site containment (simple: name contains)
    # Reference designator (altids?)
    # Organization (simple: name contains)
    # Status?
    # Type Event?

    # Check the data_products_index definition