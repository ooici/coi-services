#!/usr/bin/env python

"""
@file ion/util/test/test_resource_lcs_policy.py
@author Ian Katz
@test ion.util.resource_lcs_policy Unit test suite
"""

from unittest.case import SkipTest

from ion.util.geo_utils import GeoUtils
from pyon.public import IonObject, RT, log
from nose.plugins.attrib import attr
from pyon.util.unit_test import PyonTestCase
from pyon.core.exception import BadRequest


@attr('UNIT', group='sa')
class TestGeoUtils(PyonTestCase):

    def test_midpoint(self):
        geospatial_bounds = IonObject("GeospatialBounds")
        geospatial_bounds.geospatial_latitude_limit_north = 10
        geospatial_bounds.geospatial_latitude_limit_south = -10
        geospatial_bounds.geospatial_longitude_limit_east = 10
        geospatial_bounds.geospatial_longitude_limit_west = -10

        # Basic test
        #TODO. do we really want calc_geospatial_point_center() to return string?
        mid_point = GeoUtils.calc_geospatial_point_center(geospatial_bounds)
        lat = float(mid_point.lat)
        lon = float(mid_point.lon)
        self.assertAlmostEqual(lat, 0.0)
        self.assertAlmostEqual(lon, 0.0)

        mid_point = GeoUtils.calc_geospatial_point_center(geospatial_bounds, distance=GeoUtils.DISTANCE_SHORTEST)
        lat = float(mid_point.lat)
        lon = float(mid_point.lon)
        self.assertAlmostEqual(lat, 0.0)
        self.assertAlmostEqual(lon, 0.0)

        mid_point = GeoUtils.calc_geospatial_point_center(geospatial_bounds, distance=GeoUtils.DISTANCE_LONGEST)
        lat = float(mid_point.lat)
        lon = float(mid_point.lon)
        self.assertAlmostEqual(lat, 0.0)
        self.assertAlmostEqual(lon, -180)

        # Bad requests
        with self.assertRaises(BadRequest):
            GeoUtils.calc_geospatial_point_center(geospatial_bounds, distance="spacetime")

        geospatial_bounds.geospatial_latitude_limit_north = 10
        geospatial_bounds.geospatial_latitude_limit_south = -10
        geospatial_bounds.geospatial_longitude_limit_east = 10
        geospatial_bounds.geospatial_longitude_limit_west = -181
        with self.assertRaises(BadRequest):
            GeoUtils.calc_geospatial_point_center(geospatial_bounds)

        geospatial_bounds.geospatial_latitude_limit_north = 10
        geospatial_bounds.geospatial_latitude_limit_south = -10
        geospatial_bounds.geospatial_longitude_limit_east = 181
        geospatial_bounds.geospatial_longitude_limit_west = -10
        with self.assertRaises(BadRequest):
            GeoUtils.calc_geospatial_point_center(geospatial_bounds)

        geospatial_bounds.geospatial_latitude_limit_north = 10
        geospatial_bounds.geospatial_latitude_limit_south = -91
        geospatial_bounds.geospatial_longitude_limit_east = 181
        geospatial_bounds.geospatial_longitude_limit_west = -10
        with self.assertRaises(BadRequest):
            GeoUtils.calc_geospatial_point_center(geospatial_bounds)

        geospatial_bounds.geospatial_latitude_limit_north = 91
        geospatial_bounds.geospatial_latitude_limit_south = -10
        geospatial_bounds.geospatial_longitude_limit_east = 0
        geospatial_bounds.geospatial_longitude_limit_west = -10
        with self.assertRaises(BadRequest):
            GeoUtils.calc_geospatial_point_center(geospatial_bounds)

        geospatial_bounds.geospatial_latitude_limit_north = 10
        geospatial_bounds.geospatial_latitude_limit_south = -10
        geospatial_bounds.geospatial_longitude_limit_east = 179
        geospatial_bounds.geospatial_longitude_limit_west = -179
        mid_point = GeoUtils.calc_geospatial_point_center(geospatial_bounds)
        lat = float(mid_point.lat)
        lon = float(mid_point.lon)
        self.assertAlmostEqual(lat, 0.0)
        self.assertAlmostEqual(lon, -180.0)
        mid_point = GeoUtils.calc_geospatial_point_center(geospatial_bounds, distance=GeoUtils.DISTANCE_LONGEST)
        lat = float(mid_point.lat)
        lon = float(mid_point.lon)
        self.assertAlmostEqual(lat, 0.0)
        self.assertAlmostEqual(lon, 0.0)

        geospatial_bounds.geospatial_latitude_limit_north = 90
        geospatial_bounds.geospatial_latitude_limit_south = -90
        geospatial_bounds.geospatial_longitude_limit_east = 0
        geospatial_bounds.geospatial_longitude_limit_west = 0
        mid_point = GeoUtils.calc_geospatial_point_center(geospatial_bounds)
        lat = float(mid_point.lat)
        lon = float(mid_point.lon)
        self.assertAlmostEqual(lat, 0.0)
        self.assertAlmostEqual(lon, 0.0)

        geospatial_bounds.geospatial_latitude_limit_north = 40
        geospatial_bounds.geospatial_latitude_limit_south = 50
        geospatial_bounds.geospatial_longitude_limit_east = -75
        geospatial_bounds.geospatial_longitude_limit_west = -125
        mid_point = GeoUtils.calc_geospatial_point_center(geospatial_bounds)
        lat = float(mid_point.lat)
        lon = float(mid_point.lon)
        self.assertAlmostEqual(lat, 47.801523, 6)
        self.assertAlmostEqual(lon, -102.328644, 6)
        mid_point = GeoUtils.calc_geospatial_point_center(geospatial_bounds, distance=GeoUtils.DISTANCE_LONGEST)
        lat = float(mid_point.lat)
        lon = float(mid_point.lon)
        self.assertAlmostEqual(lat, 47.801523, 6)
        self.assertAlmostEqual(lon, 77.671356, 6)
