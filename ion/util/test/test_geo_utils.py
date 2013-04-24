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
        self.assertAlmostEqual(mid_point.lat, 0.0)
        self.assertAlmostEqual(mid_point.lon, 0.0)

        mid_point = GeoUtils.calc_geospatial_point_center(geospatial_bounds, distance=GeoUtils.DISTANCE_SHORTEST)
        self.assertAlmostEqual(mid_point.lat, 0.0)
        self.assertAlmostEqual(mid_point.lon, 0.0)

        mid_point = GeoUtils.calc_geospatial_point_center(geospatial_bounds, distance=GeoUtils.DISTANCE_LONGEST)
        self.assertAlmostEqual(mid_point.lat, 0.0)
        self.assertAlmostEqual(mid_point.lon, -180)


        geospatial_bounds.geospatial_latitude_limit_north = 10
        geospatial_bounds.geospatial_latitude_limit_south = -10
        geospatial_bounds.geospatial_longitude_limit_east = 179
        geospatial_bounds.geospatial_longitude_limit_west = -179

        mid_point = GeoUtils.calc_geospatial_point_center(geospatial_bounds)
        self.assertAlmostEqual(mid_point.lat, 0.0)
        self.assertAlmostEqual(mid_point.lon, 0.0)
        mid_point = GeoUtils.calc_geospatial_point_center(geospatial_bounds, distance=GeoUtils.DISTANCE_SHORTEST)
        self.assertAlmostEqual(mid_point.lat, 0.0)
        self.assertAlmostEqual(mid_point.lon, -180.0)
        mid_point = GeoUtils.calc_geospatial_point_center(geospatial_bounds, distance=GeoUtils.DISTANCE_LONGEST)
        self.assertAlmostEqual(mid_point.lat, 0.0)
        self.assertAlmostEqual(mid_point.lon, 0.0)
        geospatial_bounds.geospatial_longitude_limit_east = -179
        geospatial_bounds.geospatial_longitude_limit_west = 179
        mid_point = GeoUtils.calc_geospatial_point_center(geospatial_bounds)
        self.assertAlmostEqual(mid_point.lon, -180.0)


        geospatial_bounds.geospatial_latitude_limit_north = 90
        geospatial_bounds.geospatial_latitude_limit_south = -90
        geospatial_bounds.geospatial_longitude_limit_east = 0
        geospatial_bounds.geospatial_longitude_limit_west = 0
        mid_point = GeoUtils.calc_geospatial_point_center(geospatial_bounds)
        self.assertAlmostEqual(mid_point.lat, 0.0)
        self.assertAlmostEqual(mid_point.lon, 0.0)
        # MM: changed this: if the same values are given, we expect a point not the full globe
        #self.assertAlmostEqual(mid_point.lon, -180.0)

        geospatial_bounds.geospatial_latitude_limit_north = 40
        geospatial_bounds.geospatial_latitude_limit_south = 50
        geospatial_bounds.geospatial_longitude_limit_east = -75
        geospatial_bounds.geospatial_longitude_limit_west = -125
        mid_point = GeoUtils.calc_geospatial_point_center(geospatial_bounds)
        self.assertAlmostEqual(mid_point.lat, 47.801397, 6)
        self.assertAlmostEqual(mid_point.lon, -102.328727, 6)
        mid_point = GeoUtils.calc_geospatial_point_center(geospatial_bounds, distance=GeoUtils.DISTANCE_SHORTEST)
        self.assertAlmostEqual(mid_point.lat, 47.801397, 6)
        self.assertAlmostEqual(mid_point.lon, -102.328727, 6)

        mid_point = GeoUtils.calc_geospatial_point_center(geospatial_bounds, distance=GeoUtils.DISTANCE_LONGEST)
        self.assertAlmostEqual(mid_point.lat, 47.801397, 6)
        self.assertAlmostEqual(mid_point.lon, 77.671273, 6)

        geospatial_bounds.geospatial_longitude_limit_west = 165
        geospatial_bounds.geospatial_latitude_limit_north = 5
        geospatial_bounds.geospatial_longitude_limit_east = -170
        geospatial_bounds.geospatial_latitude_limit_south = 5
        mid_point = GeoUtils.calc_geospatial_point_center(geospatial_bounds)
        self.assertAlmostEqual(mid_point.lat, 5.121583, 6)
        self.assertAlmostEqual(mid_point.lon, 177.5, 6)

        geospatial_bounds.geospatial_longitude_limit_west = 65
        geospatial_bounds.geospatial_latitude_limit_north = 0
        geospatial_bounds.geospatial_longitude_limit_east = 165
        geospatial_bounds.geospatial_latitude_limit_south = 0
        mid_point = GeoUtils.calc_geospatial_point_center(geospatial_bounds)
        self.assertAlmostEqual(mid_point.lat, 0.0, 6)
        self.assertAlmostEqual(mid_point.lon, 115.0, 6)

        geospatial_bounds.geospatial_longitude_limit_west = 10.0
        geospatial_bounds.geospatial_latitude_limit_north = 0
        geospatial_bounds.geospatial_longitude_limit_east = -150
        geospatial_bounds.geospatial_latitude_limit_south = 0
        mid_point = GeoUtils.calc_geospatial_point_center(geospatial_bounds)
        self.assertAlmostEqual(mid_point.lat, 0.0, 6)
        self.assertAlmostEqual(mid_point.lon, 110.0, 6)

        geospatial_bounds.geospatial_longitude_limit_west = -150
        geospatial_bounds.geospatial_latitude_limit_north = 0
        geospatial_bounds.geospatial_longitude_limit_east = 170
        geospatial_bounds.geospatial_latitude_limit_south = 0
        mid_point = GeoUtils.calc_geospatial_point_center(geospatial_bounds)
        self.assertAlmostEqual(mid_point.lat, 0.0, 6)
        self.assertAlmostEqual(mid_point.lon, 10.0, 6)

        geospatial_bounds.geospatial_longitude_limit_west = 30
        geospatial_bounds.geospatial_latitude_limit_north = 0
        geospatial_bounds.geospatial_longitude_limit_east = 10
        geospatial_bounds.geospatial_latitude_limit_south = 0
        mid_point = GeoUtils.calc_geospatial_point_center(geospatial_bounds)
        self.assertAlmostEqual(mid_point.lat, 0.0, 6)
        self.assertAlmostEqual(mid_point.lon, -160.0, 6)

        geospatial_bounds.geospatial_longitude_limit_west = 10
        geospatial_bounds.geospatial_latitude_limit_north = 0
        geospatial_bounds.geospatial_longitude_limit_east = 50
        geospatial_bounds.geospatial_latitude_limit_south = 0
        mid_point = GeoUtils.calc_geospatial_point_center(geospatial_bounds)
        self.assertAlmostEqual(mid_point.lat, 0.0, 6)
        self.assertAlmostEqual(mid_point.lon, 30.0, 6)

        geospatial_bounds.geospatial_longitude_limit_west = -170
        geospatial_bounds.geospatial_latitude_limit_north = 0
        geospatial_bounds.geospatial_longitude_limit_east = -10
        geospatial_bounds.geospatial_latitude_limit_south = 0
        mid_point = GeoUtils.calc_geospatial_point_center(geospatial_bounds)
        self.assertAlmostEqual(mid_point.lat, 0.0, 6)
        self.assertAlmostEqual(mid_point.lon, -90.0, 6)

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