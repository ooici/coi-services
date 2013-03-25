
from pyon.public import  log, IonObject, OT
from pyon.core.exception import BadRequest
from pyproj import Geod


class GeoUtils(object):
    DISTANCE_SHORTEST = "shortest"
    DISTANCE_LONGEST = "longest"

    @classmethod
    def calc_geospatial_point_center(cls, geospatial_bounds=None, distance="shortest"):
        if not geospatial_bounds or (distance != GeoUtils.DISTANCE_SHORTEST and distance != GeoUtils.DISTANCE_LONGEST):
            raise BadRequest ("Geospatial bounds data is not set correctly")
        if (geospatial_bounds.geospatial_latitude_limit_north < -90 or geospatial_bounds.geospatial_latitude_limit_north > 90 or
            geospatial_bounds.geospatial_latitude_limit_south < -90 or geospatial_bounds.geospatial_latitude_limit_south > 90 or
            geospatial_bounds.geospatial_longitude_limit_east < -180 or geospatial_bounds.geospatial_longitude_limit_east > 180 or
            geospatial_bounds.geospatial_longitude_limit_west < -180 or geospatial_bounds.geospatial_longitude_limit_west > 180):
            raise BadRequest ("Geospatial bounds data out of range")

        geo_index_obj = IonObject(OT.GeospatialIndex)
        if distance == GeoUtils.DISTANCE_SHORTEST:
            geo_index_obj.lat, geo_index_obj.lon = GeoUtils.midpoint_shortest(geospatial_bounds.geospatial_latitude_limit_north,
                                                         geospatial_bounds.geospatial_longitude_limit_west,
                                                         geospatial_bounds.geospatial_latitude_limit_south,
                                                         geospatial_bounds.geospatial_longitude_limit_east)
        elif distance == GeoUtils.DISTANCE_LONGEST:
            geo_index_obj.lat,  geo_index_obj.lon = GeoUtils.midpoint_longest(geospatial_bounds.geospatial_latitude_limit_north,
                                                          geospatial_bounds.geospatial_longitude_limit_west,
                                                          geospatial_bounds.geospatial_latitude_limit_south,
                                                          geospatial_bounds.geospatial_longitude_limit_east)
        else:
            raise BadRequest("Distance type not specified")
        return geo_index_obj

    @staticmethod
    def midpoint_shortest(lat1, lon1, lat2, lon2):
        g = Geod(ellps='clrk66')
        af, ab, dist = g.inv(lon1, lat1, lon2, lat2)
        rlon, rlat, az = g.fwd(lon1, lat1, af, dist/2)
        # decimal places   degrees      distance
        #        0         1            111   km
        #        1         0.1          11.1  km
        #        2         0.01         1.11  km
        #        3         0.001        111   m
        #        4         0.0001       11.1  m
        #        5         0.00001      1.11  m
        #        6         0.000001     0.111 m
        #        7         0.0000001    1.11  cm
        #        8         0.00000001   1.11  mm
        rlon = "%.6f" % rlon
        rlat = "%.6f" % rlat
        return rlat, rlon

    @staticmethod
    def midpoint_longest(lat1, lon1, lat2, lon2):
        g = Geod(ellps='clrk66')
        af, ab, dist = g.inv(lon1, lat1, lon2, lat2)
        rlon, rlat, az = g.fwd(lon1, lat1, af, dist/2)
        rlon += 180 if rlon < 0 else -180
        rlon = "%.6f" % rlon
        rlat = "%.6f" % rlat
        return rlat, rlon