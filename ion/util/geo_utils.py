
from pyon.public import  log, IonObject, OT
from pyon.core.exception import BadRequest
from pyproj import Geod


class GeoUtils(object):
    DISTANCE_SHORTEST = "shortest"
    DISTANCE_LONGEST = "longest"

    @classmethod
    def calc_geospatial_point_center(cls, geospatial_bounds=None, distance=None):
        if not geospatial_bounds:
            raise BadRequest ("Geospatial bounds data is not set correctly")
        if (geospatial_bounds.geospatial_latitude_limit_north < -90 or geospatial_bounds.geospatial_latitude_limit_north > 90 or
            geospatial_bounds.geospatial_latitude_limit_south < -90 or geospatial_bounds.geospatial_latitude_limit_south > 90 or
            geospatial_bounds.geospatial_longitude_limit_east < -180 or geospatial_bounds.geospatial_longitude_limit_east > 180 or
            geospatial_bounds.geospatial_longitude_limit_west < -180 or geospatial_bounds.geospatial_longitude_limit_west > 180):
            raise BadRequest ("Geospatial bounds data out of range")
        geo_index_obj = IonObject(OT.GeospatialIndex)

        west_lon = geospatial_bounds.geospatial_longitude_limit_west
        east_lon = geospatial_bounds.geospatial_longitude_limit_east
        north_lat = geospatial_bounds.geospatial_latitude_limit_north
        south_lat = geospatial_bounds.geospatial_latitude_limit_south
        if not distance:
            if (west_lon >= 0 and east_lon >= 0) or (west_lon <= 0 and east_lon <= 0):  # Same hemisphere
                if west_lon < east_lon:  # West is "to the left" of East
                    distance = GeoUtils.DISTANCE_SHORTEST
                else:  # West is "to the right" of East
                    distance = GeoUtils.DISTANCE_LONGEST
            elif west_lon >= 0 and east_lon <= 0:
                if west_lon + abs(east_lon) < 180:
                    distance = GeoUtils.DISTANCE_LONGEST
                else:
                    distance = GeoUtils.DISTANCE_SHORTEST
            elif west_lon <= 0 and east_lon >= 0:
                if abs(west_lon) + east_lon > 180:
                    distance = GeoUtils.DISTANCE_LONGEST
                else:
                    distance = GeoUtils.DISTANCE_SHORTEST
            else:
                distance = GeoUtils.DISTANCE_SHORTEST
        if distance == GeoUtils.DISTANCE_SHORTEST:
            geo_index_obj.lat, geo_index_obj.lon = GeoUtils.midpoint_shortest( north_lat=north_lat, west_lon=west_lon, south_lat=south_lat, east_lon=east_lon)
        elif distance == GeoUtils.DISTANCE_LONGEST:
            geo_index_obj.lat,  geo_index_obj.lon = GeoUtils.midpoint_longest( north_lat=north_lat, west_lon=west_lon, south_lat=south_lat, east_lon=east_lon)
        else:
            raise BadRequest("Distance type not specified")
        return geo_index_obj

    @staticmethod
    def midpoint_shortest(north_lat, west_lon, south_lat, east_lon):
        g = Geod(ellps='clrk66')
        af, ab, dist = g.inv(west_lon, north_lat, east_lon, south_lat)
        rlon, rlat, az = g.fwd(west_lon, north_lat, af, dist/2)
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
        rlon = round(rlon, 6)
        rlat = round(rlat, 6)
        return rlat, rlon

    @staticmethod
    def midpoint_longest(north_lat, west_lon, south_lat, east_lon):
        g = Geod(ellps='clrk66')
        af, ab, dist = g.inv(west_lon, north_lat, east_lon, south_lat)
        rlon, rlat, az = g.fwd(west_lon, north_lat, af, dist/2)
        rlon += 180 if rlon < 0 else -180
        rlon = round(rlon, 6)
        rlat = round(rlat, 6)
        return rlat, rlon