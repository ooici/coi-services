
from pyon.public import  log, IonObject, OT
from pyon.core.exception import BadRequest
from pyproj import Geod

from interface.objects import GeospatialBounds, TemporalBounds


class GeoUtils(object):
    DISTANCE_SHORTEST = "shortest"
    DISTANCE_LONGEST = "longest"

    @classmethod
    def calc_geospatial_point_center(cls, geospatial_bounds=None, distance=None):
        if not geospatial_bounds:
            raise BadRequest("Geospatial bounds data is not set correctly")
        if (geospatial_bounds.geospatial_latitude_limit_north < -90 or geospatial_bounds.geospatial_latitude_limit_north > 90 or
            geospatial_bounds.geospatial_latitude_limit_south < -90 or geospatial_bounds.geospatial_latitude_limit_south > 90 or
            geospatial_bounds.geospatial_longitude_limit_east < -180 or geospatial_bounds.geospatial_longitude_limit_east > 180 or
            geospatial_bounds.geospatial_longitude_limit_west < -180 or geospatial_bounds.geospatial_longitude_limit_west > 180):
            raise BadRequest("Geospatial bounds data out of range")
        geo_index_obj = IonObject(OT.GeospatialIndex)

        west_lon = geospatial_bounds.geospatial_longitude_limit_west
        east_lon = geospatial_bounds.geospatial_longitude_limit_east
        north_lat = geospatial_bounds.geospatial_latitude_limit_north
        south_lat = geospatial_bounds.geospatial_latitude_limit_south
        if not distance:
            if (west_lon >= 0 and east_lon >= 0) or (west_lon <= 0 and east_lon <= 0):  # Same hemisphere
                if west_lon <= east_lon:  # West is "to the left" of East
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
            geo_index_obj.lat, geo_index_obj.lon = GeoUtils.midpoint_shortest(north_lat=north_lat, west_lon=west_lon, south_lat=south_lat, east_lon=east_lon)
        elif distance == GeoUtils.DISTANCE_LONGEST:
            geo_index_obj.lat,  geo_index_obj.lon = GeoUtils.midpoint_longest(north_lat=north_lat, west_lon=west_lon, south_lat=south_lat, east_lon=east_lon)
        else:
            raise BadRequest("Distance type not specified")
        return geo_index_obj

    @staticmethod
    def midpoint_shortest(north_lat, west_lon, south_lat, east_lon):
        g = Geod(ellps='WGS84')
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
        g = Geod(ellps='WGS84')
        af, ab, dist = g.inv(west_lon, north_lat, east_lon, south_lat)
        rlon, rlat, az = g.fwd(west_lon, north_lat, af, dist/2)
        rlon += 180 if rlon < 0 else -180
        rlon = round(rlon, 6)
        rlat = round(rlat, 6)
        return rlat, rlon

    @staticmethod
    def calc_geo_bounds_for_geo_bounds_list(geo_bounds_list):
        key_mapping = dict(lat_north="geospatial_latitude_limit_north",
                           lat_south="geospatial_latitude_limit_south",
                           lon_east="geospatial_longitude_limit_east",
                           lon_west="geospatial_longitude_limit_west",
                           depth_min="geospatial_vertical_min",
                           depth_max="geospatial_vertical_max")

        gb_box = GeoUtils.calc_bounding_box_for_boxes([gb.__dict__ for gb in geo_bounds_list if gb],
                                                      key_mapping=key_mapping, map_output=True)

        geo_bounds = GeospatialBounds(**gb_box)
        return geo_bounds

    @staticmethod
    def calc_bounding_box_for_boxes(obj_list, key_mapping=None, map_output=False):
        """Calculates the geospatial bounding box for a list of geospatial bounding boxes.
        The input list is represented as a list of dicts.
        This function assumes that bounding boxes do not span poles or datelines."""
        lat_north_key = key_mapping['lat_north'] if key_mapping and 'lat_north' in key_mapping else 'lat_north'
        lat_south_key = key_mapping['lat_south'] if key_mapping and 'lat_south' in key_mapping else 'lat_south'
        lon_east_key = key_mapping['lon_east'] if key_mapping and 'lon_east' in key_mapping else 'lon_east'
        lon_west_key = key_mapping['lon_west'] if key_mapping and 'lon_west' in key_mapping else 'lon_west'
        depth_min_key = key_mapping['depth_min'] if key_mapping and 'depth_min' in key_mapping else 'depth_min'
        depth_max_key = key_mapping['depth_max'] if key_mapping and 'depth_max' in key_mapping else 'depth_max'

        lat_north_list = [float(o[lat_north_key]) for o in obj_list if lat_north_key in o and o[lat_north_key]]
        lat_south_list = [float(o[lat_south_key]) for o in obj_list if lat_south_key in o and o[lat_south_key]]
        lon_east_list = [float(o[lon_east_key]) for o in obj_list if lon_east_key in o and o[lon_east_key]]
        lon_west_list = [float(o[lon_west_key]) for o in obj_list if lon_west_key in o and o[lon_west_key]]
        depth_min_list = [float(o[depth_min_key]) for o in obj_list if depth_min_key in o and o[depth_min_key]]
        depth_max_list = [float(o[depth_max_key]) for o in obj_list if depth_max_key in o and o[depth_max_key]]

        res_bb = {}
        res_bb[lat_north_key if map_output else 'lat_north'] = max(lat_north_list) if lat_north_list else 0.0
        res_bb[lat_south_key if map_output else 'lat_south'] = min(lat_south_list) if lat_south_list else 0.0

        res_bb[lon_east_key if map_output else 'lon_west'] = min(lon_west_list) if lon_west_list else 0.0
        res_bb[lon_west_key if map_output else 'lon_east'] = max(lon_east_list) if lon_east_list else 0.0

        res_bb[depth_min_key if map_output else 'depth_max'] = max(depth_min_list) if depth_min_list else 0.0
        res_bb[depth_max_key if map_output else 'depth_min'] = min(depth_max_list) if depth_max_list else 0.0

        return res_bb

    @staticmethod
    def calc_bounding_box_for_points(obj_list, key_mapping=None):
        """Calculates the geospatial bounding box for a list of geospatial points.
        The input list is represented as a list of dicts.
        This function assumes that bounding boxes do not span poles or datelines."""
        lat_key = key_mapping['latitude'] if key_mapping and 'latitude' in key_mapping else 'latitude'
        lon_key = key_mapping['longitude'] if key_mapping and 'longitude' in key_mapping else 'longitude'
        depth_key = key_mapping['depth'] if key_mapping and 'depth' in key_mapping else 'depth'

        lat_list = [float(o[lat_key]) for o in obj_list if lat_key in o and o[lat_key]]
        lon_list = [float(o[lon_key]) for o in obj_list if lon_key in o and o[lon_key]]
        depth_list = [float(o[depth_key]) for o in obj_list if depth_key in o and o[depth_key]]

        res_bb = {}
        res_bb['lat_north'] = max(lat_list) if lat_list else 0.0
        res_bb['lat_south'] = min(lat_list) if lat_list else 0.0

        res_bb['lon_east'] = max(lon_list) if lon_list else 0.0
        res_bb['lon_west'] = min(lon_list) if lon_list else 0.0

        res_bb['depth_max'] = max(depth_list) if depth_list else 0.0
        res_bb['depth_min'] = min(depth_list) if depth_list else 0.0

        return res_bb

    @staticmethod
    def calc_temp_bounds_for_temp_bounds_list(temp_bounds_list):
        startval_list = [float(tb.start_datetime) for tb in temp_bounds_list if tb and tb.start_datetime]
        start_min = str(int(min(startval_list))) if startval_list else ""
        endval_list = [float(tb.end_datetime) for tb in temp_bounds_list if tb and tb.end_datetime]
        end_max = str(int(max(endval_list))) if endval_list else ""
        temp_bounds = TemporalBounds(start_datetime=start_min, end_datetime=end_max)
        return temp_bounds
