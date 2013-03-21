
from pyon.public import  log, IonObject, OT

class GeoUtils(object):

    @classmethod
    def calc_geospatial_point_center(cls, geospatial_bounds=None):
        geo_index_obj = IonObject(OT.GeospatialIndex)
        if geospatial_bounds :
            geo_index_obj.lat = (geospatial_bounds.geospatial_latitude_limit_north + geospatial_bounds.geospatial_latitude_limit_south) / 2
            geo_index_obj.lon =  (geospatial_bounds.geospatial_longitude_limit_east + geospatial_bounds.geospatial_longitude_limit_west) / 2
        return geo_index_obj
