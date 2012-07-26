'''
@author Stephen Henrie
@description Example Transform to double salinity
'''
from prototype.sci_data.stream_defs import L2_practical_salinity_stream_definition

from pyon.ion.transform import TransformFunction


from prototype.sci_data.stream_parser import PointSupplementStreamParser
from prototype.sci_data.constructor_apis import PointSupplementConstructor

### For new granule and stream interface
from ion.services.dm.utility.granule.record_dictionary import RecordDictionaryTool
from ion.services.dm.utility.granule.taxonomy import TaxyTool
from ion.services.dm.utility.granule.granule import build_granule
from pyon.util.containers import get_safe

class SalinityDoubler(TransformFunction):

    outgoing_stream_def = L2_practical_salinity_stream_definition()

    incoming_stream_def = L2_practical_salinity_stream_definition()

    ### Taxonomies are defined before hand out of band... somehow.
    tx = TaxyTool()
    tx.add_taxonomy_set('salinity','long name for salinity')
    tx.add_taxonomy_set('lat','long name for latitude')
    tx.add_taxonomy_set('lon','long name for longitude')
    tx.add_taxonomy_set('height','long name for height')
    tx.add_taxonomy_set('time','long name for time')
    # This is an example of using groups it is not a normative statement about how to use groups
    tx.add_taxonomy_set('coordinates','This group contains coordinates...')
    tx.add_taxonomy_set('data','This group contains data...')

    def execute(self, granule):
        """
        Example process to double the salinity value
        """
        # Use the PointSupplementStreamParser to pull data from a granule
        #psd = PointSupplementStreamParser(stream_definition=self.incoming_stream_def, stream_granule=packet)
        rdt = RecordDictionaryTool.load_from_granule(granule)

        salinity = get_safe(rdt, 'salinity')

        longitude = get_safe(rdt, 'lon')
        latitude = get_safe(rdt, 'lat')
        time = get_safe(rdt, 'time')
        height = get_safe(rdt, 'height')
#        #  pull data from a granule
#        psd = PointSupplementStreamParser(stream_definition=self.incoming_stream_def, stream_granule=granule)
#
#        longitude = psd.get_values('longitude')
#        latitude = psd.get_values('latitude')
#        height = psd.get_values('height')
#        time = psd.get_values('time')

#        salinity = psd.get_values('salinity')

        salinity *= 2.0

        print ('Doubled salinity: %s' % str(salinity))


        # Use the constructor to put data into a granule
#        psc = PointSupplementConstructor(point_definition=self.outgoing_stream_def, stream_id=self.streams['output'])
#
#        for i in xrange(len(salinity)):
#            point_id = psc.add_point(time=time[i],location=(longitude[i],latitude[i],height[i]))
#            psc.add_scalar_point_coverage(point_id=point_id, coverage_id='salinity', value=salinity[i])
#
#        return psc.close_stream_granule()
        root_rdt = RecordDictionaryTool(taxonomy=self.tx)

        #data_rdt = RecordDictionaryTool(taxonomy=self.tx)
        #coord_rdt = RecordDictionaryTool(taxonomy=self.tx)

        root_rdt['salinity'] = salinity
        root_rdt['time'] = time
        root_rdt['lat'] = latitude
        root_rdt['lon'] = longitude
        root_rdt['height'] = height

        #root_rdt['coordinates'] = coord_rdt
        #root_rdt['data'] = data_rdt

        return build_granule(data_producer_id='ctd_L2_salinity', taxonomy=self.tx, record_dictionary=root_rdt)