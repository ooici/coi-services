'''
@author Stephen Henrie
@description Example Transform to double salinity
'''
from prototype.sci_data.stream_defs import L2_practical_salinity_stream_definition

from pyon.ion.transform import TransformFunction


from prototype.sci_data.stream_parser import PointSupplementStreamParser
from prototype.sci_data.constructor_apis import PointSupplementConstructor


class SalinityDoubler(TransformFunction):

    outgoing_stream_def = L2_practical_salinity_stream_definition()

    incoming_stream_def = L2_practical_salinity_stream_definition()


    def execute(self, granule):
        """
        Example process to double the salinity value
        """

        #  pull data from a granule
        psd = PointSupplementStreamParser(stream_definition=self.incoming_stream_def, stream_granule=granule)

        longitude = psd.get_values('longitude')
        latitude = psd.get_values('latitude')
        height = psd.get_values('height')
        time = psd.get_values('time')

        salinity = psd.get_values('salinity')

        salinity *= 2.0

        print ('Doubled salinity: %s' % str(salinity))


        # Use the constructor to put data into a granule
        psc = PointSupplementConstructor(point_definition=self.outgoing_stream_def, stream_id=self.streams['output'])

        for i in xrange(len(salinity)):
            point_id = psc.add_point(time=time[i],location=(longitude[i],latitude[i],height[i]))
            psc.add_scalar_point_coverage(point_id=point_id, coverage_id='salinity', value=salinity[i])

        return psc.close_stream_granule()
