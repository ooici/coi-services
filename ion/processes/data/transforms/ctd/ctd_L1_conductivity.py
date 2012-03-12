'''
@author MManning
@file ion/processes/data/transforms/ctd/ctd_L1_conductivity.py
@description Transforms CTD parsed data into L1 product for conductivity
'''

from pyon.ion.transform import TransformFunction, TransformDataProcess
from pyon.service.service import BaseService
from pyon.core.exception import BadRequest
from pyon.public import IonObject, RT, log

from prototype.sci_data.stream_defs import L1_conductivity_stream_definition, L0_conductivity_stream_definition

from prototype.sci_data.stream_parser import PointSupplementStreamParser
from prototype.sci_data.constructor_apis import PointSupplementConstructor


class CTDL1ConductivityTransform(TransformFunction):
    ''' A basic transform that receives input through a subscription,
    parses the input from a CTD, extracts the conductivity value and scales it according to
    the defined algorithm. If the transform
    has an output_stream it will publish the output on the output stream.

    '''


    # Make the stream definitions of the transform class attributes... best available option I can think of?
    incoming_stream_def = L0_conductivity_stream_definition()
    outgoing_stream_def = L1_conductivity_stream_definition()




    def execute(self, granule):
        """Processes incoming data!!!!
        """
        # Use the deconstructor to pull data from a granule
        psd = PointSupplementStreamParser(stream_definition=self.incoming_stream_def, stream_granule=granule)

        conductivity = psd.get_values('conductivity')

        longitude = psd.get_values('longitude')
        latitude = psd.get_values('latitude')
        height = psd.get_values('height')
        time = psd.get_values('time')

        log.warn('Got conductivity: %s' % str(conductivity))


        # The L1 conductivity data product algorithm takes the L0 conductivity data product and converts it
        # into Siemens per meter (S/m)
        #    SBE 37IM Output Format 0
        #    1) Standard conversion from 5-character hex string to decimal
        #    2)Scaling
        # Use the constructor to put data into a granule
        psc = PointSupplementConstructor(point_definition=self.outgoing_stream_def, stream_id=self.streams['output'])
        ### Assumes the config argument for output streams is known and there is only one 'output'.
        ### the stream id is part of the metadata which much go in each stream granule - this is awkward to do at the
        ### application level like this!

        for i in xrange(len(conductivity)):
            scaled_conductivity =  ( conductivity[i] / 100000.0 ) - 0.5
            point_id = psc.add_point(time=time[i],location=(longitude[i],latitude[i],height[i]))
            psc.add_scalar_point_coverage(point_id=point_id, coverage_id='conductivity', value=scaled_conductivity)

        return psc.close_stream_granule()

  