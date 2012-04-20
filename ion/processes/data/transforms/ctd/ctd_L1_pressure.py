'''
@author MManning
@file ion/processes/data/transforms/ctd/ctd_L1_pressure.py
@description Transforms CTD parsed data into L1 product for pressure
'''

from pyon.ion.transform import TransformFunction, TransformDataProcess
from pyon.service.service import BaseService
from pyon.core.exception import BadRequest
from pyon.public import IonObject, RT, log


from prototype.sci_data.stream_defs import L1_pressure_stream_definition, L0_pressure_stream_definition

from prototype.sci_data.stream_parser import PointSupplementStreamParser
from prototype.sci_data.constructor_apis import PointSupplementConstructor

class CTDL1PressureTransform(TransformFunction):
    ''' A basic transform that receives input through a subscription,
    parses the input from a CTD, extracts the pressure vaule and scales it accroding to
    the defined algorithm. If the transform
    has an output_stream it will publish the output on the output stream.

    '''


    # Make the stream definitions of the transform class attributes... best available option I can think of?
    incoming_stream_def = L0_pressure_stream_definition()
    outgoing_stream_def = L1_pressure_stream_definition()



    def execute(self, granule):
        """Processes incoming data!!!!
        """

        # Use the deconstructor to pull data from a granule
        psd = PointSupplementStreamParser(stream_definition=self.incoming_stream_def, stream_granule=granule)


        pressure = psd.get_values('pressure')

        longitude = psd.get_values('longitude')
        latitude = psd.get_values('latitude')
        height = psd.get_values('height')
        time = psd.get_values('time')

        log.warn('Got pressure: %s' % str(pressure))


        # L1
        # 1) The algorithm input is the L0 pressure data product (p_hex) and, in the case of the SBE 37IM, the pressure range (P_rng) from metadata.
        # 2) Convert the hexadecimal string to a decimal string
        # 3) For the SBE 37IM only, convert the pressure range (P_rng) from psia to dbar SBE 37IM
        #    Convert P_rng (input from metadata) from psia to dbar
        # 4) Perform scaling operation
        #    SBE 37IM
        #    L1 pressure data product (in dbar):


        # Use the constructor to put data into a granule
        psc = PointSupplementConstructor(point_definition=self.outgoing_stream_def, stream_id=self.streams['output'])
        ### Assumes the config argument for output streams is known and there is only one 'output'.
        ### the stream id is part of the metadata which much go in each stream granule - this is awkward to do at the
        ### application level like this!

        for i in xrange(len(pressure)):
            #todo: get pressure range from metadata (if present) and include in calc
            scaled_pressure = ( pressure[i])
            point_id = psc.add_point(time=time[i],location=(longitude[i],latitude[i],height[i]))
            psc.add_scalar_point_coverage(point_id=point_id, coverage_id='pressure', value=scaled_pressure)

        return psc.close_stream_granule()

  