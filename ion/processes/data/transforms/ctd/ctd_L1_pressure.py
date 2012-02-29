'''
@author MManning
@file ion/processes/data/transforms/ctd/ctd_L1_pressure.py
@description Transforms CTD parsed data into L1 product for pressure
'''

from pyon.ion.transform import TransformFunction
from pyon.service.service import BaseService
from pyon.core.exception import BadRequest
from pyon.public import IonObject, RT, log
from decimal import *

#from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient

from prototype.sci_data.ctd_stream import scalar_point_stream_definition, ctd_stream_definition

from prototype.sci_data.deconstructor_apis import PointSupplementDeconstructor
from prototype.sci_data.constructor_apis import PointSupplementConstructor

from seawater.gibbs import SP_from_cndr
from seawater.gibbs import cte

class CTDL1PressureTransform(TransformFunction):
    ''' A basic transform that receives input through a subscription,
    parses the input from a CTD, extracts the pressure vaule and scales it accroding to
    the defined algorithm. If the transform
    has an output_stream it will publish the output on the output stream.

    '''


    # Make the stream definitions of the transform class attributes... best available option I can think of?
    outgoing_stream_def = scalar_point_stream_definition(
        description='L1 Pressure Scale data from science transform',
        field_name = 'L1_pressure',
        field_definition = 'http://http://sweet.jpl.nasa.gov/2.2/quanPressure.owl#Pressure',
        field_units_code = '', # http://unitsofmeasure.org/ticket/27 Has no Units!
        field_range = [0.1, 40.0]
    )

    incoming_stream_def = ctd_stream_definition()




    def execute(self, granule):
        """Processes incoming data!!!!
        """

        # Use the deconstructor to pull data from a granule
        psd = PointSupplementDeconstructor(stream_definition=self.incoming_stream_def, stream_granule=granule)


        conductivity = psd.get_values('conductivity')
        pressure = psd.get_values('pressure')
        temperature = psd.get_values('temperature')

        longitude = psd.get_values('longitude')
        latitude = psd.get_values('latitude')
        time = psd.get_values('time')

        log.warn('Got pressure: %s' % str(pressure))


        # L1
        # 1) The algorithm input is the L0 pressure data product (p_hex) and, in the case of the SBE 37IM, the pressure range (P_rng) from metadata.
        # 2) Convert the hexadecimal string to a decimal string
        # 3) For the SBE 37IM only, convert the pressure range (P_rng) from psia to dbar SBE 37IM
        #    Convert P_rng (input from metadata) from psia to dbar
        #    P_rng (dbar) = 0.6894757 * [P_rng (psia) – 14.7)]
        # 4) Perform scaling operation
        #    SBE 37IM
        #    L1 pressure data product (in dbar):
        #    P_L1 = [p_dec * P_rng / (0.85 * 65536)] – (0.05* P_rng)

        # Use the constructor to put data into a granule
        psc = PointSupplementConstructor(point_definition=self.outgoing_stream_def)

        for i in xrange(len(pressure)):
            #todo: get pressure range from metadata (if present) and include in calc
            scaled_pressure = ( Decimal(pressure[i]))
            point_id = psc.add_point(time=time[i],location=(longitude[i],latitude[i],pressure[i]))
            psc.add_scalar_point_coverage(point_id=point_id, coverage_id='pressure', value=scaled_pressure)

        return psc.close_stream_granule()

  