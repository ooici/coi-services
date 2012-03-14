'''
@author MManning
@file ion/processes/data/transforms/ctd/ctd_L2_density.py
@description Transforms CTD parsed data into L2 product for density
'''

from pyon.ion.transform import TransformFunction
from pyon.service.service import BaseService
from pyon.core.exception import BadRequest
from pyon.public import IonObject, RT, log

#from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient

from prototype.sci_data.stream_defs import SBE37_CDM_stream_definition, L2_density_stream_definition

from prototype.sci_data.stream_parser import PointSupplementStreamParser
from prototype.sci_data.constructor_apis import PointSupplementConstructor

from seawater.gibbs import SP_from_cndr, rho, SA_from_SP
from seawater.gibbs import cte

class DensityTransform(TransformFunction):
    ''' A basic transform that receives input through a subscription,
    parses the input from a CTD, extracts the conductivity, pressure and Temperature value and calculates density
    according to the defined algorithm. If the transform
    has an output_stream it will publish the output on the output stream.

    '''


    # Make the stream definitions of the transform class attributes... best available option I can think of?
    incoming_stream_def = SBE37_CDM_stream_definition()
    outgoing_stream_def = L2_density_stream_definition()



    def execute(self, granule):
        """Processes incoming data!!!!
        """

        # Use the deconstructor to pull data from a granule
        psd = PointSupplementStreamParser(stream_definition=self.incoming_stream_def, stream_granule=granule)


        conductivity = psd.get_values('conductivity')
        pressure = psd.get_values('pressure')
        temperature = psd.get_values('temperature')

        longitude = psd.get_values('longitude')
        latitude = psd.get_values('latitude')
        height = psd.get_values('height')
        time = psd.get_values('time')



        log.warn('Got conductivity: %s' % str(conductivity))
        log.warn('Got pressure: %s' % str(pressure))
        log.warn('Got temperature: %s' % str(temperature))


        sp = SP_from_cndr(r=conductivity/cte.C3515, t=temperature, p=pressure)

        sa = SA_from_SP(sp, pressure, lon, lat)

        dens = rho(sa, temperature, pressure)

        log.warn('Got density: %s' % str(dens))

        # Use the constructor to put data into a granule
        psc = PointSupplementConstructor(point_definition=self.outgoing_stream_def, stream_id=self.streams['output'])
        ### Assumes the config argument for output streams is known and there is only one 'output'.
        ### the stream id is part of the metadata which much go in each stream granule - this is awkward to do at the
        ### application level like this!

        for i in xrange(len(density)):
            point_id = psc.add_point(time=time[i],location=(longitude[i],latitude[i],height[i]))
            psc.add_scalar_point_coverage(point_id=point_id, coverage_id='density', value=density[i])

        return psc.close_stream_granule()

  