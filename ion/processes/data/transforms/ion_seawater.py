'''
@author David Stuebe
@file ion/services/dm/transformation/example/ion_seawater.py
@description Transforms using the csiro/gibbs seawater toolbox
'''

from pyon.ion.transform import TransformFunction
from pyon.service.service import BaseService
from pyon.core.exception import BadRequest
from pyon.public import IonObject, RT, log

from prototype.sci_data.stream_parser import PointSupplementStreamParser
from prototype.sci_data.constructor_apis import PointSupplementConstructor

from prototype.sci_data.stream_defs import SBE37_CDM_stream_definition, L2_density_stream_definition, L2_practical_salinity_stream_definition

from seawater.gibbs import SP_from_cndr, rho, SA_from_SP
from seawater.gibbs import cte

class SalinityTransform(TransformFunction):
    '''
    L2 Transform for CTD Data.
    Input is conductivity temperature and pressure delivered as a single packet.
    Output is Practical Salinity as calculated by the Gibbs Seawater package
    '''

    outgoing_stream_def = L2_practical_salinity_stream_definition()

    incoming_stream_def = SBE37_CDM_stream_definition()




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


        salinity = SP_from_cndr(r=conductivity/cte.C3515, t=temperature, p=pressure)

        log.warn('Got salinity: %s' % str(salinity))


        # Use the constructor to put data into a granule
        psc = PointSupplementConstructor(point_definition=self.outgoing_stream_def, stream_id=self.streams['output'])

        for i in xrange(len(salinity)):
            point_id = psc.add_point(time=time[i],location=(longitude[i],latitude[i],height[i]))
            psc.add_scalar_point_coverage(point_id=point_id, coverage_id='salinity', value=salinity[i])

        return psc.close_stream_granule()




class DensityTransform(TransformFunction):
    '''
    L2 Transform for CTD Data.
    Input is conductivity temperature and pressure delivered as a single packet.
    Output is Density as calculated by the Gibbs Seawater package
    '''

    outgoing_stream_def = L2_density_stream_definition()

    incoming_stream_def = SBE37_CDM_stream_definition()



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


        dens = rho_CT25(sa, temperature, pressure)

        log.warn('Got density: %s' % str(dens))


        # Use the constructor to put data into a granule
        psc = PointSupplementConstructor(point_definition=self.outgoing_stream_def, stream_id=self.streams['output'])

        for i in xrange(len(dens)):
            point_id = psc.add_point(time=time[i],location=(longitude[i],latitude[i],height[i]))
            psc.add_scalar_point_coverage(point_id=point_id, coverage_id='density', value=dens[i])

        return psc.close_stream_granule()
