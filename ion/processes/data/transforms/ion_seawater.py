'''
@author David Stuebe
@file ion/services/dm/transformation/example/ion_seawater.py
@description Transforms using the csiro/gibbs seawater toolbox
'''

from pyon.ion.transform import TransformFunction
from pyon.service.service import BaseService
from pyon.core.exception import BadRequest
from pyon.public import IonObject, RT, log

#from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient

from prototype.sci_data.ctd_stream import scalar_point_stream_definition, ctd_stream_definition

from prototype.sci_data.stream_parser import PointSupplementStreamParser
from prototype.sci_data.constructor_apis import PointSupplementConstructor

from seawater.gibbs import SP_from_cndr, rho, SA_from_SP
from seawater.gibbs import cte

class SalinityTransform(TransformFunction):
    '''
    L2 Transform for CTD Data.
    Input is conductivity temperature and pressure delivered as a single packet.
    Output is Practical Salinity as calculated by the Gibbs Seawater package
    '''

    outgoing_stream_def = scalar_point_stream_definition(
        description='Practical Salinity Scale data from science transform',
        field_name = 'salinity',
        field_definition = 'http://sweet.jpl.nasa.gov/2.0/physThermo.owl#Salinity', # Does not exist - what to use?
        field_units_code = '', # http://unitsofmeasure.org/ticket/27 Has no Units!
        field_range = [0.1, 40.0]
    )

    incoming_stream_def = ctd_stream_definition()




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
        time = psd.get_values('time')



        log.warn('Got conductivity: %s' % str(conductivity))
        log.warn('Got pressure: %s' % str(pressure))
        log.warn('Got temperature: %s' % str(temperature))


        salinity = SP_from_cndr(r=conductivity/cte.C3515, t=temperature, p=pressure)

        log.warn('Got salinity: %s' % str(salinity))


        # Use the constructor to put data into a granule
        psc = PointSupplementConstructor(point_definition=self.outgoing_stream_def, stream_id=self.streams['output'])

        for i in xrange(len(salinity)):
            point_id = psc.add_point(time=time[i],location=(longitude[i],latitude[i],pressure[i]))
            psc.add_scalar_point_coverage(point_id=point_id, coverage_id='salinity', value=salinity[i])

        return psc.close_stream_granule()




class DensityTransform(TransformFunction):
    '''
    L2 Transform for CTD Data.
    Input is conductivity temperature and pressure delivered as a single packet.
    Output is Density as calculated by the Gibbs Seawater package
    '''

    outgoing_stream_def = scalar_point_stream_definition(
        description='Absolute Density data from science transform',
        field_name = 'density',
        field_definition = 'http://sweet.jpl.nasa.gov/2.0/physThermo.owl#Density', # Does not exist - what to use?
        field_units_code = 'kg/m3',
        field_range = [0.1, 40.0]
    )


    incoming_stream_def = ctd_stream_definition()




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
            point_id = psc.add_point(time=time[i],location=(longitude[i],latitude[i],pressure[i]))
            psc.add_scalar_point_coverage(point_id=point_id, coverage_id='density', value=dens[i])

        return psc.close_stream_granule()
