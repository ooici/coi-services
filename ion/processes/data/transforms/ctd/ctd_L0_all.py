'''
@author MManning
@file ion/processes/data/transforms/ctd/ctd_L0_all.py
@description Transforms CTD parsed data into L0 streams
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


class ctd_L0_all(TransformDataProcess):
    """Model for a TransformDataProcess

    """

    # Make the stream definitions of the transform class attributes

    outgoing_stream_conductivity = scalar_point_stream_definition(
        description='Conductivity data from science transform',
        field_name = 'conductivity',
        field_definition = 'http://http://sweet.jpl.nasa.gov/2.2/quanConductivity.owl#Conductivity',
        field_units_code = '', # http://unitsofmeasure.org/ticket/27 Has no Units!
        field_range = [0.1, 40.0]
    )

    outgoing_stream_pressure = scalar_point_stream_definition(
        description='Pressure data from science transform',
        field_name = 'pressure',
        field_definition = 'http://http://sweet.jpl.nasa.gov/2.2/quanPressure.owl#Pressure',
        field_units_code = '', # http://unitsofmeasure.org/ticket/27 Has no Units!
        field_range = [0.1, 40.0]
    )

    outgoing_stream_temperature = scalar_point_stream_definition(
        description='Temperature data from science transform',
        field_name = 'temperature',
        field_definition = 'http://http://sweet.jpl.nasa.gov/2.2/quanTemperature.owl#Temperature', # Does not exist - what to use?
        field_units_code = '', # http://unitsofmeasure.org/ticket/27 Has no Units!
        field_range = [0.1, 40.0]
    )

    incoming_stream_def = ctd_stream_definition()

    def process(self, packet):

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

        log.warn('Got conductivity: %s' % str(conductivity))
        log.warn('Got pressure: %s' % str(pressure))
        log.warn('Got temperature: %s' % str(temperature))

        # do L0 scaling here.....


        # Use the constructor to put data into a granule
        psc_conductivity = PointSupplementConstructor(stream_id="stream_id_conduct", point_definition=self.outgoing_stream_conductivity)

        psc_pressure = PointSupplementConstructor(stream_id="stream_id_pressure", point_definition=self.outgoing_stream_pressure)

        psc_temperature = PointSupplementConstructor(stream_id="stream_id_temperature", point_definition=self.outgoing_stream_temperature)

        for i in xrange(len(conductivity)):
            point_id = psc_conductivity.add_point(time=time[i],location=(longitude[i],latitude[i],pressure[i]))
            psc_conductivity.add_scalar_point_coverage(point_id=point_id, coverage_id='salinity', value=Decimal(conductivity[i]))
        self.conductivity.publish(psc_conductivity.close_stream_granule())

        for i in xrange(len(pressure)):
            point_id = psc_pressure.add_point(time=time[i],location=(longitude[i],latitude[i],pressure[i]))
            psc_pressure.add_scalar_point_coverage(point_id=point_id, coverage_id='salinity', value=Decimal(pressure[i]))
        self.conductivity.publish(psc_pressure.close_stream_granule())

        for i in xrange(len(temperature)):
            point_id = psc_temperature.add_point(time=time[i],location=(longitude[i],latitude[i],pressure[i]))
            psc_temperature.add_scalar_point_coverage(point_id=point_id, coverage_id='salinity', value=Decimal(temperature[i]))
        self.conductivity.publish(psc_temperature.close_stream_granule())

        return






  