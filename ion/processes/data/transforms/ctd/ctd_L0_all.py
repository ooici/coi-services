'''
@author MManning
@file ion/processes/data/transforms/ctd/ctd_L0_all.py
@description Transforms CTD parsed data into L0 streams
'''

from pyon.ion.transform import TransformFunction, TransformDataProcess
from pyon.service.service import BaseService
from pyon.core.exception import BadRequest
from pyon.public import IonObject, RT, log

from prototype.sci_data.stream_defs import SBE37_CDM_stream_definition, L0_pressure_stream_definition, L0_temperature_stream_definition, L0_conductivity_stream_definition

from prototype.sci_data.stream_parser import PointSupplementStreamParser
from prototype.sci_data.constructor_apis import PointSupplementConstructor
from prototype.sci_data.stream_defs import ctd_stream_definition


class ctd_L0_all(TransformDataProcess):
    """Model for a TransformDataProcess

    """

    # Make the stream definitions of the transform class attributes
    incoming_stream_def = SBE37_CDM_stream_definition()

    outgoing_stream_pressure = L0_pressure_stream_definition()
    outgoing_stream_temperature = L0_temperature_stream_definition()
    outgoing_stream_conductivity = L0_conductivity_stream_definition()



    # Retrieve the id of the OUTPUT stream from the out Data Product for each of the three output streams
#    stream_ids, _ = self.clients.resource_registry.find_objects(out_data_product_id, PRED.hasStream, None, True)
#
#    log.debug("DataProcessManagementService:create_data_process retrieve out data prod streams: %s", str(stream_ids))
#    if not stream_ids:
#        raise NotFound("No Stream created for output Data Product " + str(out_data_product_id))
#    if len(stream_ids) != 1:
#        raise BadRequest("Data Product should only have ONE stream at this time" + str(out_data_product_id))
#    self.output_stream_dict[name] = stream_ids[0]


    def process(self, packet):

        """Processes incoming data!!!!
        """

        # Use the PointSupplementStreamParser to pull data from a granule
        psd = PointSupplementStreamParser(stream_definition=self.incoming_stream_def, stream_granule=packet)


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

    

        # do L0 scaling here.....


        # Use the constructor to put data into a granule

        psc_conductivity = PointSupplementConstructor(point_definition=self.outgoing_stream_conductivity, stream_id=self.streams['conductivity'])

        psc_pressure = PointSupplementConstructor(point_definition=self.outgoing_stream_pressure, stream_id=self.streams['pressure'])

        psc_temperature = PointSupplementConstructor(point_definition=self.outgoing_stream_temperature, stream_id=self.streams['temperature'])

        ### The stream id is part of the metadata which much go in each stream granule - this is awkward to do at the
        ### application level like this!

        for i in xrange(len(conductivity)):
            point_id = psc_conductivity.add_point(time=time[i],location=(longitude[i],latitude[i],height[i]))
            psc_conductivity.add_scalar_point_coverage(point_id=point_id, coverage_id='conductivity', value=conductivity[i])
        self.conductivity.publish(psc_conductivity.close_stream_granule())

        for i in xrange(len(pressure)):
            point_id = psc_pressure.add_point(time=time[i],location=(longitude[i],latitude[i],height[i]))
            psc_pressure.add_scalar_point_coverage(point_id=point_id, coverage_id='pressure', value=pressure[i])
        self.pressure.publish(psc_pressure.close_stream_granule())

        for i in xrange(len(temperature)):
            point_id = psc_temperature.add_point(time=time[i],location=(longitude[i],latitude[i],height[i]))
            psc_temperature.add_scalar_point_coverage(point_id=point_id, coverage_id='temperature', value=temperature[i])
        self.temperature.publish(psc_temperature.close_stream_granule())

        return




  