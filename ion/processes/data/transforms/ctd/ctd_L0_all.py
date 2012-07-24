'''
@author MManning
@file ion/processes/data/transforms/ctd/ctd_L0_all.py
@description Transforms CTD parsed data into L0 streams
'''

from pyon.ion.transform import TransformFunction, TransformDataProcess
from pyon.service.service import BaseService
from pyon.core.exception import BadRequest
from pyon.public import IonObject, RT, log
from pyon.util.containers import get_safe

from prototype.sci_data.stream_defs import SBE37_CDM_stream_definition, L0_pressure_stream_definition, L0_temperature_stream_definition, L0_conductivity_stream_definition

from prototype.sci_data.stream_parser import PointSupplementStreamParser
#from prototype.sci_data.constructor_apis import PointSupplementConstructor
#from prototype.sci_data.stream_defs import ctd_stream_definition

### For new granule and stream interface
from ion.services.dm.utility.granule.record_dictionary import RecordDictionaryTool
from ion.services.dm.utility.granule.taxonomy import TaxyTool
from ion.services.dm.utility.granule.granule import build_granule

#from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
#pmsc = PubsubManagementServiceClient(node=cc.node)
#c_stream_id = pmsc.create_stream(name='conductivity')
#t_stream_id = pmsc.create_stream(name='temperature')
#p_stream_id = pmsc.create_stream(name='pressure')
#cc.spawn_process('l0_transform', 'ion.processes.data.transforms.ctd_L0_all','ctd_L0_all', config={'processes':{'publish_streams':{'conductivity':c_stream_id, 'temperature':t_stream_id, 'pressure': p_stream_id } } })


class ctd_L0_all(TransformDataProcess):
    """Model for a TransformDataProcess

    """

    # Make the stream definitions of the transform class attributes
    incoming_stream_def = SBE37_CDM_stream_definition()

    #outgoing_stream_pressure = L0_pressure_stream_definition()
    #outgoing_stream_temperature = L0_temperature_stream_definition()
    #outgoing_stream_conductivity = L0_conductivity_stream_definition()

    ### Taxonomies are defined before hand out of band... somehow.
    pres_tx = TaxyTool()
    pres_tx.add_taxonomy_set('pres','long name for pres')
    pres_tx.add_taxonomy_set('lat','long name for latitude')
    pres_tx.add_taxonomy_set('lon','long name for longitude')
    pres_tx.add_taxonomy_set('height','long name for height')
    pres_tx.add_taxonomy_set('time','long name for time')
    # This is an example of using groups it is not a normative statement about how to use groups
    pres_tx.add_taxonomy_set('coordinates','This group contains coordinates...')
    pres_tx.add_taxonomy_set('data','This group contains data...')

    temp_tx = TaxyTool()
    temp_tx.add_taxonomy_set('temp','long name for temp')
    temp_tx.add_taxonomy_set('lat','long name for latitude')
    temp_tx.add_taxonomy_set('lon','long name for longitude')
    temp_tx.add_taxonomy_set('height','long name for height')
    temp_tx.add_taxonomy_set('time','long name for time')
    # This is an example of using groups it is not a normative statement about how to use groups
    temp_tx.add_taxonomy_set('coordinates','This group contains coordinates...')
    temp_tx.add_taxonomy_set('data','This group contains data...')

    cond_tx = TaxyTool()
    cond_tx.add_taxonomy_set('cond','long name for cond')
    cond_tx.add_taxonomy_set('lat','long name for latitude')
    cond_tx.add_taxonomy_set('lon','long name for longitude')
    cond_tx.add_taxonomy_set('height','long name for height')
    cond_tx.add_taxonomy_set('time','long name for time')
    # This is an example of using groups it is not a normative statement about how to use groups
    cond_tx.add_taxonomy_set('coordinates','This group contains coordinates...')
    cond_tx.add_taxonomy_set('data','This group contains data...')

    def process(self, packet):

        """Processes incoming data!!!!
        """

        # Use the PointSupplementStreamParser to pull data from a granule
        #psd = PointSupplementStreamParser(stream_definition=self.incoming_stream_def, stream_granule=packet)
        rdt = RecordDictionaryTool.load_from_granule(packet)
        #todo: use only flat dicts for now, may change later...
#        rdt0 = rdt['coordinates']
#        rdt1 = rdt['data']

        conductivity = get_safe(rdt, 'cond') #psd.get_values('conductivity')
        pressure = get_safe(rdt, 'pres') #psd.get_values('pressure')
        temperature = get_safe(rdt, 'temp') #psd.get_values('temperature')

        longitude = get_safe(rdt, 'lon') # psd.get_values('longitude')
        latitude = get_safe(rdt, 'lat')  #psd.get_values('latitude')
        time = get_safe(rdt, 'time') # psd.get_values('time')
        height = get_safe(rdt, 'height') # psd.get_values('time')

        log.warn('Got conductivity: %s' % str(conductivity))
        log.warn('Got pressure: %s' % str(pressure))
        log.warn('Got temperature: %s' % str(temperature))

        g = self._build_granule_settings(self.cond_tx, 'cond', conductivity, time, latitude, longitude, height)
        
        self.conductivity.publish(g)

        g = self._build_granule_settings(self.temp_tx, 'temp', temperature, time, latitude, longitude, height)

        self.temperature.publish(g)

        g = self._build_granule_settings(self.pres_tx, 'pres', pressure, time, latitude, longitude, height)

        self.pressure.publish(g)

        return

    def _build_granule_settings(self, taxonomy=None, field_name='', value=None, time=None, latitude=None, longitude=None, height=None):

        root_rdt = RecordDictionaryTool(taxonomy=taxonomy)

        #data_rdt = RecordDictionaryTool(taxonomy=taxonomy)

        root_rdt[field_name] = value

        #coor_rdt = RecordDictionaryTool(taxonomy=taxonomy)

        root_rdt['time'] = time
        root_rdt['lat'] = latitude
        root_rdt['lon'] = longitude
        root_rdt['height'] = height

        #todo: use only flat dicts for now, may change later...
#        root_rdt['coordinates'] = coor_rdt
#        root_rdt['data'] = data_rdt

        log.debug("ctd_L0_all:_build_granule_settings: logging published Record Dictionary:\n %s", str(root_rdt.pretty_print()))

        return build_granule(data_producer_id='ctd_L0', taxonomy=taxonomy, record_dictionary=root_rdt)





  