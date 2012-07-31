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
from coverage_model.parameter import ParameterDictionary, ParameterContext
from coverage_model.parameter import QuantityType
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

    def __init__(self):
        # Make the stream definitions of the transform class attributes
        self.incoming_stream_def = SBE37_CDM_stream_definition()

        #outgoing_stream_pressure = L0_pressure_stream_definition()
        #outgoing_stream_temperature = L0_temperature_stream_definition()
        #outgoing_stream_conductivity = L0_conductivity_stream_definition()

        ### Taxonomies are defined before hand out of band... somehow.
    #    pres = TaxyTool()
    #    pres.add_taxonomy_set('pres','long name for pres')
    #    pres.add_taxonomy_set('lat','long name for latitude')
    #    pres.add_taxonomy_set('lon','long name for longitude')
    #    pres.add_taxonomy_set('height','long name for height')
    #    pres.add_taxonomy_set('time','long name for time')
    #    # This is an example of using groups it is not a normative statement about how to use groups
    #    pres.add_taxonomy_set('coordinates','This group contains coordinates...')
    #    pres.add_taxonomy_set('data','This group contains data...')
    #
    #    temp = TaxyTool()
    #    temp.add_taxonomy_set('temp','long name for temp')
    #    temp.add_taxonomy_set('lat','long name for latitude')
    #    temp.add_taxonomy_set('lon','long name for longitude')
    #    temp.add_taxonomy_set('height','long name for height')
    #    temp.add_taxonomy_set('time','long name for time')
    #    # This is an example of using groups it is not a normative statement about how to use groups
    #    temp.add_taxonomy_set('coordinates','This group contains coordinates...')
    #    temp.add_taxonomy_set('data','This group contains data...')
    #
    #    coord = TaxyTool()
    #    coord.add_taxonomy_set('cond','long name for cond')
    #    coord.add_taxonomy_set('lat','long name for latitude')
    #    coord.add_taxonomy_set('lon','long name for longitude')
    #    coord.add_taxonomy_set('height','long name for height')
    #    coord.add_taxonomy_set('time','long name for time')
    #    # This is an example of using groups it is not a normative statement about how to use groups
    #    coord.add_taxonomy_set('coordinates','This group contains coordinates...')
    #    coord.add_taxonomy_set('data','This group contains data...')

        ### Parameter dictionaries

        self.pres = ParameterDictionary()
        self.pres.add_context(ParameterContext('pres', param_type=QuantityType(value_encoding='f', uom='Pa') ))
        self.pres.add_context(ParameterContext('lat', param_type=QuantityType(value_encoding='f', uom='deg') ))
        self.pres.add_context(ParameterContext('lon', param_type=QuantityType(value_encoding='f', uom='deg') ))
        self.pres.add_context(ParameterContext('height', param_type=QuantityType(value_encoding='f', uom='km') ))

        self.temp = ParameterDictionary()
        self.temp.add_context(ParameterContext('temp', param_type=QuantityType(value_encoding='f', uom='K') ))
        self.temp.add_context(ParameterContext('lat', param_type=QuantityType(value_encoding='f', uom='deg') ))
        self.temp.add_context(ParameterContext('lon', param_type=QuantityType(value_encoding='f', uom='deg') ))
        self.temp.add_context(ParameterContext('height', param_type=QuantityType(value_encoding='f', uom='km') ))

        self.cond = ParameterDictionary()
        self.cond.add_context(ParameterContext('cond', param_type=QuantityType(value_encoding='f', uom='K') ))
        self.cond.add_context(ParameterContext('lat', param_type=QuantityType(value_encoding='f', uom='deg') ))
        self.cond.add_context(ParameterContext('lon', param_type=QuantityType(value_encoding='f', uom='deg') ))
        self.cond.add_context(ParameterContext('height', param_type=QuantityType(value_encoding='f', uom='km') ))

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

        g = self._build_granule_settings(self.cond, 'cond', conductivity, time, latitude, longitude, height)

        conductivity.publish(g)

        g = self._build_granule_settings(self.temp, 'temp', temperature, time, latitude, longitude, height)

        temperature.publish(g)

        g = self._build_granule_settings(self.pres, 'pres', pressure, time, latitude, longitude, height)

        pressure.publish(g)

        return

    def _build_granule_settings(self, param_dictionary=None, field_name='', value=None, time=None, latitude=None, longitude=None, height=None):

        root_rdt = RecordDictionaryTool(param_dictionary=param_dictionary)

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

        return build_granule(data_producer_id='ctd_L0', param_dictionary=param_dictionary, record_dictionary=root_rdt)





  