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
from coverage_model.parameter_types import QuantityType
from coverage_model.basic_types import AxisTypeEnum
from prototype.sci_data.stream_defs import SBE37_CDM_stream_definition, L0_pressure_stream_definition, L0_temperature_stream_definition, L0_conductivity_stream_definition
from pyon.net.endpoint import Publisher
import numpy as np
import uuid
from pyon.core.bootstrap import get_sys_name
from pyon.net.transport import NameTrio

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

    incoming_stream_def = SBE37_CDM_stream_definition()

    def __init__(self):

        super(ctd_L0_all, self).__init__()

        # Make the stream definitions of the transform class attributes

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
        self.defining_parameter_dictionary()

        self.publisher = Publisher(to_name=NameTrio(get_sys_name(), str(uuid.uuid4())[0:6]))

    def defining_parameter_dictionary(self):

        # Define the parameter context objects

        t_ctxt = ParameterContext('time', param_type=QuantityType(value_encoding=np.int64))
        t_ctxt.reference_frame = AxisTypeEnum.TIME
        t_ctxt.uom = 'seconds since 1970-01-01'
        t_ctxt.fill_value = 0x0

        lat_ctxt = ParameterContext('lat', param_type=QuantityType(value_encoding=np.float32))
        lat_ctxt.reference_frame = AxisTypeEnum.LAT
        lat_ctxt.uom = 'degree_north'
        lat_ctxt.fill_value = 0e0

        lon_ctxt = ParameterContext('lon', param_type=QuantityType(value_encoding=np.float32))
        lon_ctxt.reference_frame = AxisTypeEnum.LON
        lon_ctxt.uom = 'degree_east'
        lon_ctxt.fill_value = 0e0

        height_ctxt = ParameterContext('height', param_type=QuantityType(value_encoding=np.float32))
        height_ctxt.reference_frame = AxisTypeEnum.HEIGHT
        height_ctxt.uom = 'meters'
        height_ctxt.fill_value = 0e0

        pres_ctxt = ParameterContext('pres', param_type=QuantityType(value_encoding=np.float32))
        pres_ctxt.uom = 'degree_Celsius'
        pres_ctxt.fill_value = 0e0

        temp_ctxt = ParameterContext('temp', param_type=QuantityType(value_encoding=np.float32))
        temp_ctxt.uom = 'degree_Celsius'
        temp_ctxt.fill_value = 0e0

        cond_ctxt = ParameterContext('cond', param_type=QuantityType(value_encoding=np.float32))
        cond_ctxt.uom = 'unknown'
        cond_ctxt.fill_value = 0e0

        data_ctxt = ParameterContext('data', param_type=QuantityType(value_encoding=np.int8))
        data_ctxt.uom = 'byte'
        data_ctxt.fill_value = 0x0

        # Define the parameter dictionary objects

        self.pres = ParameterDictionary()
        self.pres.add_context(t_ctxt)
        self.pres.add_context(lat_ctxt)
        self.pres.add_context(lon_ctxt)
        self.pres.add_context(height_ctxt)
        self.pres.add_context(pres_ctxt)
        self.pres.add_context(data_ctxt)

        self.temp = ParameterDictionary()
        self.temp.add_context(t_ctxt)
        self.temp.add_context(lat_ctxt)
        self.temp.add_context(lon_ctxt)
        self.temp.add_context(height_ctxt)
        self.temp.add_context(temp_ctxt)
        self.temp.add_context(data_ctxt)

        self.cond = ParameterDictionary()
        self.cond.add_context(t_ctxt)
        self.cond.add_context(lat_ctxt)
        self.cond.add_context(lon_ctxt)
        self.cond.add_context(height_ctxt)
        self.cond.add_context(cond_ctxt)
        self.cond.add_context(data_ctxt)

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

        # publish a granule
        self.cond_publisher = self.publisher
        self.cond_publisher.publish(g)

        g = self._build_granule_settings(self.temp, 'temp', temperature, time, latitude, longitude, height)

        # publish a granule
        self.temp_publisher = self.publisher
        self.temp_publisher.publish(g)

        g = self._build_granule_settings(self.pres, 'pres', pressure, time, latitude, longitude, height)

        # publish a granule
        self.pres_publisher = self.publisher
        self.pres_publisher.publish(g)

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





  