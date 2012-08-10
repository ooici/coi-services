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
from ion.services.dm.utility.granule.granule import build_granule
from ion.services.dm.utility.granule_utils import CoverageCraft

#from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
#pmsc = PubsubManagementServiceClient(node=cc.node)
#c_stream_id = pmsc.create_stream(name='conductivity')
#t_stream_id = pmsc.create_stream(name='temperature')
#p_stream_id = pmsc.create_stream(name='pressure')
#cc.spawn_process('l0_transform', 'ion.processes.data.transforms.ctd_L0_all','ctd_L0_all', config={'processes':{'publish_streams':{'conductivity':c_stream_id, 'temperature':t_stream_id, 'pressure': p_stream_id } } })

craft = CoverageCraft
sdom, tdom = craft.create_domains()
sdom = sdom.dump()
tdom = tdom.dump()
parameter_dictionary = craft.create_parameters()

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

        self.publisher = Publisher(to_name=NameTrio(get_sys_name(), str(uuid.uuid4())[0:6]))


    def process(self, packet):

        """Processes incoming data!!!!
        """

        # Use the PointSupplementStreamParser to pull data from a granule
        #psd = PointSupplementStreamParser(stream_definition=self.incoming_stream_def, stream_granule=packet)
        rdt = RecordDictionaryTool.load_from_granule(packet)
        #todo: use only flat dicts for now, may change later...
#        rdt0 = rdt['coordinates']
#        rdt1 = rdt['data']

        conductivity = get_safe(rdt, 'conductivity') #psd.get_values('conductivity')
        pressure = get_safe(rdt, 'pressure') #psd.get_values('pressure')
        temperature = get_safe(rdt, 'temp') #psd.get_values('temperature')

        longitude = get_safe(rdt, 'lon') # psd.get_values('longitude')
        latitude = get_safe(rdt, 'lat')  #psd.get_values('latitude')
        time = get_safe(rdt, 'time') # psd.get_values('time')
        depth = get_safe(rdt, 'depth') # psd.get_values('time')

        log.warn('Got conductivity: %s' % str(conductivity))
        log.warn('Got pressure: %s' % str(pressure))
        log.warn('Got temperature: %s' % str(temperature))

        g = self._build_granule_settings(self.cond, 'conductivity', conductivity, time, latitude, longitude, depth)

        # publish a granule
        self.cond_publisher = self.publisher
        self.cond_publisher.publish(g)

        g = self._build_granule_settings(self.temp, 'temp', temperature, time, latitude, longitude, depth)

        # publish a granule
        self.temp_publisher = self.publisher
        self.temp_publisher.publish(g)

        g = self._build_granule_settings(self.pres, 'pressure', pressure, time, latitude, longitude, depth)

        # publish a granule
        self.pres_publisher = self.publisher
        self.pres_publisher.publish(g)

    def _build_granule_settings(self, param_dictionary=None, field_name='', value=None, time=None, latitude=None, longitude=None, depth=None):

        root_rdt = RecordDictionaryTool(param_dictionary=param_dictionary)


        root_rdt[field_name] = value

        root_rdt['time'] = time
        root_rdt['lat'] = latitude
        root_rdt['lon'] = longitude
        root_rdt['depth'] = depth

        log.debug("ctd_L0_all:_build_granule_settings: logging published Record Dictionary:\n %s", str(root_rdt.pretty_print()))

        return build_granule(data_producer_id='ctd_L0', param_dictionary=param_dictionary, record_dictionary=root_rdt)





  