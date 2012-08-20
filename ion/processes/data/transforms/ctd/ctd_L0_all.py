'''
@author MManning
@file ion/processes/data/transforms/ctd/ctd_L0_all.py
@description Transforms CTD parsed data into L0 streams
'''

from pyon.ion.transforma import TransformDataProcess, TransformAlgorithm
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
import re

from prototype.sci_data.stream_parser import PointSupplementStreamParser
#from prototype.sci_data.constructor_apis import PointSupplementConstructor
#from prototype.sci_data.stream_defs import ctd_stream_definition

### For new granule and stream interface
from ion.services.dm.utility.granule.record_dictionary import RecordDictionaryTool
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
        self.cond_stream = self.CFG.process.publish_streams.conductivity
        self.temp_stream = self.CFG.process.publish_streams.temperature
        self.pres_stream = self.CFG.process.publish_streams.pressure
        super(ctd_L0_all, self).__init__()

        # Make the stream definitions of the transform class attributes

        #outgoing_stream_pressure = L0_pressure_stream_definition()
        #outgoing_stream_temperature = L0_temperature_stream_definition()
        #outgoing_stream_conductivity = L0_conductivity_stream_definition()

    def recv_packet(self, msg, headers):
        stream_id = headers['routing_key']
        stream_id = re.sub(r'\.data', '', stream_id)
        self.receive_msg(msg, stream_id)

    def publish(self, msg, stream_id):
        self.publisher.publish(msg=msg, stream_id=stream_id)

    def receive_msg(self, packet, stream_id):

        """Processes incoming data!!!!
        """
        if packet == {}:
            return

        # Use the PointSupplementStreamParser to pull data from a granule
        #psd = PointSupplementStreamParser(stream_definition=self.incoming_stream_def, stream_granule=packet)
        rdt = RecordDictionaryTool.load_from_granule(packet)

        conductivity = get_safe(rdt, 'conductivity') #psd.get_values('conductivity')
        pressure = get_safe(rdt, 'pressure') #psd.get_values('pressure')
        temperature = get_safe(rdt, 'temp') #psd.get_values('temperature')

        longitude = get_safe(rdt, 'lon') # psd.get_values('longitude')
        latitude = get_safe(rdt, 'lat')  #psd.get_values('latitude')
        time = get_safe(rdt, 'time') # psd.get_values('time')
        depth = get_safe(rdt, 'depth') # psd.get_values('time')

        # Apply the algorithm
        conductivity = ctd_L0_algorithm.execute(conductivity)
        pressure = ctd_L0_algorithm.execute(pressure)
        temperature = ctd_L0_algorithm.execute(temperature)

        # create parameter settings
        self.cond = self._create_parameter("conductivity")
        self.pres = self._create_parameter("pressure")
        self.temp = self._create_parameter("temp")

        # build the granule for conductivity
        g = self._build_granule_settings(self.cond, 'conductivity', conductivity, time, latitude, longitude, depth)
        self.publish(msg= g, stream_id=self.cond_stream)

        # build the granule for temperature
        g = self._build_granule_settings(self.temp, 'temp', temperature, time, latitude, longitude, depth)
        self.publish(msg= g, stream_id=self.temp_stream)

        # build the granule for pressure
        g = self._build_granule_settings(self.pres, 'pressure', pressure, time, latitude, longitude, depth)
        self.publish(msg= g, stream_id=self.pres_stream)

    def _build_granule_settings(self, param_dictionary=None, field_name='', value=None, time=None, latitude=None, longitude=None, depth=None):

        root_rdt = RecordDictionaryTool(param_dictionary=param_dictionary)


        root_rdt[field_name] = value

        root_rdt['time'] = time
        root_rdt['lat'] = latitude
        root_rdt['lon'] = longitude
        root_rdt['depth'] = depth

        log.debug("ctd_L0_all:_build_granule_settings: logging published Record Dictionary:\n %s", str(root_rdt.pretty_print()))

        return build_granule(data_producer_id='ctd_L0', param_dictionary=param_dictionary, record_dictionary=root_rdt)

    def _create_parameter(self, name):

        pdict = ParameterDictionary()

        pdict = self._add_location_time_ctxt(pdict)

        if name == 'conductivity':
            cond_ctxt = ParameterContext('conductivity', param_type=QuantityType(value_encoding=np.float32))
            cond_ctxt.uom = 'unknown'
            cond_ctxt.fill_value = 0e0
            pdict.add_context(cond_ctxt)

        elif name == "pressure":
            pres_ctxt = ParameterContext('pressure', param_type=QuantityType(value_encoding=np.float32))
            pres_ctxt.uom = 'Pascal'
            pres_ctxt.fill_value = 0x0
            pdict.add_context(pres_ctxt)

        elif name == "temp":
            temp_ctxt = ParameterContext('temp', param_type=QuantityType(value_encoding=np.float32))
            temp_ctxt.uom = 'degree_Celsius'
            temp_ctxt.fill_value = 0e0
            pdict.add_context(temp_ctxt)

        return pdict

    def _add_location_time_ctxt(self, pdict):

        t_ctxt = ParameterContext('time', param_type=QuantityType(value_encoding=np.int64))
        t_ctxt.reference_frame = AxisTypeEnum.TIME
        t_ctxt.uom = 'seconds since 1970-01-01'
        t_ctxt.fill_value = 0x0
        pdict.add_context(t_ctxt)

        lat_ctxt = ParameterContext('lat', param_type=QuantityType(value_encoding=np.float32))
        lat_ctxt.reference_frame = AxisTypeEnum.LAT
        lat_ctxt.uom = 'degree_north'
        lat_ctxt.fill_value = 0e0
        pdict.add_context(lat_ctxt)

        lon_ctxt = ParameterContext('lon', param_type=QuantityType(value_encoding=np.float32))
        lon_ctxt.reference_frame = AxisTypeEnum.LON
        lon_ctxt.uom = 'degree_east'
        lon_ctxt.fill_value = 0e0
        pdict.add_context(lon_ctxt)

        depth_ctxt = ParameterContext('depth', param_type=QuantityType(value_encoding=np.float32))
        depth_ctxt.reference_frame = AxisTypeEnum.HEIGHT
        depth_ctxt.uom = 'meters'
        depth_ctxt.fill_value = 0e0
        pdict.add_context(depth_ctxt)

        return pdict


class ctd_L0_algorithm(TransformAlgorithm):

    @staticmethod
    def execute(*args, **kwargs):
        return args[0]