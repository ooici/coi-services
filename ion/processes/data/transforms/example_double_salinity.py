'''
@author Stephen Henrie
@description Example Transform to double salinity
'''
from prototype.sci_data.stream_defs import L2_practical_salinity_stream_definition
from pyon.util.log import log
from pyon.ion.transforma import TransformDataProcess, TransformAlgorithm
from prototype.sci_data.stream_parser import PointSupplementStreamParser
from prototype.sci_data.constructor_apis import PointSupplementConstructor
from coverage_model.parameter import ParameterContext, ParameterDictionary
from coverage_model.parameter_types import QuantityType
from coverage_model.basic_types import AxisTypeEnum
### For new granule and stream interface
from ion.services.dm.utility.granule.record_dictionary import RecordDictionaryTool
from ion.services.dm.utility.granule.granule import build_granule
from pyon.util.containers import get_safe
import numpy, re

class SalinityDoubler(TransformDataProcess):

    outgoing_stream_def = L2_practical_salinity_stream_definition()
    incoming_stream_def = L2_practical_salinity_stream_definition()

    def on_start(self):
        super(SalinityDoubler, self).on_start()
        self.sal_stream = self.CFG.process.publish_streams.salinity

    def recv_packet(self, msg, headers):
        log.warn('ctd_L2_salinity.recv_packet: {0}'.format(msg))
        stream_id = headers['routing_key']
        stream_id = re.sub(r'\.data', '', stream_id)
        self.receive_msg(msg, stream_id)

    def publish(self, msg, stream_id):
        self.publisher.publish(msg=msg, stream_id=stream_id)

    def receive_msg(self, granule, stream_id):
        """
        Example process to double the salinity value
        """
        # Use the PointSupplementStreamParser to pull data from a granule
        #psd = PointSupplementStreamParser(stream_definition=self.incoming_stream_def, stream_granule=packet)
        rdt = RecordDictionaryTool.load_from_granule(granule)

        salinity = get_safe(rdt, 'salinity')

        longitude = get_safe(rdt, 'lon')
        latitude = get_safe(rdt, 'lat')
        time = get_safe(rdt, 'time')
        depth = get_safe(rdt, 'depth')

        # Use the constructor to put data into a granule
#        psc = PointSupplementConstructor(point_definition=self.outgoing_stream_def, stream_id=self.streams['output'])
#
#        for i in xrange(len(salinity)):
#            point_id = psc.add_point(time=time[i],location=(longitude[i],latitude[i],depth[i]))
#            psc.add_scalar_point_coverage(point_id=point_id, coverage_id='salinity', value=salinity[i])
#
#        return psc.close_stream_granule()
        parameter_dictionary = self._create_parameter()
        root_rdt = RecordDictionaryTool(param_dictionary=parameter_dictionary)

        root_rdt['salinity'] = ctd_L2_salinity_algorithm.execute(salinity)
        root_rdt['time'] = time
        root_rdt['lat'] = latitude
        root_rdt['lon'] = longitude
        root_rdt['depth'] = depth

        g = build_granule(data_producer_id='ctd_L2_salinity', param_dictionary=parameter_dictionary, record_dictionary=root_rdt)
        self.publish(msg=g, stream_id=self.sal_stream)

    def _create_parameter(self):

        pdict = ParameterDictionary()

        pdict = self._add_location_time_ctxt(pdict)

        sal_ctxt = ParameterContext('salinity', param_type=QuantityType(value_encoding=numpy.float32))
        sal_ctxt.uom = 'PSU'
        sal_ctxt.fill_value = 0x0
        pdict.add_context(sal_ctxt)

        return pdict

    def _add_location_time_ctxt(self, pdict):

        t_ctxt = ParameterContext('time', param_type=QuantityType(value_encoding=numpy.int64))
        t_ctxt.reference_frame = AxisTypeEnum.TIME
        t_ctxt.uom = 'seconds since 1970-01-01'
        t_ctxt.fill_value = 0x0
        pdict.add_context(t_ctxt)

        lat_ctxt = ParameterContext('lat', param_type=QuantityType(value_encoding=numpy.float32))
        lat_ctxt.reference_frame = AxisTypeEnum.LAT
        lat_ctxt.uom = 'degree_north'
        lat_ctxt.fill_value = 0e0
        pdict.add_context(lat_ctxt)

        lon_ctxt = ParameterContext('lon', param_type=QuantityType(value_encoding=numpy.float32))
        lon_ctxt.reference_frame = AxisTypeEnum.LON
        lon_ctxt.uom = 'degree_east'
        lon_ctxt.fill_value = 0e0
        pdict.add_context(lon_ctxt)

        depth_ctxt = ParameterContext('depth', param_type=QuantityType(value_encoding=numpy.float32))
        depth_ctxt.reference_frame = AxisTypeEnum.HEIGHT
        depth_ctxt.uom = 'meters'
        depth_ctxt.fill_value = 0e0
        pdict.add_context(depth_ctxt)

        return pdict

class ctd_L2_salinity_algorithm(TransformAlgorithm):

    @staticmethod
    def execute(*args, **kwargs):
        return 2*args[0]