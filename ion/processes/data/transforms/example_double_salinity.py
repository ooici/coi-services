'''
@author Stephen Henrie
@description Example Transform to double salinity
'''
from pyon.util.log import log
from ion.core.process.transform import TransformDataProcess
from ion.core.function.transform_function import SimpleGranuleTransformFunction
from coverage_model.parameter import ParameterContext, ParameterDictionary
from coverage_model.parameter_types import QuantityType
from coverage_model.basic_types import AxisTypeEnum
### For new granule and stream interface
from ion.services.dm.utility.granule.record_dictionary import RecordDictionaryTool
from pyon.util.containers import get_safe
import numpy, re

class SalinityDoubler(TransformDataProcess):


    def on_start(self):


        if self.CFG.process.publish_streams.has_key('salinity'):
            self.sal_stream = self.CFG.process.publish_streams.salinity
        elif self.CFG.process.publish_streams.has_key('output'):
            self.sal_stream = self.CFG.process.publish_streams.output
        self.CFG.process.stream_id = self.sal_stream
        super(SalinityDoubler, self).on_start()

    def publish(self, msg, stream_id):
        self.publisher.publish(msg)

    def recv_packet(self, granule, stream_route, stream_id):
        """
        Example process to double the salinity value
        """
        g = ctd_L2_salinity_algorithm.execute(granule)

        self.publish(msg=g, stream_id=self.sal_stream)

        return g

class ctd_L2_salinity_algorithm(SimpleGranuleTransformFunction):

    @staticmethod
    @SimpleGranuleTransformFunction.validate_inputs
    def execute(input=None, context=None, config=None, params=None, state=None):
        rdt = RecordDictionaryTool.load_from_granule(input)

        salinity = get_safe(rdt, 'salinity')

        longitude = get_safe(rdt, 'lon')
        latitude = get_safe(rdt, 'lat')
        time = get_safe(rdt, 'time')
        depth = get_safe(rdt, 'depth')

        parameter_dictionary = ctd_L2_salinity_algorithm._create_parameter()
        root_rdt = RecordDictionaryTool(param_dictionary=parameter_dictionary)

        root_rdt['salinity'] = 2 * salinity
        root_rdt['time'] = time
        root_rdt['lat'] = latitude
        root_rdt['lon'] = longitude
        root_rdt['depth'] = depth

        g = root_rdt.to_granule()
        return g

    @staticmethod
    def _create_parameter():

        pdict = ParameterDictionary()

        pdict = ctd_L2_salinity_algorithm._add_location_time_ctxt(pdict)

        sal_ctxt = ParameterContext('salinity', param_type=QuantityType(value_encoding=numpy.float32))
        sal_ctxt.uom = 'PSU'
        sal_ctxt.fill_value = 0x0
        pdict.add_context(sal_ctxt)

        return pdict

    @staticmethod
    def _add_location_time_ctxt(pdict):

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
