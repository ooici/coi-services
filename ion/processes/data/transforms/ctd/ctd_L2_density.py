
'''
@author MManning
@file ion/processes/data/transforms/ctd/ctd_L2_density.py
@description Transforms CTD parsed data into L2 product for density
'''

from pyon.ion.transforma import TransformDataProcess
from pyon.public import log
from ion.services.dm.utility.granule.record_dictionary import RecordDictionaryTool
from ion.core.function.transform_function import SimpleGranuleTransformFunction
from coverage_model.parameter import ParameterDictionary, ParameterContext
from coverage_model.parameter_types import QuantityType
from coverage_model.basic_types import AxisTypeEnum
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient

from seawater.gibbs import SP_from_cndr, rho, SA_from_SP
from seawater.gibbs import cte

# For usage: please refer to the integration tests in
# ion/processes/data/transforms/ctd/test/test_ctd_transforms.py

class DensityTransform(TransformDataProcess):
    ''' A basic transform that receives input through a subscription,
    parses the input from a CTD, extracts the pressure value and scales it according to
    the defined algorithm. If the transform
    has an output_stream it will publish the output on the output stream.
    '''

    def on_start(self):
        super(DensityTransform, self).on_start()
        if not self.CFG.process.publish_streams.has_key('density'):
            raise AssertionError("For CTD transforms, please send the stream_id "
                                 "using a special keyword (ex: density)")
        self.dens_stream = self.CFG.process.publish_streams.density

        # Read the parameter dict from the stream def of the stream
        pubsub = PubsubManagementServiceClient()
        stream_definition = pubsub.read_stream_definition(stream_id=self.dens_stream)
        pdict = stream_definition.parameter_dictionary
        self.dens_pdict = ParameterDictionary.load(pdict)

    def recv_packet(self, packet, stream_route, stream_id):
        """
        Processes incoming data!!!!
        """
        if packet == {}:
            return
        granule = CTDL2DensityTransformAlgorithm.execute(packet, params=self.dens_pdict)
        self.density.publish(msg=granule)


class CTDL2DensityTransformAlgorithm(SimpleGranuleTransformFunction):

    @staticmethod
    @SimpleGranuleTransformFunction.validate_inputs
    def execute(input=None, context=None, config=None, params=None, state=None):

        rdt = RecordDictionaryTool.load_from_granule(input)

        conductivity = rdt['conductivity']
        pressure = rdt['pressure']
        temperature = rdt['temp']

        longitude = rdt['lon']
        latitude = rdt['lat']

        sp = SP_from_cndr(r=conductivity/cte.C3515, t=temperature, p=pressure)
        sa = SA_from_SP(sp, pressure, longitude, latitude)
        dens_value = rho(sa, temperature, pressure)
        # build the granule for density
        result = CTDL2DensityTransformAlgorithm._build_granule(param_dictionary=params,
                                                                        field_name='density',
                                                                        value=dens_value)

        return result

    @staticmethod
    def _build_granule(param_dictionary=None, field_name='', value=None):

        root_rdt = RecordDictionaryTool(param_dictionary=param_dictionary)
        root_rdt[field_name] = value
        return root_rdt.to_granule()
