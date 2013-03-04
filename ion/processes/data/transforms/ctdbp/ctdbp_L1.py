
'''
@author Swarbhanu Chatterjee
@file ion/processes/data/transforms/ctdbp/ctd_L1.py
@description Transforms CTD parsed data into L1 products for conductivity, temperature and pressure
'''

from ion.core.process.transform import TransformDataProcess
from pyon.core.exception import BadRequest

from interface.services.dm.ipubsub_management_service import PubsubManagementServiceProcessClient
from ion.services.dm.utility.granule.record_dictionary import RecordDictionaryTool
from ion.core.function.transform_function import SimpleGranuleTransformFunction


class CTDBP_L1_Transform(TransformDataProcess):
    '''
    L1 takes the L0 stream and the passed in Calibration Coefficients and performs the appropriate calculations
    to output the L1 values.
    '''
    output_bindings = ['conductivity']

    def on_start(self):
        super(CTDBPL1Transform, self).on_start()

        if not self.CFG.process.publish_streams.has_key('L1_stream'):
            raise BadRequest("For CTD transforms, please send the stream_id for the L1_stream using "
                             "a special keyword (L1_stream)")
        self.L1_stream = self.CFG.process.publish_streams.L1_stream

        # Read the parameter dict from the stream def of the stream
        pubsub = PubsubManagementServiceProcessClient(process=self)
        self.stream_definition = pubsub.read_stream_definition(stream_id=self.L1_stream)

    def recv_packet(self, packet, stream_route, stream_id):
        """Processes incoming data!!!!
        """
        if packet == {}:
            return

        granule = CTDBP_L1_ConductivityTransformAlgorithm.execute(packet, params=self.stream_definition._id)
        self.conductivity.publish(msg=granule)


class CTDBP_L1_TransformAlgorithm(SimpleGranuleTransformFunction):
    '''

    '''
    @staticmethod
    @SimpleGranuleTransformFunction.validate_inputs
    def execute(input=None, context=None, config=None, params=None, state=None):

        rdt = RecordDictionaryTool.load_from_granule(input)
        out_rdt = RecordDictionaryTool(stream_definition_id=params)

        out_rdt = CTDBP_L1_TransformAlgorithm.calculate_conductivity(rdt, out_rdt)
        out_rdt = CTDBP_L1_TransformAlgorithm.calculate_temperature(rdt, out_rdt)
        out_rdt = CTDBP_L1_TransformAlgorithm.calculate_pressure(rdt, out_rdt)

        # build the granule for conductivity
        return out_rdt.to_granule()

    @staticmethod
    def calculate_conductivity(rdt = None, out_rdt = None):
        conductivity = rdt['conductivity']
        cond_value = (conductivity / 100000.0) - 0.5

        for key, value in rdt.iteritems():
            if key in out_rdt:
                out_rdt[key] = value[:]

        # Update the conductivity values
        out_rdt['conductivity'] = cond_value

        return out_rdt

    @staticmethod
    def calculate_temperature(rdt = None, out_rdt = None):
        temperature = rdt['temp']
        temp_value = (temperature / 10000.0) - 10

        for key, value in rdt.iteritems():
            if key in out_rdt:
                out_rdt[key] = value[:]

        out_rdt['temp'] = temp_value

        return out_rdt

    @staticmethod
    def calculate_pressure(rdt = None, out_rdt = None):
        pressure = rdt['pressure']
        pres_value = (pressure / 100.0) + 0.5

        for key, value in rdt.iteritems():
            if key in out_rdt:
                out_rdt[key] = value[:]

        out_rdt['pressure'] = pres_value

        return out_rdt
