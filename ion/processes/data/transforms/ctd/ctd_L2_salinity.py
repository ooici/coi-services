
'''
@author MManning
@file ion/processes/data/transforms/ctd/ctd_L2_salinity.py
@description Transforms CTD parsed data into L2 product for salinity
'''
from pyon.util.log import log
from ion.core.process.transform import TransformDataProcess
from pyon.core.exception import BadRequest
from ion.services.dm.utility.granule.record_dictionary import RecordDictionaryTool
from ion.core.function.transform_function import SimpleGranuleTransformFunction
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceProcessClient

from seawater.gibbs import SP_from_cndr
from seawater.gibbs import cte

# For usage: please refer to the integration tests in
# ion/processes/data/transforms/ctd/test/test_ctd_transforms.py

class SalinityTransform(TransformDataProcess):
    ''' A basic transform that receives input through a subscription,
    parses the input from a CTD, extracts the pressure value and scales it according to
    the defined algorithm. If the transform
    has an output_stream it will publish the output on the output stream.
    '''
    output_bindings = ['salinity']

    def on_start(self):
        super(SalinityTransform, self).on_start()

        self.sal_stream = self.CFG.process.publish_streams.values()[0]

        # Read the parameter dict from the stream def of the stream
        pubsub = PubsubManagementServiceProcessClient(process=self)
        self.stream_definition = pubsub.read_stream_definition(stream_id=self.sal_stream)

    def recv_packet(self, packet, stream_route, stream_id):
        """
        Processes incoming data!!!!
        """
        if packet == {}:
            return

        log.debug("L2 salinity transform received granule with record dict: %s", packet.record_dictionary)

        granule = CTDL2SalinityTransformAlgorithm.execute(packet, params=self.stream_definition._id)

        log.debug("L2 salinity transform publishing granule with record dict: %s", granule.record_dictionary)

        granule.data_producer_id=self.id
        self.publisher.publish(msg=granule)


class CTDL2SalinityTransformAlgorithm(SimpleGranuleTransformFunction):

    @staticmethod
    @SimpleGranuleTransformFunction.validate_inputs
    def execute(input=None, context=None, config=None, params=None, state=None):

        rdt = RecordDictionaryTool.load_from_granule(input)
        out_rdt = RecordDictionaryTool(stream_definition_id=params)

        conductivity = rdt['conductivity']
        pressure = rdt['pressure']
        temperature = rdt['temp']

        sal_value = SP_from_cndr(r=conductivity/cte.C3515, t=temperature, p=pressure)

        log.debug("Salinity algorithm calculated the sp (practical salinity) values: %s", sal_value)

        for key, value in rdt.iteritems():
            if key in out_rdt:
                if key=='conductivity' or key=='temp' or key=='pressure':
                    continue
                out_rdt[key] = value[:]

        out_rdt['salinity'] = sal_value

        return out_rdt.to_granule()

