'''
@author Swarbhanu Chatterjee
@file ion/processes/data/transforms/ctdbp/ctdbp_L0.py
@description Transforms incoming CTD parsed data into L0 products to send out through the L0 stream
'''

from pyon.core.exception import BadRequest
from pyon.public import log

from ion.core.process.transform import TransformDataProcess
from ion.services.dm.utility.granule.record_dictionary import RecordDictionaryTool
from ion.core.function.transform_function import MultiGranuleTransformFunction
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceProcessClient

class CTDBP_L0_all(TransformDataProcess):
    """
    L0 listens to the parsed and pulls only C, T, & Pressure from the parsed and puts it onto the L0 stream. The other parameters are dropped.
    """
    output_bindings = ['L0_stream']
    def on_start(self):
        super(CTDBP_L0_all, self).on_start()

        if not self.CFG.process.publish_streams.has_key('L0_stream'):
            raise BadRequest("For CTD transforms, please send the stream_id for the L0_stream using "
                             "a special keyword (L0_stream)")
        self.L0_stream_id = self.CFG.process.publish_streams.L0_stream

        log.debug("the output stream: %s", self.L0_stream_id)

        pubsub = PubsubManagementServiceProcessClient(process=self)
        self.stream_def_L0 = pubsub.read_stream_definition(stream_id=self.L0_stream_id)

        self.params = {'L0_stream' : self.stream_def_L0._id }

        log.debug("the params: %s", self.params)

    def recv_packet(self, packet,stream_route, stream_id):
        """Processes incoming data!!!!
            @param packet granule
            @param stream_route StreamRoute
            @param stream_id str
        """

        log.debug("CTDBP_L0_all received in the stream, %s, the packet: %s", stream_id, packet)

        if packet == {}:
            return
        granules = ctdbp_L0_algorithm.execute([packet], params=self.params)
        for granule in granules:
            self.L0_stream.publish(msg=granule)

class ctdbp_L0_algorithm(MultiGranuleTransformFunction):

    @staticmethod
    @MultiGranuleTransformFunction.validate_inputs
    def execute(input=None, context=None, config=None, params=None, state=None):
        """
        @param input granule
        @retval result_list list of dictionaries containing granules as values
        """

        result_list = []
        for x in input:
            rdt = RecordDictionaryTool.load_from_granule(x)

            conductivity = rdt['conductivity']
            pressure = rdt['pressure']
            temperature = rdt['temperature']
            time = rdt['time']

            # build the granule for conductivity, temperature and pressure
            granule = ctdbp_L0_algorithm._build_granule(stream_definition_id= params['L0_stream'],
                field_names= ['conductivity', 'pressure', 'temperature', 'time'], # these are the field names for the output record dictionary
                values= [conductivity, pressure, temperature, time])

            result_list.append(granule)

        return result_list

    @staticmethod
    def _build_granule(stream_definition_id=None, field_names=None, values=None):
        """
        Builds a granule with values corresponding only to the params specified in the field names

        @param param_dictionary ParameterDictionary
        @param field_name str
        @param value numpy.array

        @retval Granule
        """
        root_rdt = RecordDictionaryTool(stream_definition_id=stream_definition_id)
        zipped = zip(field_names, values)

        for k,v in zipped:
            root_rdt[k] = v

        return root_rdt.to_granule()

