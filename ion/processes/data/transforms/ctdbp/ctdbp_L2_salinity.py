
"""
@author Swarbhanu Chatterjee
@file ion/processes/data/transforms/ctdbp/ctdbp_L2_salinity.py
@description Transforms CTD parsed data into L2 product for salinity
"""
from pyon.util.log import log
from ion.core.process.transform import TransformDataProcess
from pyon.core.exception import BadRequest
from ion.services.dm.utility.granule.record_dictionary import RecordDictionaryTool
from ion.core.function.transform_function import SimpleGranuleTransformFunction
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceProcessClient


import pygsw.vectors as gsw

# For usage: please refer to the integration tests in
# ion/processes/data/transforms/ctd/test/test_ctd_transforms.py

class CTDBP_SalinityTransform(TransformDataProcess):
    """ A basic transform that receives input through a subscription,
    parses the input from a CTD, extracts the pressure value and scales it according to
    the defined algorithm. If the transform
    has an output_stream it will publish the output on the output stream.
    """
    output_bindings = ['salinity']

    def on_start(self):
        super(CTDBP_SalinityTransform, self).on_start()

        if not self.CFG.process.publish_streams.has_key('salinity'):
            raise BadRequest("For CTD transforms, please send the stream_id "
                             "using a special keyword (ex: salinity)")

        self.sal_stream_id = self.CFG.process.publish_streams.salinity

        # Read the parameter dict from the stream def of the stream
        pubsub = PubsubManagementServiceProcessClient(process=self)
        self.stream_definition = pubsub.read_stream_definition(stream_id=self.sal_stream_id)

    def recv_packet(self, packet, stream_route, stream_id):
        """
        Processes incoming data!!!!
        """
        if packet == {}:
            return

        log.debug("CTDBP L2 salinity transform received granule with record dict: %s", packet.record_dictionary)

        granule = CTDBP_SalinityTransformAlgorithm.execute(packet, params=self.stream_definition._id)

        log.debug("CTDBP L2 salinity transform publishing granule with record dict: %s", granule.record_dictionary)

        self.salinity.publish(msg=granule)


class CTDBP_SalinityTransformAlgorithm(SimpleGranuleTransformFunction):

    @staticmethod
    @SimpleGranuleTransformFunction.validate_inputs
    def execute(input=None, context=None, config=None, params=None, state=None):
        """
        Dependencies
        ------------
        CONDWAT_L1, TEMPWAT_L1, PRESWAT_L1


        Algorithms used
        ---------------
        PRACSAL = gsw_SP_from_C((CONDWAT_L1 * 10),TEMPWAT_L1,PRESWAT_L1)


        Reference
        ---------
        The calculations below are based on the following spreadsheet document:
        https://docs.google.com/spreadsheet/ccc?key=0Au7PUzWoCKU4dDRMeVI0RU9yY180Z0Y5U0hyMUZERmc#gid=0

        """

        rdt = RecordDictionaryTool.load_from_granule(input)
        out_rdt = RecordDictionaryTool(stream_definition_id=params)

        out_rdt['time'] = rdt['time']

        conductivity = rdt['conductivity']
        pressure = rdt['pressure']
        temperature = rdt['temp']
        sal_value = gsw.sp_from_c(conductivity * 10, temperature, pressure)

        log.debug("CTDBP Salinity algorithm calculated the sp (practical salinity) values: %s", sal_value)

        for key, value in rdt.iteritems():
            if key in out_rdt:
                if key=='conductivity' or key=='temp' or key=='pressure':
                    continue
                out_rdt[key] = value[:]

        out_rdt['salinity'] = sal_value

        return out_rdt.to_granule()

