
'''
@author Raj Singh
@file ion/processes/data/transforms/viz/google_dt.py
@description Convert CDM data to Google datatabbles
'''


from pyon.core.exception import BadRequest
from pyon.public import log


from ion.core.function.transform_function import SimpleGranuleTransformFunction
from ion.services.dm.utility.granule.record_dictionary import RecordDictionaryTool
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceProcessClient

import numpy as np
import ntplib

from ion.core.process.transform import TransformDataProcess


class VizTransformGoogleDT(TransformDataProcess):

    """
    This class is used for  converting incoming data from CDM format to JSON style Google DataTables

    Note: One behaviour that this class is expected to achieve specifically is to determine if its supposed
        to work as a realtime transform (exists indefinitely and maintains a sliding window of data) or as
        a replay transform (one-shot).

        [2] This transform behaves as an instantaneous forwarder. There is no waiting for the entire stream
            to create the complete datatable. As the granules come in, they are translated to the datatable
            'components'. Components, because we are not creating the actual datatable in this code. That's the job
            of the viz service to put all the components of a datatable together in JSON format before sending it
            to the client

        [3] The time stamp in the incoming stream can't be converted to the datetime object here because
            the Raw stream definition only expects regular primitives (strings, floats, ints etc)

    Usage: https://gist.github.com/3834918

    """

    output_bindings = ['google_dt_components']


    def __init__(self):
        super(VizTransformGoogleDT, self).__init__()

    def on_start(self):
        self.pubsub_management = PubsubManagementServiceProcessClient(process=self)

        self.stream_info  = self.CFG.get_safe('process.publish_streams', {})
        self.stream_names = self.stream_info.keys()
        self.stream_ids   = self.stream_info.values()
        if not self.stream_names:
            raise BadRequest('Google DT Transform has no output streams.')

        super(VizTransformGoogleDT,self).on_start()



    def recv_packet(self, packet, in_stream_route, in_stream_id):
        log.info('Received packet')
        outgoing = VizTransformGoogleDTAlgorithm.execute(packet, params=self.get_stream_definition())
        for stream_name in self.stream_names:
            publisher = getattr(self, stream_name)
            publisher.publish(outgoing)


    def get_stream_definition(self):
        stream_id = self.stream_ids[0]
        self.stream_def = self.pubsub_management.read_stream_definition(stream_id=stream_id)
        return self.stream_def._id


class VizTransformGoogleDTAlgorithm(SimpleGranuleTransformFunction):

    @staticmethod
    @SimpleGranuleTransformFunction.validate_inputs
    def execute(input=None, context=None, config=None, params=None, state=None):

        stream_definition_id = params

        #init stuff
        var_tuple = []
        data_description = []
        data_table_content = []
        gdt_allowed_numerical_types = ['int8', 'int16', 'int32', 'int64', 'uint8', 'uint16', 'uint32',
                                       'uint64', 'float32', 'float64','str']

        rdt = RecordDictionaryTool.load_from_granule(input)
        data_description = []

        if stream_definition_id == None:
            log.error("GoogleDT transform: Need a output stream definition to process graphs")
            return None

        fields = []
        if config and config['parameters']:
            fields = config['parameters']
        else:
            fields = rdt.fields

        # if time was null or missing, do not process
        if 'time' not in rdt: return None
        if rdt['time'] is None:
            return None

        time_fill_value = 0.0 # should be derived from the granule's param dict.
        data_description.append(('time','number','time'))

        for field in fields:
            if field == 'time':
                continue

            # If a config block was passed, consider only the params listed in it
            if config and config['parameters']:
                if not field in config['parameters']:
                    continue

            # only consider fields which are allowed.
            if rdt[field] == None:
                continue
            if (rdt[field] != None) and (rdt[field].dtype not in gdt_allowed_numerical_types):
                continue

            if (rdt[field].dtype == 'string'):
                data_description.append((field, 'string', field))
            else:
                data_description.append((field, 'number', field))

        for i in xrange(len(rdt)):
            varTuple = []

            # Put time first if its not zero. Retrieval returns 0 secs for malformed entries
            if rdt['time'][i] == time_fill_value:
                continue
            # convert timestamp from instrument to UNIX time stamp since thats what Google DT expects
            varTuple.append(ntplib.ntp_to_system_time(rdt['time'][i]))

            for dd in data_description:
                field = dd[0]
                # ignore time since its been already added
                if field == None or field == 'time':
                    continue

                if rdt[field] == None or rdt[field][i] == None:
                    varTuple.append(0.0)
                else:
                    varTuple.append(rdt[field][i])

            # Append the tuples to the data table
            if len(varTuple) > 0:
                data_table_content.append(varTuple)

        out_rdt = RecordDictionaryTool(stream_definition_id=stream_definition_id)

        # Prepare granule content
        out_dict = {"viz_product_type" : "google_dt",
                    "data_description" : data_description,
                    "data_content" : data_table_content}

        out_rdt["google_dt_components"] = np.array([out_dict])

        log.debug('Google DT transform: Sending a granule')

        out_granule = out_rdt.to_granule()

        return out_granule
