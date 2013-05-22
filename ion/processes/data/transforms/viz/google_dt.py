
'''
@author Raj Singh
@file ion/processes/data/transforms/viz/google_dt.py
@description Convert CDM data to Google datatabbles
'''


from pyon.core.exception import BadRequest, Timeout
from pyon.public import log

from ion.core.function.transform_function import SimpleGranuleTransformFunction
from ion.services.dm.utility.granule.record_dictionary import RecordDictionaryTool
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceProcessClient
from coverage_model import ArrayType, ParameterFunctionType

import numpy as np
import ntplib
import time
from pyon.util.containers import get_ion_ts
from ion.util.time_utils import TimeUtils

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

        self.stream_info  = self.CFG.get_safe('process.publish_streams', {})
        self.stream_names = self.stream_info.keys()
        self.stream_ids   = self.stream_info.values()
        if not self.stream_names or not self.stream_ids:
            raise BadRequest('Google DT Transform has no output streams.')

        self.pubsub_management = PubsubManagementServiceProcessClient(process=self)
        self.stream_def = self.pubsub_management.read_stream_definition(stream_id=self.stream_ids[0])
        super(VizTransformGoogleDT,self).on_start()



    def recv_packet(self, packet, in_stream_route, in_stream_id):
        log.info('Received packet')
        outgoing = VizTransformGoogleDTAlgorithm.execute(packet, params=self.stream_def._id)
        for stream_name in self.stream_names:
            publisher = getattr(self, stream_name)
            publisher.publish(outgoing)


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
        # TODO : Move this in to container parameter
        default_precision = 5

        rdt = RecordDictionaryTool.load_from_granule(input)


        data_description = []
        # Buid a local precisions and fill value dictionary to use for parsing data correctly

        precisions = {}
        fill_values = {}
        for field in rdt.fields:
            precision_str = rdt.context(field).precision
            if not precision_str:
                precisions[field] = default_precision
            else:
                try:
                    precisions[field] = int(precision_str)
                except ValueError:
                    precisions[field] = default_precision


            fill_values[field] = rdt.fill_value(field)

        if stream_definition_id == None:
            log.error("GoogleDT transform: Need a output stream definition to process graphs")
            return None

        fields = []
        fields = rdt.fields


        # if time was null or missing, do not process
        if 'time' not in rdt: return None
        if rdt['time'] is None:
            return None

        time_fill_value = 0.0 # should be derived from the granule's param dict.
        data_description.append(('time','number','time'))

        import re
        for field in fields:

            if field == rdt.temporal_parameter:
                continue

            # If a config block was passed, consider only the params listed in it
            if config and 'parameters' in config and len(config['parameters']) > 0:
                if not field in config['parameters']:
                    continue

            # only consider fields which are allowed.
            if rdt[field] == None:
                continue

            # Check if visibility is false (system generated params)
            if hasattr(rdt.context(field),'visible') and not rdt.context(field).visible:
                continue

            # If it's a QC parameter ignore it
            if field.endswith('_qc'):
                continue

            # Handle string type or if its an unknown type, convert to string
            context = rdt.context(field)
            if (rdt[field].dtype == 'string' or rdt[field].dtype not in gdt_allowed_numerical_types):
                data_description.append((field, 'string', field ))
            elif (isinstance(context.param_type, ArrayType) or isinstance(context.param_type,ParameterFunctionType)) and len(rdt[field].shape)>1:
                for i in xrange(rdt[field].shape[1]):
                    data_description.append(('%s[%s]' % (field,i), 'number', '%s[%s]' % (field,i), {'precision':str(precisions[field])}))
            else:
                data_description.append((field, 'number', field, {'precision':str(precisions[field])} ))

        for i in xrange(len(rdt)):
            varTuple = []

            # Put time first if its not zero. Retrieval returns 0 secs for malformed entries
            if rdt['time'][i] == time_fill_value:
                continue
            # convert timestamp from instrument to UNIX time stamp since thats what Google DT expects
            varTuple.append(ntplib.ntp_to_system_time(rdt['time'][i]))

            for dd in data_description:
                field = dd[0]
                field_type = dd[1]
                # ignore time since its been already added
                if field == None or field == 'time':
                    continue

                if re.match(r'.*\[\d+\]', field):
                    field, j = re.match(r'(.*)\[(\d+)\]', field).groups()
                    j = int(j)
                    varTuple.append(float(rdt[field][i][j]))
                elif rdt[field] == None or rdt[field][i] == None:
                    varTuple.append(None)
                else:
                    if(field_type == 'number'):
                        if rdt[field][i] == None or rdt[field][i] == fill_values[field]:
                            varTuple.append(None)
                        else:
                            # Adjust float for precision
                            if (precisions[field] == None):
                                varTuple.append(float(rdt[field][i]))
                            else:
                                varTuple.append(round(float(rdt[field][i]), precisions[field]))

                    # if field type is string, there are two possibilities. Either it really is a string or
                    # its an object that needs to be converted to string.
                    if(field_type == 'string'):
                        varTuple.append(str(rdt[field][i]))

            # Append the tuples to the data table
            if len(varTuple) > 0:
                data_table_content.append(varTuple)

        out_rdt = RecordDictionaryTool(stream_definition_id=stream_definition_id)

        # Prepare granule content
        out_dict = {"viz_product_type" : "google_dt",
                    "data_description" : data_description,
                    "data_content" : data_table_content}

        out_rdt["google_dt_components"] = np.array([out_dict])
        out_rdt["viz_timestamp"] = TimeUtils.ts_to_units(rdt.context(rdt.temporal_parameter).uom, time.time())

        log.debug('Google DT transform: Sending a granule')
        out_granule = out_rdt.to_granule()

        return out_granule
