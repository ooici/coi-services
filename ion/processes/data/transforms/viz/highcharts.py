
'''
@author Raj Singh
@file ion/processes/data/transforms/viz/highcharts.py
@description Convert CDM data to Highcharts datasets used to populate the charts
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


class VizTransformHighCharts(TransformDataProcess):

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

    """

    def __init__(self):
        super(VizTransformHighCharts, self).__init__()

    def on_start(self):

        self.stream_info  = self.CFG.get_safe('process.publish_streams', {})
        self.stream_names = self.stream_info.keys()
        self.stream_ids   = self.stream_info.values()
        if not self.stream_names or not self.stream_ids:
            raise BadRequest('HighCharts Transform: No output streams.')

        self.pubsub_management = PubsubManagementServiceProcessClient(process=self)
        self.stream_def = self.pubsub_management.read_stream_definition(stream_id=self.stream_ids[0])
        super(VizTransformHighCharts,self).on_start()



    def recv_packet(self, packet, in_stream_route, in_stream_id):
        log.info('HighCharts Transform: Received packet')
        outgoing = VizTransformHighChartsAlgorithm.execute(packet, params=self.stream_def._id)
        for stream_name in self.stream_names:
            publisher = getattr(self, stream_name)
            publisher.publish(outgoing)


class VizTransformHighChartsAlgorithm(SimpleGranuleTransformFunction):

    @staticmethod
    @SimpleGranuleTransformFunction.validate_inputs
    def execute(input=None, context=None, config=None, params=None, state=None):

        stream_definition_id = params

        #init stuff
        rdt_for_nones = {}
        hc_data = []
        normalized_ts = []
        hc_allowed_numerical_types = ['int8', 'int16', 'int32', 'int64', 'uint8', 'uint16', 'uint32',
                                       'uint64', 'float32', 'float64','str']
        # TODO : Move this in to container parameter
        default_precision = 5

        rdt = RecordDictionaryTool.load_from_granule(input)
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
            log.error("HighCharts transform: Need a output stream definition to process graphs")
            return None

        fields = []
        fields = rdt.fields

        # Ascertain temporal field. Use 'time' as backup
        time_field = rdt.temporal_parameter or 'time'
        #if 'time' not in rdt: return None
        if rdt[time_field] is None:
            return None

        #time_fill_value = 0.0 # should be derived from the granule's param dict.
        time_fill_value = fill_values[time_field] # should be derived from the granule's param dict.
        total_num_of_records = len(rdt[time_field])

        # convert timestamps from ntp to system
        count = 0
        normalized_ts = [None] * total_num_of_records
        for ts in rdt[time_field]:
            if ts == time_fill_value:
                normalized_ts[count] = time_fill_value
            else:
                normalized_ts[count] = float(ntplib.ntp_to_system_time(ts))
            count += 1

        ###### DEBUG ##########
        #for field in fields:
        #    if hasattr(rdt.context(field),'visible'):
        #        print "  >>>>>>>> '", field, "' [visible = ", rdt.context(field).visible,"] : ", rdt[field]
        #    else:
        #        print "  >>>>>>>> '", field, "' [visible = NOT SPECIFIED] : ", rdt[field]

        # Convert the fields in to HC series format
        import re
        for field in fields:
            field_precision = precisions[field]

            if field == time_field:
                continue

            # If a config block was passed, consider only the params listed in it
            if config and 'parameters' in config and len(config['parameters']) > 0:
                if not field in config['parameters']:
                    log.info("Skipping ", field, " since it was not present in the list of allowed parameters")
                    continue

            # If the value is none, assign it a small one fill_value array for now to generate description,
            # Actual array of fill_values will be assigned later
            rdt_field =rdt[field]
            if rdt_field == None:
                rdt_for_nones[field] = np.array([fill_values[field]] * total_num_of_records)
                #rdt_for_nones[field] = [fill_values[field]] * total_num_of_records
                rdt_field = rdt_for_nones[field]

            # Check if visibility is false (system generated params) or not specified explicitly
            if hasattr(rdt.context(field),'visible') and not rdt.context(field).visible:
                continue

            # If it's a QC parameter ignore it
            if field.endswith('_qc'):
                continue

            # NOTE: The reason why the following libes of code seem to make a lot of branches are to pull the major decisions
            #       outside the primary loops responsible for arranging data in the data structures to be sent to the charts.
            #       This is better than having to make the decisions on a per record entry.

            # Handle arrays by spliting them them in to individual parameters
            context = rdt.context(field)
            if (isinstance(context.param_type, ArrayType) or isinstance(context.param_type,ParameterFunctionType)) and len(rdt_field.shape)>1:
                if (rdt_field.dtype == 'string' or rdt_field.dtype not in hc_allowed_numerical_types):
                    for i in xrange(rdt_field.shape[1]):
                        series = {}
                        series["name"] = field + "[" + str(i) + "]"
                        series["visible"] = False
                        series["data"] = VizTransformHighChartsAlgorithm.form_series_data_str(normalized_ts, rdt_field, i, time_fill_value, fill_values[field])
                        hc_data.append(series)

                else: # Assume its a float or number
                    for i in xrange(rdt_field.shape[1]):
                        series = {}
                        series["name"] = field + "[" + str(i) + "]"
                        series["visible"] = True
                        series["tooltip"] = {"valueDecimals":field_precision}
                        series["data"] = VizTransformHighChartsAlgorithm.form_series_data_num(normalized_ts, rdt_field, i, time_fill_value, fill_values[field], field_precision)
                        hc_data.append(series)
            else:
                if (rdt_field.dtype == 'string' or rdt_field.dtype not in hc_allowed_numerical_types):
                    series = {}
                    series["name"] = field
                    series["visible"] = False
                    series["data"] = VizTransformHighChartsAlgorithm.form_series_data_str(normalized_ts, rdt_field, None, time_fill_value, fill_values[field])
                    hc_data.append(series)

                else:
                    series = {}
                    series["name"] = field
                    series["tooltip"] = {"valueDecimals":field_precision}
                    series["visible"] = True
                    series["data"] = VizTransformHighChartsAlgorithm.form_series_data_num(normalized_ts, rdt_field, None, time_fill_value, fill_values[field], field_precision)

                # Append series to the hc data
                hc_data.append(series)

        # Prep the outgoing granule
        out_rdt = RecordDictionaryTool(stream_definition_id=stream_definition_id)

        #out_dict = {"hc_data": hc_data}
        #out_rdt["hc_data"] = np.array([out_dict])

        out_rdt["hc_data"] = [hc_data]
        out_rdt["viz_timestamp"] = TimeUtils.ts_to_units(rdt.context(time_field).uom, time.time())

        log.debug('HighCharts Transform: Sending a granule')
        out_granule = out_rdt.to_granule()

        return out_granule


    @staticmethod
    def form_series_data_num(timestamps, val, idx, ts_fill_value, val_fill_value,  precision):
        num_of_recs = len(timestamps)
        _data = np.array([None] * num_of_recs)

        if idx != None:
            for i in xrange(num_of_recs):
                if timestamps[i] == ts_fill_value:
                    continue
                if val[i][idx] == None or val[i][idx] == val_fill_value:
                    _data[i] = [timestamps[i] * 1000, None]
                else:
                    _data[i] = [timestamps[i] * 1000, round(float(val[i][idx]), precision)]
        else:
            for i in xrange(num_of_recs):
                if timestamps[i] == ts_fill_value:
                    continue
                if val[i] == None or val[i] == val_fill_value:
                    _data[i] = [timestamps[i] * 1000, None]
                else:
                    _data[i] = [timestamps[i] * 1000, round(float(val[i]), precision)]

        return _data


    @staticmethod
    def form_series_data_str(timestamps, val, idx, ts_fill_value, val_fill_value):

        num_of_recs = len(timestamps)
        _data = np.array([None] * num_of_recs)

        if idx != None:
            for i in xrange(num_of_recs):
                if timestamps[i] == ts_fill_value:
                    continue
                if not val[i][idx] or val[i][idx] == val_fill_value:
                    _data[i] = [timestamps[i] * 1000, None]
                else:
                    _data[i] = [timestamps[i] * 1000, str(val[i][idx])]
        else:
            for i in xrange(num_of_recs):
                if timestamps[i] == ts_fill_value:
                    continue
                if not val[i] or val[i] == val_fill_value:
                    _data[i] = [timestamps[i] * 1000, None]
                else:
                    _data[i] = [timestamps[i] * 1000, str(val[i])]

        return _data