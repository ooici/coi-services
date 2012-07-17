
'''
@author Raj Singh
@file ion/processes/data/transforms/viz/google_dt.py
@description Convert CDM data to Google datatabbles
'''


from pyon.ion.transform import TransformFunction
from pyon.service.service import BaseService
from pyon.core.exception import BadRequest
from pyon.public import IonObject, RT, log

from datetime import datetime
import numpy
from pyon.ion.granule.granule import build_granule
from pyon.ion.granule.taxonomy import TaxyTool
from pyon.ion.granule.record_dictionary import RecordDictionaryTool
from prototype.sci_data.stream_defs import SBE37_CDM_stream_definition, SBE37_RAW_stream_definition

from prototype.sci_data.stream_parser import PointSupplementStreamParser
from prototype.sci_data.constructor_apis import PointSupplementConstructor, RawSupplementConstructor

### For new granule and stream interface
from pyon.ion.granule.record_dictionary import RecordDictionaryTool
from pyon.ion.granule.taxonomy import TaxyTool
from pyon.ion.granule.granule import build_granule
from pyon.util.containers import get_safe

# Google viz library for google charts
import ion.services.ans.gviz_api as gviz_api

tx = TaxyTool()
tx.add_taxonomy_set('google_dt','Google datatable')


class VizTransformGoogleDT(TransformFunction):

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

    outgoing_stream_def = SBE37_RAW_stream_definition()
    #outgoing_stream_def = SBE37_CDM_stream_definition()
    incoming_stream_def = SBE37_CDM_stream_definition()

    def on_start(self):
        super(VizTransformGoogleDT,self).on_start()


    def execute(self, granule):

        log.debug('(Google DT transform): Received Viz Data Packet' )

        #init stuff
        # Need to move it to YAML or something
        #self.realtime_window_size = 100
        self.max_google_dt_len = 1024 # Remove this once the decimation has been moved in to the incoming dp

        self.varTuple = []
        self.total_num_of_records_recvd = 0
        self.dataDescription = []
        self.dataTableContent = []

        rdt = RecordDictionaryTool.load_from_granule(granule)

        vardict = {}
        vardict['time'] = get_safe(rdt, 'time')
        vardict['conductivity'] = get_safe(rdt, 'conductivity')
        vardict['pressure'] = get_safe(rdt, 'pressure')
        vardict['temperature'] = get_safe(rdt, 'temperature')

        vardict['longitude'] = get_safe(rdt, 'longitude')
        vardict['latitude'] = get_safe(rdt, 'latitude')
        vardict['height'] = get_safe(rdt, 'height')
        arrLen = len(vardict['time'])  # Figure out how many values are present in the granule

        #iinit the dataTable
        # create data description from the variables in the message
        self.dataDescription = [('time', 'datetime', 'time')]

        # split the data string to extract variable names
        for varname in  vardict.keys():   #psd.list_field_names():
            if varname == 'time':
                continue

            self.dataDescription.append((varname, 'number', varname))

        # Add the records to the datatable
        for i in xrange(arrLen):
            varTuple = []

            for varname,_,_ in self.dataDescription:

                if vardict[varname] == None:
                    val = 0.0
                else:
                    if varname == 'time':
                        val = datetime.fromtimestamp(vardict[varname][i])
                    else:
                        val = float(vardict[varname][i])

                varTuple.append(val)

            # Append the tuples to the data table
            self.dataTableContent.append (varTuple)


        # submit the partial datatable to the viz service
        out_rdt = RecordDictionaryTool(taxonomy=tx)

        # Use the components to create the actual google datatable
        gdt = gviz_api.DataTable(self.dataDescription)
        gdt.LoadData(self.dataTableContent)

        # submit resulting table back using the out stream publisher. The data_product_id is the input dp_id
        # responsible for the incoming data
        msg = {"viz_product_type": "google_dt",
               "data_table": gdt.ToJSonResponse()}

        out_rdt['google_dt'] = numpy.array([msg])

        log.debug('Google DT transform: Sending a granule')
        out_granule = build_granule(data_producer_id='google_dt_transform', taxonomy=tx, record_dictionary=out_rdt)

        # clear the tuple for future use
        self.varTuple[:] = []

        return out_granule
