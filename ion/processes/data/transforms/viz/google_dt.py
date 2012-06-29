
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

tx = TaxyTool()
tx.add_taxonomy_set('google_dt_components','Google DT components for part or entire datatable')


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

        # init variables
        self.varTuple = []
        self.total_num_of_records_recvd = 0

        # init config. Need to move it to YAML or something
        self.realtime_window_size = 100
        self.max_google_dt_len = 1024 # Remove this once the decimation has been moved in to the incoming dp

        # Note some transform parameters


    def execute(self, granule):

        log.debug('(Google DT transform): Received Viz Data Packet' )

        self.dataDescription = []
        self.dataTableContent = []
        element_count_id = 0
        expected_range = []

        # NOTE : Detect somehow that this is a replay stream with a set number of expected granules. Based on this
        #       calculate the number of expected records and set the self.realtime_window_size bigger or equal to this
        #       number.



#        psd = PointSupplementStreamParser(stream_definition=self.incoming_stream_def, stream_granule=granule)
#        vardict = {}
#        arrLen = None
#        for varname in psd.list_field_names():
#            vardict[varname] = psd.get_values(varname)
#            arrLen = len(vardict[varname])

        rdt = RecordDictionaryTool.load_from_granule(granule)
        rdt0 = rdt['coordinates']
        rdt1 = rdt['data']

        vardict = {}
        vardict['conductivity'] = get_safe(rdt1, 'cond')
        vardict['pressure'] = get_safe(rdt1, 'pres')
        vardict['temperature'] = get_safe(rdt1, 'temp')

        vardict['longitude'] = get_safe(rdt0, 'lon')
        vardict['latitude'] = get_safe(rdt0, 'lat')
        vardict['time'] = get_safe(rdt0, 'time')
        vardict['height'] = get_safe(rdt0, 'height')
        arrLen = len(vardict)

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
                val = float(vardict[varname][i])
                if varname == 'time':
                    #varTuple.append(datetime.fromtimestamp(val))
                    varTuple.append(val)
                else:
                    varTuple.append(val)

            # Append the tuples to the data table
            self.dataTableContent.append (varTuple)

            # Maintain a sliding window for realtime transform processes
            if len(self.dataTableContent) > self.realtime_window_size:
                # always pop the first element till window size is what we want
                while len(self.dataTableContent) > realtime_window_size:
                    self.dataTableContent.pop(0)


        """ To Do : Do we need to figure out the how many granules have been received for a replay stream ??

        if not self.realtime_flag:
            # This is the historical view part. Make a note of how many records were received
            in_data_stream_id = self.incoming_stream_def.data_stream_id
            element_count_id = self.incoming_stream_def.identifiables[in_data_stream_id].element_count_id
            # From each granule you can check the constraint on the number of records
            expected_range = granule.identifiables[element_count_id].constraint.intervals[0]

            # The number of records in a given packet is:
            self.total_num_of_records_recvd += packet.identifiables[element_count_id].value

        """

        # define an output container of data


        # submit the partial datatable to the viz service
        rdt = RecordDictionaryTool(taxonomy=tx)

        # submit resulting table back using the out stream publisher. The data_product_id is the input dp_id
        # responsible for the incoming data
        msg = {"viz_product_type": "google_realtime_dt",
               "data_product_id": "FAKE_DATAPRODUCT_ID_0000",
               "data_table_description": self.dataDescription,
               "data_table_content": self.dataTableContent}

        rdt['google_dt_components'] = numpy.array([msg])

        log.debug('Google DT transform: Sending a granule')
        out_granule = build_granule(data_producer_id='google_dt_transform', taxonomy=tx, record_dictionary=rdt)

        #self.publish(out_granule)

        # clear the tuple for future use
        self.varTuple[:] = []

        return out_granule
