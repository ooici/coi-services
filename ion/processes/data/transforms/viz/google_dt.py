
'''
@author Raj Singh
@file ion/processes/data/transforms/viz/google_dt.py
@description Convert CDM data to Google datatabbles
'''


from pyon.ion.transform import TransformFunction
from pyon.service.service import BaseService
from pyon.core.exception import BadRequest
from pyon.public import IonObject, RT, log

from ion.services.dm.utility.granule.granule import build_granule
from ion.services.dm.utility.granule.record_dictionary import RecordDictionaryTool

from coverage_model.parameter import ParameterDictionary, ParameterContext
from coverage_model.parameter_types import QuantityType
from coverage_model.basic_types import AxisTypeEnum
import numpy as np

from pyon.util.containers import get_safe
from pyon.ion.transforma import TransformDataProcess

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

    """


    def __init__(self):
        super(VizTransformGoogleDT, self).__init__()

        ### Parameter dictionaries
        self.define_parameter_dictionary()



    def recv_packet(self, packet, stream_route, stream_id):
        log.info('Received packet')
        for stream_id in self.CFG.get_safe('process.output_streams',[]):
            self.publisher.publish(self.execute(packet), stream_id=stream_id)
            log.info('Publishing on: %s', stream_id)


    def define_parameter_dictionary(self):
        """
        viz_product_type_ctxt = ParameterContext('viz_product_type', param_type=QuantityType(value_encoding=np.str))
        viz_product_type_ctxt.uom = 'unknown'
        viz_product_type_ctxt.fill_value = 0x0

        data_description_ctxt = ParameterContext('data_description', param_type=QuantityType(value_encoding=np.int8))
        data_description_ctxt.uom = 'unknown'
        data_description_ctxt.fill_value = 0x0

        data_content_ctxt = ParameterContext('data_content', param_type=QuantityType(value_encoding=np.int8))
        data_content_ctxt.uom = 'unknown'
        data_content_ctxt.fill_value = 0x0

        # Define the parameter dictionary objects
        self.gdt_paramdict = ParameterDictionary()
        self.gdt_paramdict.add_context(viz_product_type_ctxt)
        self.gdt_paramdict.add_context(data_description_ctxt)
        self.gdt_paramdict.add_context(data_content_ctxt)
        """

        gdt_components_ctxt = ParameterContext('google_dt_components', param_type=QuantityType(value_encoding=np.int8))
        gdt_components_ctxt.uom = 'unknown'
        gdt_components_ctxt.fill_value = 0x0

        # Define the parameter dictionary objects
        self.gdt_paramdict = ParameterDictionary()
        self.gdt_paramdict.add_context(gdt_components_ctxt)

    def execute(self, granule):

        log.debug('(Google DT transform): Received Viz Data Packet' )

        #init stuff
        varTuple = []
        dataDescription = []
        dataTableContent = []

        rdt = RecordDictionaryTool.load_from_granule(granule)

        vardict = {}
        vardict['time'] = get_safe(rdt, 'time')
        vardict['conductivity'] = get_safe(rdt, 'conductivity')
        vardict['pressure'] = get_safe(rdt, 'pressure')
        vardict['temperature'] = get_safe(rdt, 'temp')

        vardict['longitude'] = get_safe(rdt, 'lon')
        vardict['latitude'] = get_safe(rdt, 'lat')
        vardict['height'] = get_safe(rdt, 'height')
        arrLen = len(vardict['time'])  # Figure out how many values are present in the granule

        #iinit the dataTable
        # create data description from the variables in the message
        dataDescription = [('time', 'float', 'time')]

        # split the data string to extract variable names
        for varname in  vardict.keys():   #psd.list_field_names():
            if varname == 'time':
                continue

            dataDescription.append((varname, 'number', varname))

        # Add the records to the datatable
        for i in xrange(arrLen):
            varTuple = []

            for varname,_,_ in dataDescription:

                if vardict[varname] == None or len(vardict[varname]) == 0:
                    val = 0.0
                else:
                    val = float(vardict[varname][i])

                varTuple.append(val)

            # Append the tuples to the data table
            if len(varTuple) > 0:
                dataTableContent.append(varTuple)

        # Create output dictionary from the param dict
        out_rdt = RecordDictionaryTool(param_dictionary=self.gdt_paramdict)

        # Prepare granule content
        out_dict = {"viz_product_type" : "google_dt",
                    "data_description" : dataDescription,
                    "data_content" : dataTableContent}

        out_rdt["google_dt_components"] = np.array([out_dict])

        log.debug('Google DT transform: Sending a granule')

        out_granule = build_granule(data_producer_id='google_dt_transform', param_dictionary = self.gdt_paramdict, record_dictionary=out_rdt)
        return out_granule
