
from ion.core.process.transform import TransformStreamListener
from ion.core.function.transform_function import SimpleGranuleTransformFunction
from pyon.service.service import BaseService
from pyon.ion.streamproc import StreamProcess
from pyon.core.exception import BadRequest
from pyon.public import IonObject, RT, log

import time
import numpy
from ion.services.dm.utility.granule.record_dictionary import RecordDictionaryTool
from pyon.util.containers import get_safe
from prototype.sci_data.stream_defs import SBE37_CDM_stream_definition, SBE37_RAW_stream_definition

from coverage_model.parameter import ParameterDictionary, ParameterContext
from coverage_model.parameter_types import QuantityType
from coverage_model.basic_types import AxisTypeEnum
import numpy as np

import StringIO
from numpy import array, append

# Matplotlib related imports
# Need try/catch because of weird import error
try:
    from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
    from matplotlib.figure import Figure
except:
    import sys
    print >> sys.stderr, "Cannot import matplotlib"

class VizTransformMatplotlibGraphs(TransformStreamListener):

    """
    This class is used for instantiating worker processes that have subscriptions to data streams and convert
    incoming data from CDM format to Matplotlib graphs

    """

    # Need to extract the incoming stream def automatically
    outgoing_stream_def = SBE37_RAW_stream_definition()
    incoming_stream_def = SBE37_CDM_stream_definition()

    def __init__(self):
        super(VizTransformMatplotlibGraphs, self).__init__()

        ### Parameter dictionaries
        #self.define_parameter_dictionary()

    def recv_packet(self, msg, stream_route, stream_id):
        pass

class VizTransformMatplotlibGraphsFunction(SimpleGranuleTransformFunction):

    @staticmethod
    def define_parameter_dictionary():

        """
        viz_product_type_ctxt = ParameterContext('viz_product_type', param_type=QuantityType(value_encoding=np.str))
        viz_product_type_ctxt.uom = 'unknown'
        viz_product_type_ctxt.fill_value = 0x0

        image_obj_ctxt = ParameterContext('image_obj', param_type=QuantityType(value_encoding=np.int8))
        image_obj_ctxt.uom = 'unknown'
        image_obj_ctxt.fill_value = 0x0

        image_name_ctxt = ParameterContext('image_name', param_type=QuantityType(value_encoding=np.str))
        image_name_ctxt.uom = 'unknown'
        image_name_ctxt.fill_value = 0x0

        content_type_ctxt = ParameterContext('content_type', param_type=QuantityType(value_encoding=np.str))
        content_type_ctxt.uom = 'unknown'
        content_type_ctxt.fill_value = 0x0

        # Define the parameter dictionary objects
        self.mpl_paramdict = ParameterDictionary()
        self.mpl_paramdict.add_context(viz_product_type_ctxt)
        self.mpl_paramdict.add_context(image_obj_ctxt)
        self.mpl_paramdict.add_context(image_name_ctxt)
        self.mpl_paramdict.add_context(content_type_ctxt)
        """

        mpl_graph_ctxt = ParameterContext('mpl_graph', param_type=QuantityType(value_encoding=np.int8))
        mpl_graph_ctxt.uom = 'unknown'
        mpl_graph_ctxt.fill_value = 0x0

        # Define the parameter dictionary objects
        mpl_paramdict = ParameterDictionary()
        mpl_paramdict.add_context(mpl_graph_ctxt)
        return mpl_paramdict


    @staticmethod
    @SimpleGranuleTransformFunction.validate_inputs
    def execute(input=None, context=None, config=None, params=None, state=None):
        log.debug('Matplotlib transform: Received Viz Data Packet')

        #init stuff

        # parse the incoming data
        rdt = RecordDictionaryTool.load_from_granule(input)

        vardict = {}
        vardict['time'] = get_safe(rdt, 'time')
        vardict['conductivity'] = get_safe(rdt, 'conductivity')
        vardict['pressure'] = get_safe(rdt, 'pressure')
        vardict['temperature'] = get_safe(rdt, 'temp')

        vardict['longitude'] = get_safe(rdt, 'lon')
        vardict['latitude'] = get_safe(rdt, 'lat')
        vardict['height'] = get_safe(rdt, 'height')
        arrLen = len(vardict['time'])

        # init the graph_data structure for storing values
        graph_data = {}
        for varname in vardict.keys():    #psd.list_field_names():
            graph_data[varname] = []


        # If code reached here, the graph data storage has been initialized. Just add values
        # to the list
        for varname in vardict.keys():  # psd.list_field_names():
            if vardict[varname] == None:
                # create an array of zeros to compensate for missing values
                graph_data[varname].extend([0.0]*arrLen)
            else:
                graph_data[varname].extend(vardict[varname])

        out_granule = VizTransformMatplotlibGraphs.render_graphs(graph_data)

        return out_granule

    @staticmethod
    def render_graphs(graph_data):

        # init Matplotlib
        fig = Figure(figsize=(8,4), dpi=200, frameon=True)
        ax = fig.add_subplot(111)
        canvas = FigureCanvas(fig)
        imgInMem = StringIO.StringIO()

        # If there's no data, wait
        # For the simple case of testing, lets plot all time variant variables one at a time
        xAxisVar = 'time'
        xAxisFloatData = graph_data[xAxisVar]

        # Prepare the set of y axis variables that will be plotted. This needs to be smarter and passed as
        # config variable to the transform
        yAxisVars = []
        for varName, varData in graph_data.iteritems():
            if varName == 'time' or varName == 'height' or varName == 'longitude' or varName == 'latitude':
                continue
            yAxisVars.append(varName)

        idx = 0
        for varName in yAxisVars:
            yAxisFloatData = graph_data[varName]

            # Generate the plot
            ax.plot(xAxisFloatData, yAxisFloatData, VizTransformMatplotlibGraphs.line_style(idx), label=varName)
            idx += 1

        yAxisLabel = ""
        # generate a filename for the output image
        for varName in yAxisVars:
            if yAxisLabel:
                yAxisLabel = yAxisLabel + "-" + varName
            else:
                yAxisLabel = varName

        fileName = yAxisLabel + '_vs_' + xAxisVar + '.png'

        ax.set_xlabel(xAxisVar)
        ax.set_ylabel(yAxisLabel)
        ax.set_title(yAxisLabel + ' vs ' + xAxisVar)
        ax.set_autoscale_on(False)
        ax.legend(loc='upper left')

        # Save the figure to the in memory file
        canvas.print_figure(imgInMem, format="png")
        imgInMem.seek(0)

        # Create output dictionary from the param dict
        out_rdt = RecordDictionaryTool(param_dictionary=VizTransformMatplotlibGraphs.define_parameter_dictionary())

        # Prepare granule content
        out_dict = {}
        out_dict["viz_product_type"] = "matplotlib_graphs"
        out_dict["image_obj"] = imgInMem.getvalue()
        out_dict["image_name"] = fileName
        out_dict["content_type"] = "image/png"

        out_rdt["mpl_graph"] = np.array([out_dict])
        return out_rdt.to_granule()


    # This method picks out a matplotlib line style based on an index provided. These styles are set in an order
    # as a utility. No other reason
    @staticmethod
    def line_style(index):

        color = ['b','g','r','c','m','y','k','w']
        stroke = ['-','--','-.',':','.',',','o','+','x','*']

        style = color[index % len(color)] + stroke [(index / len(color)) % len(stroke)]

        return style
