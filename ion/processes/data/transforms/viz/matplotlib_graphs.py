
from pyon.ion.transform import TransformFunction
from pyon.service.service import BaseService
from pyon.core.exception import BadRequest
from pyon.public import IonObject, RT, log

import time
import numpy
from pyon.ion.granule.granule import build_granule
from pyon.ion.granule.taxonomy import TaxyTool
from pyon.ion.granule.record_dictionary import RecordDictionaryTool
from pyon.util.containers import get_safe
from prototype.sci_data.stream_defs import SBE37_CDM_stream_definition, SBE37_RAW_stream_definition

from prototype.sci_data.stream_parser import PointSupplementStreamParser
from prototype.sci_data.constructor_apis import PointSupplementConstructor, RawSupplementConstructor

import StringIO
from numpy import array, append

# Matplotlib related imports
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
from matplotlib.figure import Figure

tx = TaxyTool()
tx.add_taxonomy_set('matplotlib_graphs','Matplotlib generated graphs for a particular data product')

class VizTransformMatplotlibGraphs(TransformFunction):

    """
    This class is used for instantiating worker processes that have subscriptions to data streams and convert
    incoming data from CDM format to Matplotlib graphs

    """

    # Need to extract the incoming stream def automatically
    outgoing_stream_def = SBE37_RAW_stream_definition()
    incoming_stream_def = SBE37_CDM_stream_definition()

    def on_start(self):
        super(VizTransformMatplotlibGraphs,self).on_start()


    def execute(self, granule):
        log.debug('Matplotlib transform: Received Viz Data Packet')

        #init stuff

        # parse the incoming data
        rdt = RecordDictionaryTool.load_from_granule(granule)

        vardict = {}
        vardict['time'] = get_safe(rdt, 'time')
        vardict['conductivity'] = get_safe(rdt, 'cond')
        vardict['pressure'] = get_safe(rdt, 'pres')
        vardict['temperature'] = get_safe(rdt, 'temp')

        vardict['longitude'] = get_safe(rdt, 'long')
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

        out_granule = self.render_graphs(graph_data)

        return out_granule

    def render_graphs(self, graph_data):

        # init Matplotlib
        fig = Figure()
        ax = fig.add_subplot(111)
        canvas = FigureCanvas(fig)
        imgInMem = StringIO.StringIO()

        # If there's no data, wait
        # For the simple case of testing, lets plot all time variant variables one at a time
        xAxisVar = 'time'
        xAxisFloatData = graph_data[xAxisVar]
        rdt = RecordDictionaryTool(taxonomy=tx)
        msgs = []

        for varName, varData in graph_data.iteritems():
            if varName == 'time' or varName == 'height' or varName == 'longitude' or varName == 'latitude':
                continue

            yAxisVar = varName
            yAxisFloatData = graph_data[varName]

            # Generate the plot
            ax.plot(xAxisFloatData, yAxisFloatData, 'ro')
            ax.set_xlabel(xAxisVar)
            ax.set_ylabel(yAxisVar)
            ax.set_title(yAxisVar + ' vs ' + xAxisVar)
            ax.set_autoscale_on(False)

            # generate filename for the output image
            fileName = yAxisVar + '_vs_' + xAxisVar + '.png'
            # Save the figure to the in memory file
            canvas.print_figure(imgInMem, format="png")
            imgInMem.seek(0)

            # submit resulting table back using the out stream publisher
            msg = {"viz_product_type": "matplotlib_graphs",
                   "image_obj": imgInMem.getvalue(),
                   "image_name": fileName}

            msgs.append(msg)

            #clear the canvas for the next image
            ax.clear()

        rdt['matplotlib_graphs'] = numpy.array([msgs])
        #Generate a list of the graph objects generated
        return build_granule(data_producer_id='matplotlib_graphs_transform', taxonomy=tx, record_dictionary=rdt)


