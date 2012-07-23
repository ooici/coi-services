
from pyon.ion.transform import TransformFunction
from pyon.service.service import BaseService
from pyon.core.exception import BadRequest
from pyon.public import IonObject, RT, log

import time
import numpy
from ion.services.dm.utility.granule.granule import build_granule
from ion.services.dm.utility.granule.taxonomy import TaxyTool
from ion.services.dm.utility.granule.record_dictionary import RecordDictionaryTool
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

    outgoing_stream_def = SBE37_RAW_stream_definition()
    incoming_stream_def = SBE37_CDM_stream_definition()

    def on_start(self):
        super(VizTransformMatplotlibGraphs,self).on_start()


    def execute(self, granule):
        log.debug('Matplotlib transform: Received Viz Data Packet')

        #init stuff
        self.out_granule = None

        # parse the incoming data
        rdt = RecordDictionaryTool.load_from_granule(granule)

        vardict = {}
        vardict['time'] = get_safe(rdt, 'time')
        vardict['conductivity'] = get_safe(rdt, 'conductivity')
        vardict['pressure'] = get_safe(rdt, 'pressure')
        vardict['temperature'] = get_safe(rdt, 'temperature')

        vardict['longitude'] = get_safe(rdt, 'longitude')
        vardict['latitude'] = get_safe(rdt, 'latitude')
        vardict['height'] = get_safe(rdt, 'height')
        arrLen = len(vardict['time'])

        # init the graph_data structure for storing values
        self.graph_data = {}
        for varname in vardict.keys():    #psd.list_field_names():
            self.graph_data[varname] = []


        # If code reached here, the graph data storage has been initialized. Just add values
        # to the list
        for varname in vardict.keys():  # psd.list_field_names():
            if vardict[varname] == None:
                # create an array of zeros to compensate for missing values
                self.graph_data[varname].extend([0.0]*arrLen)
            else:
                self.graph_data[varname].extend(vardict[varname])

        self.render_graphs()
        return self.out_granule


    def render_graphs(self):

        # init Matplotlib
        fig = Figure()
        ax = fig.add_subplot(111)
        canvas = FigureCanvas(fig)
        imgInMem = StringIO.StringIO()

        # If there's no data, wait
        # For the simple case of testing, lets plot all time variant variables one at a time
        xAxisVar = 'time'
        xAxisFloatData = self.graph_data[xAxisVar]
        rdt = RecordDictionaryTool(taxonomy=tx)
        msgs = []

        for varName, varData in self.graph_data.iteritems():
            if varName == 'time' or varName == 'height' or varName == 'longitude' or varName == 'latitude':
                continue

            yAxisVar = varName
            yAxisFloatData = self.graph_data[varName]

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

        rdt['matplotlib_graphs'] = numpy.array(msgs)
        #Generate a list of the graph objects generated
        self.out_granule = build_granule(data_producer_id='matplotlib_graphs_transform', taxonomy=tx, record_dictionary=rdt)


