
from pyon.ion.transform import TransformFunction
from pyon.service.service import BaseService
from pyon.core.exception import BadRequest
from pyon.public import IonObject, RT, log

import time
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

    outgoing_stream_def = SBE37_RAW_stream_definition()
    incoming_stream_def = SBE37_CDM_stream_definition()

    def on_start(self):
        super(VizTransformMatplotlibGraphs,self).on_start()

        self.initDataFlag = True
        self.out_granule = None
        self.graph_data = {} # Stores a dictionary of variables : [List of values]
        self.lastRenderTime = 0
        self.renderTimeThreshold = 10 # if two consecutive calls to renderGraphs() was done within this time
                                    # interval, it would be ignored (in seconds)



    def execute(self, granule):
        log.debug('Matplotlib transform: Received Viz Data Packet')

        # parse the incoming data
#        psd = PointSupplementStreamParser(stream_definition=self.incoming_stream_def, stream_granule=granule)
#
#        # re-arrange incoming data into an easy to parse dictionary
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

        if self.initDataFlag:
            # look at the incoming packet and store
            for varname in vardict.keys():    #psd.list_field_names():
                self.graph_data[varname] = []

            self.initDataFlag = False

        # If code reached here, the graph data storage has been initialized. Just add values
        # to the list
        for varname in vardict.keys():  # psd.list_field_names():
            self.graph_data[varname].extend(vardict[varname])

        if (time.time() - self.lastRenderTime) > self.renderTimeThreshold:
            self.lastRenderTime = time.time()
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
        rdt['matplotlib_graphs'] = array([])

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
                   "data_product_id": "FAKE_DATAPRODUCT_ID_0001",
                   "image_obj": imgInMem.getvalue(),
                   "image_name": fileName}

            append(rdt['matplotlib_graphs'], msg)

            #clear the canvas for the next image
            ax.clear()

        #Generate a list of the graph objects generated
        self.out_granule = build_granule(data_producer_id='matplotlib_graphs_transform', taxonomy=tx, record_dictionary=rdt)


