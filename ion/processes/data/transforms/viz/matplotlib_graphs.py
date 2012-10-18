
from ion.core.function.transform_function import SimpleGranuleTransformFunction
from pyon.core.exception import BadRequest
from pyon.public import log

from ion.services.dm.utility.granule.record_dictionary import RecordDictionaryTool
from pyon.util.containers import get_safe

import numpy as np

import StringIO
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceProcessClient
from pyon.ion.transforma import TransformDataProcess

# Matplotlib related imports
# Need try/catch because of weird import error
try:
    from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
    from matplotlib.figure import Figure
except:
    import sys
    print >> sys.stderr, "Cannot import matplotlib"


class VizTransformMatplotlibGraphs(TransformDataProcess):

    """
    This class is used for instantiating worker processes that have subscriptions to data streams and convert
    incoming data from CDM format to Matplotlib graphs

    """
    output_bindings = ['graph']


    def on_start(self):

        self.pubsub_management = PubsubManagementServiceProcessClient(process=self)

        self.stream_info  = self.CFG.get_safe('process.publish_streams',{})
        self.stream_names = self.stream_info.keys()
        self.stream_ids   = self.stream_info.values()

        if not self.stream_names:
            raise BadRequest('MPL Transform has no output streams.')


        super(VizTransformMatplotlibGraphs,self).on_start()

    def recv_packet(self, packet, in_stream_route, in_stream_id):
        log.info('Received packet')
        outgoing = VizTransformMatplotlibGraphsAlgorithm.execute(packet, params=self.get_stream_definition())
        for stream_name in self.stream_names:
            publisher = getattr(self, stream_name)
            publisher.publish(outgoing)

    def get_stream_definition(self):
        stream_id = self.stream_ids[0]
        stream_def = self.pubsub_management.read_stream_definition(stream_id=stream_id)
        return stream_def._id


class VizTransformMatplotlibGraphsAlgorithm(SimpleGranuleTransformFunction):
    @staticmethod
    @SimpleGranuleTransformFunction.validate_inputs
    def execute(input=None, context=None, config=None, params=None, state=None):
        log.debug('Matplotlib transform: Received Viz Data Packet')
        stream_definition_id = params

        #Note config parameters

        # parse the incoming data
        rdt = RecordDictionaryTool.load_from_granule(input)

        vardict = {}
        vardict['time'] = get_safe(rdt, 'time')

        for field in rdt.fields:
            if field == 'time':
                continue

            vardict[field] = get_safe(rdt, field)

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

        out_granule = VizTransformMatplotlibGraphsAlgorithm.render_graphs(graph_data, stream_definition_id)

        return out_granule

    @classmethod
    def render_graphs(cls, graph_data, stream_definition_id):

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
            ax.plot(xAxisFloatData, yAxisFloatData, cls.line_style(idx), label=varName)
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
        out_rdt = RecordDictionaryTool(stream_definition_id=stream_definition_id)

        # Prepare granule content
        #out_dict = {}
        out_rdt["viz_product_type"] = ["matplotlib_graphs"]
        out_rdt["image_obj"] = [imgInMem.getvalue()]
        out_rdt["image_name"] = [fileName]
        out_rdt["content_type"] = ["image/png"]

        #out_rdt["graph_image_param_dict"] = np.array([out_dict])
        return out_rdt.to_granule()


    # This method picks out a matplotlib line style based on an index provided. These styles are set in an order
    # as a utility. No other reason
    @classmethod
    def line_style(cls, index):

        color = ['b','g','r','c','m','y','k']
        stroke = ['-','--','-.',':','.',',','o','+','x','*']

        style = color[index % len(color)] + stroke [(index / len(color)) % len(stroke)]

        return style
