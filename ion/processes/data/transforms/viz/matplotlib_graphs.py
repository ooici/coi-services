
from ion.core.function.transform_function import SimpleGranuleTransformFunction
from pyon.core.exception import BadRequest
from pyon.public import log

from ion.services.dm.utility.granule.record_dictionary import RecordDictionaryTool
from pyon.util.containers import get_safe

import numpy as np

import StringIO
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceProcessClient
from ion.core.process.transform import TransformDataProcess, TransformEventListener
from interface.services.cei.ischeduler_service import SchedulerServiceProcessClient
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceProcessClient
from pyon.event.event import EventSubscriber

# Matplotlib related imports
# Need try/catch because of weird import error
try:
    from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
    from matplotlib.figure import Figure
except:
    import sys
    print >> sys.stderr, "Cannot import matplotlib"


class VizTransformMatplotlibGraphs(TransformDataProcess, TransformEventListener):

    """
    This class is used for instantiating worker processes that have subscriptions to data streams and convert
    incoming data from CDM format to Matplotlib graphs

    """
    output_bindings = ['graph_image_param_dict']


    def on_start(self):

        self.pubsub_management = PubsubManagementServiceProcessClient(process=self)
        self.ssclient = SchedulerServiceProcessClient(node=self.container.node, process=self)
        self.rrclient = ResourceRegistryServiceProcessClient(node=self.container.node, process=self)

        self.stream_info  = self.CFG.get_safe('process.publish_streams',{})
        self.stream_names = self.stream_info.keys()
        self.stream_ids   = self.stream_info.values()

        if not self.stream_names:
            raise BadRequest('MPL Transform has no output streams.')

        graph_time_periods= self.CFG.get_safe('graph_time_periods')
        print ">>>>>>>>>>>>>>>>>>>>> CFG.graph_time_periods = ", graph_time_periods

        # If this is meant to be an event driven process, schedule an event to be generated every few minutes/hours
        event_timer_interval = self.CFG.get_safe('event_timer_interval')
        if event_timer_interval:
            event_origin = "Interval_Timer_Matplotlib"
            sub = EventSubscriber(event_type="ResourceEvent", callback=self.interval_timer_callback, origin=event_origin)
            sub.start()

            self.interval_timer_id = self.ssclient.create_interval_timer(start_time="now" , interval=event_timer_interval,
                event_origin=event_origin, event_subtype="")

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

    def process_event(self, msg, headers):

        return

    def interval_timer_callback(self, *args, **kwargs):

        print " >>>>>>>>>>>>>>>>>>> EVENT GENERATED <<<<<<<<<<<<<<<<"
        # retrieve data for every case of the output graph
        return

    def on_quit(self):

        #Cancel the timer
        if hasattr(self, 'interval_timer_id'):
            self.ssclient.cancel_timer(self.interval_timer_id)

        super(VizTransformMatplotlibGraphs,self).on_quit()


class VizTransformMatplotlibGraphsAlgorithm(SimpleGranuleTransformFunction):
    @staticmethod
    @SimpleGranuleTransformFunction.validate_inputs
    def execute(input=None, context=None, config=None, params=None, state=None):
        log.debug('Matplotlib transform: Received Viz Data Packet')
        stream_definition_id = params

        # parse the incoming data
        rdt = RecordDictionaryTool.load_from_granule(input)

        # build a list of fields/variables that need to be plotted. Use the list provided by the UI
        # since the retrieved granule might have extra fields. Why ? Ans : Bugs, baby, bugs !
        fields = []
        if config:
            if config['parameters']:
                fields = config['parameters']
        else:
            fields = rdt.fields

        vardict = {}
        vardict['time'] = get_safe(rdt, 'time')
        if vardict['time'] == None:
            log.error("Matplotlib transform: Did not receive a time field to work with")
            return None

        for field in fields:
            if field == 'time':
                continue

            vardict[field] = get_safe(rdt, field)

            print

        arrLen = len(vardict['time'])
        # init the graph_data structure for storing values
        graph_data = {}
        for varname in vardict.keys():
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
