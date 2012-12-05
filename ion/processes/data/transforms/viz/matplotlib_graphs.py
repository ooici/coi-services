
from ion.core.function.transform_function import SimpleGranuleTransformFunction
from pyon.core.exception import BadRequest
from pyon.public import log, PRED, RT

from ion.services.dm.utility.granule.record_dictionary import RecordDictionaryTool
from pyon.util.containers import get_safe

import numpy as np
import ntplib
from datetime import datetime

import StringIO
import time
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceProcessClient
from ion.core.process.transform import TransformStreamPublisher, TransformEventListener, TransformStreamListener

from interface.services.cei.ischeduler_service import SchedulerServiceProcessClient
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceProcessClient
from pyon.event.event import EventSubscriber
from interface.services.dm.idata_retriever_service import DataRetrieverServiceProcessClient
from interface.services.dm.idataset_management_service import DatasetManagementServiceProcessClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceProcessClient

# Matplotlib related imports
# Need try/catch because of weird import error
try:
    from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
    from matplotlib.figure import Figure
    from matplotlib import dates
    from matplotlib.font_manager import FontProperties
except:
    import sys
    print >> sys.stderr, "Cannot import matplotlib"

mpl_output_image_format = "png"

class VizTransformMatplotlibGraphs(TransformStreamPublisher, TransformEventListener, TransformStreamListener):

    """
    This class is used for instantiating worker processes that have subscriptions to data streams and convert
    incoming data from CDM format to Matplotlib graphs

    """
    output_bindings = ['graph_image_param_dict']
    event_timer_interval = None


    def on_start(self):
        #print ">>>>>>>>>>>>>>>>>>>>>> MPL CFG = ", self.CFG

        self.pubsub_management = PubsubManagementServiceProcessClient(process=self)
        self.ssclient = SchedulerServiceProcessClient(process=self)
        self.rrclient = ResourceRegistryServiceProcessClient(process=self)
        self.data_retriever_client = DataRetrieverServiceProcessClient(process=self)
        self.dsm_client = DatasetManagementServiceProcessClient(process=self)
        self.pubsub_client = PubsubManagementServiceProcessClient(process = self)

        self.stream_info  = self.CFG.get_safe('process.publish_streams',{})
        self.stream_names = self.stream_info.keys()
        self.stream_ids   = self.stream_info.values()

        if not self.stream_names:
            raise BadRequest('MPL Transform has no output streams.')

        graph_time_periods= self.CFG.get_safe('graph_time_periods')

        # If this is meant to be an event driven process, schedule an event to be generated every few minutes/hours
        self.event_timer_interval = self.CFG.get_safe('graph_update_interval')
        if self.event_timer_interval:
            event_origin = "Interval_Timer_Matplotlib"
            sub = EventSubscriber(event_type="ResourceEvent", callback=self.interval_timer_callback, origin=event_origin)
            sub.start()

            self.interval_timer_id = self.ssclient.create_interval_timer(start_time="now" , interval=self._str_to_secs(self.event_timer_interval),
                event_origin=event_origin, event_subtype="")

        super(VizTransformMatplotlibGraphs,self).on_start()

    # when tranform is used as a data process
    def recv_packet(self, packet, in_stream_route, in_stream_id):
        #Check to see if the class instance was set up as a event triggered transform. If yes, skip the packet
        if self.event_timer_interval:
            return

        log.info('Received packet')
        mpl_data_granule = VizTransformMatplotlibGraphsAlgorithm.execute(packet, params=self.get_stream_definition())
        for stream_name in self.stream_names:
            publisher = getattr(self, stream_name)
            publisher.publish(mpl_data_granule)

    def get_stream_definition(self):
        stream_id = self.stream_ids[0]
        stream_def = self.pubsub_management.read_stream_definition(stream_id=stream_id)
        return stream_def._id

    def process_event(self, msg, headers):

        return

    def interval_timer_callback(self, *args, **kwargs):
        #Find out the input data product to this process
        in_dp_id = self.CFG.get_safe('in_dp_id')

        #print " >>>>>>>>>>>>>> IN DP ID from cfg : ", in_dp_id,  " and stream_names = ", self.stream_names

        # get the dataset_id associated with the data_product. Need it to do the data retrieval
        ds_ids,_ = self.rrclient.find_objects(in_dp_id, PRED.hasDataset, RT.DataSet, True)
        if ds_ids is None or not ds_ids:
            return None

        # retrieve data for the specified time interval
        end_time = ntplib.system_to_ntp_time(time.time()) # Now
        start_time = end_time - self._str_to_secs(self.CFG.get_safe('graph_time_period'))
        #retrieved_granule = self.data_retriever_client.retrieve(ds_ids[0],{'start_time':start_time,'end_time':end_time})
        retrieved_granule = self.data_retriever_client.retrieve(ds_ids[0])

        visualization_parameters = {}
        # send the granule through the Algorithm code to get the matplotlib graphs
        mpl_pdict_id = self.dsm_client.read_parameter_dictionary_by_name('graph_image_param_dict',id_only=True)

        mpl_stream_def = self.pubsub_client.create_stream_definition('mpl', parameter_dictionary_id=mpl_pdict_id)
        fileName = self.CFG.get_safe('graph_time_period')
        mpl_data_granule = VizTransformMatplotlibGraphsAlgorithm.execute(retrieved_granule, config=visualization_parameters, params=mpl_stream_def, fileName=fileName)

        if mpl_data_granule == None:
            return None

        # publish on all specified output streams
        for stream_name in self.stream_names:
            publisher = getattr(self, stream_name)
            publisher.publish(mpl_data_granule)

        return

    def _str_to_secs(self, time_period):
        # this method converts commonly used time periods to its actual seconds counterpart
        #separate alpha and numeric parts of the time period
        time_n = time_period.lower().rstrip('abcdefghijklmnopqrstuvwxyz ')
        time_a = time_period.lower().lstrip('0123456789. ')

        # determine if user specified, secs, mins, hours, days, weeks, months, years
        factor = None
        if time_a == 'sec' or time_a == "secs" or time_a == 'second' or time_a == "seconds":
            factor = 1
        if time_a == "min" or time_a == "mins" or time_a == "minute" or time_a == "minutes":
            factor = 60
        if time_a == "hr" or time_a == "hrs" or time_a == "hour" or time_a == "hours":
            factor = 60 * 60
        if time_a == "day" or time_a == "days":
            factor = 60 * 60 * 24
        if time_a == "wk" or time_a == "wks" or time_a == "week" or time_a == "weeks":
            factor = 60 * 60 * 24 * 7
        if time_a == "mon" or time_a == "mons" or time_a == "month" or time_a == "months":
            factor = 60 * 60 * 24 * 30
        if time_a == "yr" or time_a == "yrs" or time_a == "year" or time_a == "years":
            factor = 60 * 60 * 24 * 365

        time_period_secs = float(time_n) * factor
        return time_period_secs


    def on_quit(self):

        #Cancel the timer
        if hasattr(self, 'interval_timer_id'):
            self.ssclient.cancel_timer(self.interval_timer_id)

        super(VizTransformMatplotlibGraphs,self).on_quit()


class VizTransformMatplotlibGraphsAlgorithm(SimpleGranuleTransformFunction):
    @staticmethod
    @SimpleGranuleTransformFunction.validate_inputs
    def execute(input=None, context=None, config=None, params=None, state=None, fileName = None):

        stream_definition_id = params
        mpl_allowed_numerical_types = ['int32', 'int64', 'uint32', 'uint64', 'float32', 'float64']

        if stream_definition_id == None:
            log.error("Matplotlib transform: Need a output stream definition to process graphs")
            return None

        # parse the incoming data
        rdt = RecordDictionaryTool.load_from_granule(input)

        # build a list of fields/variables that need to be plotted. Use the list provided by the UI
        # since the retrieved granule might have extra fields.
        fields = []
        if config and config['parameters']:
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

            # only consider fields which are supposed to be numbers.
            if (rdt[field] != None) and (rdt[field].dtype not in mpl_allowed_numerical_types):
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

        out_granule = VizTransformMatplotlibGraphsAlgorithm.render_graphs(graph_data, stream_definition_id, fileName)

        return out_granule

    @classmethod
    def render_graphs(cls, graph_data, stream_definition_id, fileName = None):

        # init Matplotlib
        fig = Figure(figsize=(8,4), dpi=200, frameon=True)
        ax = fig.add_subplot(111)
        canvas = FigureCanvas(fig)
        imgInMem = StringIO.StringIO()

        # If there's no data, wait
        # For the simple case of testing, lets plot all time variant variables one at a time
        xAxisVar = 'time'

        # Prepare the set of y axis variables that will be plotted. This needs to be smarter and passed as
        # config variable to the transform
        yAxisVars = []
        for varName, varData in graph_data.iteritems():
            if varName == 'time' or varName == 'height' or varName == 'longitude' or varName == 'latitude':
                continue
            yAxisVars.append(varName)


        # Do a error check for incorrect time values. Ignore all time == fill_values
        time_fill_value = 0.0
        clean_data_flag = False
        while not clean_data_flag:
            clean_data_flag = True
            # Go through the data and see if we can find empty time entries. Delete it along
            # with all corresponding variables
            for idx in xrange(len(graph_data[xAxisVar])):
                if graph_data[xAxisVar][idx] == time_fill_value:
                    graph_data[xAxisVar].pop(idx)
                    for varName in yAxisVars:
                        try:
                            graph_data[varName].pop(idx)
                        except IndexError:
                            # Do nothing really. Must be a malformed granule
                            pass
                    clean_data_flag = False
                    break

        xAxisFloatData = graph_data[xAxisVar]
        #xAxisFloatData = [dates.date2num(datetime.fromtimestamp(ntplib.ntp_to_system_time(t))) for t in graph_data[xAxisVar]]
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

        if not fileName:
            fileName = yAxisLabel + '_vs_' + xAxisVar

        fileName = fileName + "." + mpl_output_image_format

        # Choose a small font for the legend
        legend_font_prop = FontProperties()
        legend_font_prop.set_size('small')
        ax.set_xlabel(xAxisVar)
        ax.set_ylabel(yAxisLabel)
        ax.set_title(yAxisLabel + ' vs ' + xAxisVar)
        ax.set_autoscale_on(False)
        ax.legend(loc='upper left', ncol=3, fancybox=True, shadow=True, prop=legend_font_prop)

        # Date formatting
        # matplotlib date format object
        #hfmt = dates.DateFormatter('%m/%d %H:%M')
        #ax.xaxis.set_major_locator(dates.MinuteLocator())
        #ax.xaxis.set_major_formatter(hfmt)

        # Save the figure to the in memory file
        canvas.print_figure(imgInMem, format=mpl_output_image_format)
        imgInMem.seek(0)

        # Create output dictionary from the param dict
        out_rdt = RecordDictionaryTool(stream_definition_id=stream_definition_id)

        # Prepare granule content
        #out_dict = {}
        out_rdt["viz_product_type"] = ["matplotlib_graphs"]
        out_rdt["image_obj"] = [imgInMem.getvalue()]
        out_rdt["image_name"] = [fileName]
        out_rdt["content_type"] = ["image/png"]

        #print " >>>>>>>>>> OUT_IMAGE_NAME : ", out_rdt['image_name']

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
