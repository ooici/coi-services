#!/usr/bin/env python
'''
@author M Mannning
@file ion/processes/data/transform/transform_worker.py
@description Data Process worker
'''
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.objects import Granule, DataProcessStatusType
from ion.core.process.transform import TransformStreamListener, TransformStreamProcess
from ion.services.dm.utility.granule.record_dictionary import RecordDictionaryTool
from pyon.ion.stream import StreamPublisher
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.services.sa.idata_process_management_service import DataProcessManagementServiceClient
from ion.util.time_utils import TimeUtils
from pyon.util.containers import DotDict



from gevent.coros import RLock

from pyon.public import log, RT, PRED, CFG, OT
from pyon.util.arg_check import validate_is_instance
from pyon.event.event import EventPublisher
from pyon.core.object import IonObjectSerializer
from pyon.ion.stream import StreamSubscriber
from pyon.ion.event import handle_stream_exception

from tempfile import gettempdir
import os
import requests


class TransformWorker(TransformStreamListener):
    CACHE_LIMIT=CFG.get_safe('container.ingestion_cache',5)

    # Status publishes after a set of granules has been processed
    STATUS_INTERVAL = 100

    def __init__(self, *args,**kwargs):
        super(TransformWorker, self).__init__(*args, **kwargs)

        # the set of data processes hosted by this worker
        self._dataprocesses = {}
        self._streamid_map = {}
        self._publisher_map = {}

        self._transforms = {}


    def on_start(self): #pragma no cover
        #super(TransformWorker,self).on_start()
        #--------------------------------------------------------------------------------
        # Explicit on_start
        #--------------------------------------------------------------------------------

        # Skip TransformStreamListener and go to StreamProcess to avoid the subscriber being created
        # We want explicit management of the thread and subscriber object for ingestion
        #todo: check how to manage multi queue subscription (transform scenario 3)

        TransformStreamProcess.on_start(self)

        #todo: can the subscription be changed or updated when new dataprocesses are added ?
        self.queue_name = self.CFG.get_safe('process.queue_name',self.id)
        self.subscriber = StreamSubscriber(process=self, exchange_name=self.queue_name, callback=self.receive_callback)
        self.thread_lock = RLock()

        self._rpc_server = self.container.proc_manager._create_listening_endpoint(from_name=self.id, process=self)
        self.add_endpoint(self._rpc_server)

        self.start_listener()

        #todo: determine and publish appropriate set of status events
        self.event_publisher = EventPublisher(OT.DataProcessStatusEvent)



    def on_quit(self): #pragma no cover
        self.event_publisher.close()
        if self.subscriber_thread:
            self.stop_listener()
        super(TransformWorker, self).on_quit()

    def start_listener(self):
        # We use a lock here to prevent possible race conditions from starting multiple listeners and coverage clobbering
        with self.thread_lock:
            self.subscriber_thread = self._process.thread_manager.spawn(self.subscriber.listen, thread_name='%s-subscriber' % self.id)


    def stop_listener(self):
        # Avoid race conditions with coverage operations (Don't start a listener at the same time as closing one)
        with self.thread_lock:
            self.subscriber.close()
            self.subscriber_thread.join(timeout=10)
            self.subscriber_thread = None




    @handle_stream_exception()
    def recv_packet(self, msg, stream_route, stream_id):
        ''' receive packet for ingestion '''
        log.debug('received granule for stream %s', stream_id)

        if msg == {}:
            log.error('Received empty message from stream: %s', stream_id)
            return
        # Message validation
        if not isinstance(msg, Granule):
            log.error('Ingestion received a message that is not a granule: %s', msg)
            return


        rdt = RecordDictionaryTool.load_from_granule(msg)
        log.debug('received granule for stream rdt %s', rdt)
        if rdt is None:
            log.error('Invalid granule (no RDT) for stream %s', stream_id)
            return
        if not len(rdt):
            log.debug('Empty granule for stream %s', stream_id)
            return

        dp_id_list = self.retrieve_dataprocess_for_stream(stream_id)

        for dp_id in dp_id_list:

            function, argument_list, context = self.retrieve_function_and_define_args(stream_id, dp_id)

            args = []
            rdt = RecordDictionaryTool.load_from_granule(msg)

            #create the input arguments list
            #todo: this logic is tied to the example function, generalize
            #todo: how to inject params not in the granule such as stream_id, dp_id, etc?
            for func_param, record_param in argument_list.iteritems():
                args.append(rdt[record_param])
            if context:
                args.append(context)

            try:
                #run the calc
                #todo: nothing in the data process resource to specify multi-out map
                result = ''
                try:
                    result = function(*args)
                    log.debug('recv_packet  result: %s',result)
                except:
                    log.error('Error running transform %s with args %s.', dp_id, args, exc_info=True)
                    raise

                out_stream_definition, output_parameter = self.retrieve_dp_output_params(dp_id)

                if out_stream_definition and output_parameter:
                    rdt_out = RecordDictionaryTool(stream_definition_id=out_stream_definition)
                    publisher = self._publisher_map.get(dp_id,'')

                    for param in rdt:
                        if param in rdt_out:
                            rdt_out[param] = rdt[param]
                    rdt_out[ output_parameter ] = result

                    if publisher:
                        log.debug('output rdt: %s',rdt)
                        publisher.publish(rdt_out.to_granule())
                    else:
                        log.error('Publisher not found for data process %s', dp_id)

                self.update_dp_metrics( dp_id )

            except ImportError:
                log.error('Error running transform')

    def retrieve_dataprocess_for_stream(self, stream_id):
        # if any data procrocesses apply to this stream
        dp_id_list = []
        if stream_id in self._streamid_map:
            dp_id_list = self._streamid_map[stream_id]
        else:
            dp_id_list = self.load_data_process(stream_id)
        return dp_id_list


    def retrieve_function_and_define_args(self, stream_id, dataprocess_id):
        import importlib
        argument_list = {}
        args = []
        #load the details of this data process
        dataprocess_info = self._dataprocesses[dataprocess_id]
        try:
            #todo: load once into a 'set' of modules?
            #load the associated transform function
            egg_uri = dataprocess_info.get_safe('uri','')
            if egg_uri:
                egg = self.download_egg(egg_uri)
                import pkg_resources
                pkg_resources.working_set.add_entry(egg)
            else:
                log.warning('No uri provided for module in data process definition.')

            module = importlib.import_module(dataprocess_info.get_safe('module', '') )

            function = getattr(module, dataprocess_info.get_safe('function','') )
            arguments = dataprocess_info.get_safe('arguments', '')
            argument_list = dataprocess_info.get_safe('argument_map', {})

            context = {}
            if self.has_context_arg(function,argument_list ):
                context = self.create_context_arg(stream_id, dataprocess_id)

        except ImportError:
            log.error('Error running transform')
        log.debug('retrieve_function_and_define_args  argument_list: %s',argument_list)
        return function, argument_list, context

    def retrieve_dp_output_params(self, dataprocess_id):
        dataprocess_info = self._dataprocesses[dataprocess_id]
        out_stream_definition = dataprocess_info.get_safe('out_stream_def', '')
        output_parameter = dataprocess_info.get_safe('output_param','')
        return out_stream_definition, output_parameter


    def update_dp_metrics(self, dataprocess_id):
        #update metrics
        dataprocess_info = self._dataprocesses[dataprocess_id]
        dataprocess_info.granule_counter += 1
        if dataprocess_info.granule_counter % self.STATUS_INTERVAL == 0:
            #publish a status update event
            self.event_publisher.publish_event(origin=dataprocess_id, origin_type='DataProcess', status=DataProcessStatusType.NORMAL,
                                   description='data process status update. %s granules processed'% dataprocess_info.granule_counter )


    def load_data_process(self, stream_id=""):

        dpms_client = DataProcessManagementServiceClient()

        print 'looking at', stream_id
        dataprocess_details_list = dpms_client.read_data_process_for_stream(stream_id)

        dataprocess_ids = []
        #this returns a list of data process info dicts
        for dataprocess_details in dataprocess_details_list:

            dataprocess_details = DotDict(dataprocess_details or {})
            dataprocess_id = dataprocess_details.dataprocess_id

            #set metrics attributes
            dataprocess_details.granule_counter = 0

            self._dataprocesses[dataprocess_id] = dataprocess_details
            log.debug('load_data_process  dataprocess_id: %s', dataprocess_id)
            log.debug('load_data_process  dataprocess_details: %s', dataprocess_details)

            # validate details
            # if not outstream info avaialable log a warning but TF may publish an event so proceed
            if not dataprocess_details.out_stream_def or not dataprocess_details.output_param:
                log.warning('No output stream details provided for data process %s, will not publish a granule', dataprocess_id)

            #add the stream id to the map
            if 'in_stream_id' in dataprocess_details:
                if dataprocess_details['in_stream_id'] in self._streamid_map:
                    (self._streamid_map[ dataprocess_details['in_stream_id'] ]).append(dataprocess_id)
                else:
                    self._streamid_map[ dataprocess_details['in_stream_id'] ]  = [dataprocess_id]
            #todo: add transform worker id
            self.event_publisher.publish_event(origin=dataprocess_id, origin_type='DataProcess', status=DataProcessStatusType.NORMAL,
                                               description='data process loaded into transform worker')

            #create a publisher for output stream
            self.create_publisher(dataprocess_id, dataprocess_details)
            dataprocess_ids.append(dataprocess_id)

        return dataprocess_ids


    def create_publisher(self, dataprocess_id, dataprocess_details):
        #todo: create correct publisher type for the transform type
        #todo: DataMonitor, Event Monitor get EventPublishers
        #todo: DataProcess, EventProcess get stream publishers
        out_stream_route = dataprocess_details.get('out_stream_route', '')
        out_stream_id = dataprocess_details.get('out_stream_id', '')
        publisher = StreamPublisher(process=self, stream_id=out_stream_id, stream_route=out_stream_route)

        self._publisher_map[dataprocess_id] = publisher

    @classmethod
    def download_egg(cls, url):
        '''
        Downloads an egg from the URL specified into the cache directory
        Returns the full path to the egg
        '''
        # Get the filename based on the URL
        filename = url.split('/')[-1]
        # Store it in the $TMPDIR
        egg_cache = gettempdir()
        path = os.path.join(egg_cache, filename)
        r = requests.get(url, stream=True)
        if r.status_code == 200:
            # Download the file using requests stream
            with open(path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=1024):
                    if chunk:
                        f.write(chunk)
                        f.flush()
            return path
        raise IOError("Couldn't download the file at %s" % url)

    def has_context_arg(self, func , argument_map):
        import inspect
        argspec = inspect.getargspec(func)
        return argspec.args != argument_map and 'context' in argspec.args

    def create_context_arg(self, stream_id, dataprocess_id):
        context = DotDict()
        context.stream_id = stream_id
        context.dataprocess_id = dataprocess_id
        return context
