'''
@author Luke Campbell
@file ion/processes/data/dispatcher/dispatcher_launcher.py
@description Launcher for the dispatcher infrastructure
'''
from pyon.container.shell_api import public_api
from ion.processes.data.dispatcher.dispatcher_render import DispatcherRender
from interface.objects import ExchangeQuery, ProcessDefinition, DataProduct, StreamQuery, StreamGranuleContainer
from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.dm.itransform_management_service import TransformManagementServiceClient
from interface.services.sa.idata_product_management_service import DataProductManagementServiceClient
from pyon.service.service import BaseService
from ion.processes.data.dispatcher.dispatcher_cache import DISPATCH_DATASTORE
from ion.processes.data.dispatcher.dispatcher_visualization import DispatcherVisualization
from pyon.util.file_sys import FileSystem, FS

class DispatcherLauncher(BaseService):
    '''
    DispatcherLauncher
    This service acts as an arbiter and sub-service for aiding users in rendering data.
    When the service is started it spawns N number of caching processes that ingest and persist all streams on the exchange point
    After start up the service appends an instance method to public_api giving the user easy access to the visualize method.

    If the user desires to cache a different subscription, the subscription id can be passed as a config arg

    We use instantiated objects in lieu of processes to beat the overhead and stochastic results of having two separate
    processes communicate without patching in an event-driven pattern or a lot of monkey patching.

    This pattern is clean, elegant, uses OO and saves on memory usage, instead of having lingering processes with huge
    buffers containing granules and images, they are released at the end of the visualize call.

    '''

    def on_start(self):
        super(DispatcherLauncher, self).on_start()
        self.pubsub_cli = PubsubManagementServiceClient()
        tms_cli = TransformManagementServiceClient()
        pd_cli = ProcessDispatcherServiceClient()

        #-------------------------------------------------
        # Make or get the subscription for this dispatcher
        #-------------------------------------------------
        subscription_id = self.CFG.get_safe('service.subscription_id',None)
        if not subscription_id:
            # Exchange Subscription
            subscription_id = self.pubsub_cli.create_subscription(query=ExchangeQuery(),exchange_name=DISPATCH_DATASTORE)

        number_of_processes = self.CFG.get_safe('service.number_of_processes',1)

        #-------------------------------------------------
        # Process definition
        #-------------------------------------------------
        proc_def = ProcessDefinition()
        proc_def.executable['module'] = 'ion.processes.data.dispatcher.dispatcher_cache'
        proc_def.executable['class'] = 'DispatcherCache'
        proc_def_id = pd_cli.create_process_definition(process_definition=proc_def)
        config = {
            'process':{
                'datastore_name':self.CFG.get_safe('process.datastore_name',DISPATCH_DATASTORE),
                'datastore_profile':'SCIDATA'
            }
        }
        #-------------------------------------------------
        # Launch N Transforms acting as the caching processes
        #-------------------------------------------------
        transform_id = ''
        for i in xrange(number_of_processes):
            config['process']['number']=i
            transform_id = tms_cli.create_transform(
                name='dispatcher_cache_%d' % i,
                in_subscription_id=subscription_id,
                configuration=config,
                process_definition_id=proc_def_id
            )
        if transform_id:
            tms_cli.activate_transform(transform_id=transform_id)

        visualize = self.visualize
        public_api.append(visualize)


    
    def visualize(self,stream_id):
        '''
        Simple method that renders and generates the files for the visual representation of the stream's granules.
        '''

        #-------------------------------------------------
        # Services and definitions
        #-------------------------------------------------

        dpms_cli = DataProductManagementServiceClient()

        stream_definition_id = self.pubsub_cli.find_stream_definition(stream_id=stream_id,id_only=True)

        # Assemble a data product for this visualization
        visual_product = DataProduct(name='granule visualization')
        visual_product_id = dpms_cli.create_data_product(
            data_product=visual_product,
            stream_definition_id=stream_definition_id
        )

        #-------------------------------------------------
        # Instantiation for the rending objects
        #-------------------------------------------------
        
        dispatcher_visualization = DispatcherVisualization(
            datastore_manager=self.container.datastore_manager,
            stream_id=stream_id,
            stream_definition_id=stream_definition_id
        )

        dispatcher_render = DispatcherRender(data_product_id=visual_product_id, stream_def_id=stream_definition_id)

        #-------------------------------------------------
        # Process and Render the data
        #-------------------------------------------------

        granule = dispatcher_visualization.execute_replay()

        dispatcher_render.process(granule)
        for result in dispatcher_render.results:
            with open(FileSystem.get_url(FS.TEMP,result['file_name']),'w') as f:
                f.write(result['image_string'])
        return dispatcher_render.results