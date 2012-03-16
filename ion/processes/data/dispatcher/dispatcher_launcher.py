'''
@author Luke Campbell
@file ion/processes/data/dispatcher/dispatcher_launcher.py
@description Launcher for the dispatcher infrastructure
'''
from interface.objects import ExchangeQuery, ProcessDefinition, DataProduct, StreamQuery
from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.dm.itransform_management_service import TransformManagementServiceClient
from interface.services.sa.idata_product_management_service import DataProductManagementServiceClient
from pyon.service.service import BaseService
from prototype.sci_data.stream_defs import SBE37_CDM_stream_definition
from ion.processes.data.dispatcher.dispatcher_cache import DISPATCH_DATASTORE

class DispatcherLauncher(BaseService):
    def on_start(self):
        super(DispatcherLauncher, self).on_start()
        pubsub_cli = PubsubManagementServiceClient()
        tms_cli = TransformManagementServiceClient()
        pd_cli = ProcessDispatcherServiceClient()

        #-------------------------------------------------
        # Make or get the subscription for this dispatcher
        #-------------------------------------------------
        subscription_id = self.CFG.get_safe('process.subscription_id',None)
        if not subscription_id:
            # Exchange Subscription
            subscription_id = pubsub_cli.create_subscription(query=ExchangeQuery(),exchange_name=DISPATCH_DATASTORE)

        number_of_processes = self.CFG.get_safe('process.number_of_processes',1)

        #-------------------------------------------------
        # Proc def
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

    @staticmethod
    def visualize(stream_id):
        import re
        dpms_cli = DataProductManagementServiceClient()
        pubsub_cli = PubsubManagementServiceClient()
        tms_cli = TransformManagementServiceClient()
        pd_cli = ProcessDispatcherServiceClient()
        stream_definition_id = pubsub_cli.find_stream_definition(stream_id=stream_id)
        en = re.sub('.data','.viz',stream_id)
        subscription_id = pubsub_cli.create_subscription(query=StreamQuery(stream_ids=[stream_id]), exchange_name=en)
        visual_product = DataProduct(name='visual product')
        visual_product_id = dpms_cli.create_data_product(data_product=visual_product, stream_definition_id=stream_definition_id)

        config = {
            'data_product_id':visual_product_id,
            'stream_def_id':stream_definition_id
        }

        proc_def = ProcessDefinition()
        proc_def.executable['module'] = 'ion.services.ans.visualization_service'
        proc_def.executable['class'] = 'VizTransformProcForMatplotlibGraphs'
        proc_def_id = pd_cli.create_process_definition(process_definition=proc_def)

        transform_id = tms_cli.create_transform(
            name='%s_vis' % stream_id,
            in_subscription_id=subscription_id,
            process_definition_id=proc_def_id,
            configuration=config
        )
        tms_cli.activate_transform(transform_id)