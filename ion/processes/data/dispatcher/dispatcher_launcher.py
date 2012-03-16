'''
@author Luke Campbell
@file ion/processes/data/dispatcher/dispatcher_launcher.py
@description Launcher for the dispatcher infrastructure
'''
from interface.objects import ExchangeQuery, ProcessDefinition
from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.dm.itransform_management_service import TransformManagementServiceClient
from pyon.service.service import BaseService


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
            subscription_id = pubsub_cli.create_subscription(query=ExchangeQuery(),exchange_name='dispatcher_cache')

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
                'datastore_name':self.CFG.get_safe('process.datastore_name','dispatcher_cache'),
                'datastore_profile':'SCIDATA'
            }
        }


        for i in xrange(number_of_processes):
            config['process']['number']=i
            transform_id = tms_cli.create_transform(
                name='dispatcher_cache_%d' % i,
                in_subscription_id=subscription_id,
                configuration=config,
                process_definition_id=proc_def_id
            )
            tms_cli.activate_transform(transform_id=transform_id)

        #-------------------------------------------------
        # Time now, N processes are running
        # Need to start the visualizer
        #-------------------------------------------------

