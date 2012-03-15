'''
@author Luke Campbell
@file ion/processes/data/dispatcher/dispatcher_launcher.py
@description Launcher for the dispatcher infrastructure
'''
from interface.objects import ExchangeQuery
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from pyon.service.service import BaseService


class DispatcherLauncher(BaseService):
    def on_start(self):
        super(DispatcherLauncher, self).on_start()
        pubsub_cli = PubsubManagementServiceClient()

        #-------------------------------------------------
        # Make or get the subscription for this dispatcher
        #-------------------------------------------------
        subscription_id = self.CFG.get_safe('process.subscription_id',None)
        if not subscription_id:
            # Exchange Subscription
            subscription_id = pubsub_cli.create_subscription(query=ExchangeQuery(),exchange_name='dispatcher_cache')

        