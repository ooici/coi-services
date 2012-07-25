#!/usr/bin/env python

from ion.core.bootstrap_process import BootstrapPlugin
from pyon.public import Container
from interface.objects import IngestionQueue
from interface.services.dm.iingestion_management_service import IngestionManagementServiceProcessClient


class BootstrapIngestion(BootstrapPlugin):
    """
    Bootstrap process for ingestion management.
    """

    def on_initial_bootstrap(self, process, config, **kwargs):
        """
        Defining the ingestion worker process is done in post_process_dispatcher.

        Creating transform workers happens here...
        """
        ing_ms_client = IngestionManagementServiceProcessClient(process=process)
        self.container = Container.instance

        exchange_point = config.get_safe('ingestion.exchange_point','science_data')
        queues = config.get_safe('ingestion.queues',None)
        if queues is None:
            queues = [dict(name='science_granule_ingestion', type='SCIDATA')]
        for i in xrange(len(queues)):
            item = queues[i]
            queues[i] = IngestionQueue(name=item['name'], type=item['type'], datastore_name=item['datastore_name'])
            xn = self.container.ex_manager.create_xn_queue(item['name'])
            xn.purge()

        ing_ms_client.create_ingestion_configuration(name='standard ingestion config',
            exchange_point_id=exchange_point,
            queues=queues)
        
