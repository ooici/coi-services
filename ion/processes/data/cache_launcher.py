
"""
@author Luke Campbell
@file ion/processes/data/cache_launcher.py
@description Last Update Cache Process Launcher
"""
from interface.objects import ProcessDefinition, ExchangeQuery
from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from pyon.core import bootstrap
from pyon.service.service import BaseService
from interface.services.dm.itransform_management_service import TransformManagementServiceClient
from pyon.util.config import CFG
from ion.processes.data.last_update_cache import CACHE_DATASTORE_NAME

class CacheLauncher(BaseService):
    def on_start(self):
        super(CacheLauncher,self).on_start()
        tms_cli = TransformManagementServiceClient()
        pubsub_cli = PubsubManagementServiceClient()
        pd_cli = ProcessDispatcherServiceClient()
        dname = CACHE_DATASTORE_NAME
        number_of_workers = self.CFG.get_safe('process.number_of_workers',2)

        proc_def = ProcessDefinition()
        proc_def.executable['module'] = 'ion.processes.data.last_update_cache'
        proc_def.executable['class'] = 'LastUpdateCache'
        proc_def_id = pd_cli.create_process_definition(process_definition=proc_def)

        xs_dot_xp = CFG.core_xps.science_data
        try:
            self.XS, xp_base = xs_dot_xp.split('.')
            self.XP = '.'.join([bootstrap.get_sys_name(), xp_base])
        except ValueError:
            raise StandardError('Invalid CFG for core_xps.science_data: "%s"; must have "xs.xp" structure' % xs_dot_xp)

        subscription_id = pubsub_cli.create_subscription(query=ExchangeQuery(), exchange_name='last_update_cache')

        config = {
            'couch_storage' : {
                'datastore_name' : dname,
                'datastore_profile' : 'SCIDATA'
            }
        }

        for i in xrange(number_of_workers):

            transform_id = tms_cli.create_transform(
                name='last_update_cache%d' % i,
                description='last_update that compiles an aggregate of metadata',
                in_subscription_id=subscription_id,
                process_definition_id=proc_def_id,
                configuration=config
            )


            tms_cli.activate_transform(transform_id=transform_id)