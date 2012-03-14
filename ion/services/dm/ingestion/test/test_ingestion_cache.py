'''
@author Luke Campbell <lcampbell@asascience.com>
@file ion/services/dm/ingestion/test/test_aggregate_ingestion.py
@description Integration Test for aggregate ingestion worker
'''
from interface.objects import ProcessDefinition, ExchangeQuery
from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.dm.itransform_management_service import TransformManagementServiceClient
from prototype.sci_data.stream_defs import SBE37_CDM_stream_definition
from pyon.core import bootstrap
from pyon.datastore.datastore import DataStore
from pyon.net.endpoint import Publisher
from pyon.util.config import CFG
from pyon.util.int_test import IonIntegrationTestCase
from nose.plugins.attrib import attr

@attr('INT',group='dm')
class IngestionCacheTest(IonIntegrationTestCase):
    def setUp(self):
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2dm.yml')
        self.db = self.container.datastore_manager.get_datastore('dm_cache',DataStore.DS_PROFILE.SCIDATA)
        self.tms_cli = TransformManagementServiceClient()
        self.pubsub_cli = PubsubManagementServiceClient()
        self.pd_cli = ProcessDispatcherServiceClient()
        xs_dot_xp = CFG.core_xps.science_data
        try:
            self.XS, xp_base = xs_dot_xp.split('.')
            self.XP = '.'.join([bootstrap.get_sys_name(), xp_base])
        except ValueError:
            raise StandardError('Invalid CFG for core_xps.science_data: "%s"; must have "xs.xp" structure' % xs_dot_xp)


    def make_points(self,definition,stream_id='I am very special', N=100):
        from prototype.sci_data.constructor_apis import PointSupplementConstructor
        import numpy as np
        import random


        definition.stream_resource_id = stream_id


        total = N
        n = 10 # at most n records per granule
        i = 0

        while i < total:
            r = random.randint(1,n)

            psc = PointSupplementConstructor(point_definition=definition, stream_id=stream_id)
            for x in xrange(r):
                i+=1
                point_id = psc.add_point(time=i, location=(0,0,0))
                psc.add_scalar_point_coverage(point_id=point_id, coverage_id='temperature', value=np.random.normal(loc=48.0,scale=4.0, size=1)[0])
                psc.add_scalar_point_coverage(point_id=point_id, coverage_id='pressure', value=np.float32(1.0))
                psc.add_scalar_point_coverage(point_id=point_id, coverage_id='conductivity', value=np.float32(2.0))
            granule = psc.close_stream_granule()
            hdf_string = granule.identifiables[definition.data_stream_id].values
            granule.identifiables[definition.data_stream_id].values = hdf_string
            yield granule
        return

    def start_worker(self):


        proc_def = ProcessDefinition()
        proc_def.executable['module'] = 'ion.processes.data.ingestion.ingestion_cache'
        proc_def.executable['class'] = 'IngestionCache'
        proc_def_id = self.pd_cli.create_process_definition(process_definition=proc_def)


        subscription_id = self.pubsub_cli.create_subscription(query=ExchangeQuery(), exchange_name='ingestion_aggregate')

        config = {
            'couch_storage' : {
                'datastore_name' :'dm_cache',
                'datastore_profile' : 'SCIDATA'
            }
        }

        transform_id = self.tms_cli.create_transform(
            name='ingestion_cache',
            description='Ingestion that compiles an aggregate of metadata',
            in_subscription_id=subscription_id,
            process_definition_id=proc_def_id,
            configuration=config
        )


        self.tms_cli.activate_transform(transform_id=transform_id)

    def test_ingestion_cache(self):
        import time as tm
        self.start_worker()

        definition = SBE37_CDM_stream_definition()
        publisher = Publisher()

        stream_def_id = self.pubsub_cli.create_stream_definition(container=definition)
        stream_id = self.pubsub_cli.create_stream(stream_definition_id=stream_def_id)

        time = float(0.0)

        for granule in self.make_points(definition=definition, stream_id=stream_id, N=10):

            publisher.publish(granule, to_name=(self.XP, stream_id+'.data'))
            # Determinism sucks
            tm.sleep(0.5)
            doc = self.db.read(stream_id)
            ntime = doc.variables['time'].value
            self.assertTrue(ntime >= time, 'The documents did not sequentially get updated correctly.')
            time = ntime


