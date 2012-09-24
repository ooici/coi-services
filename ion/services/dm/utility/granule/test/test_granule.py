from pyon.util.int_test import IonIntegrationTestCase
from pyon.public import log
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from ion.services.dm.utility.granule.record_dictionary import RecordDictionaryTool
from pyon.ion.stream import StandaloneStreamPublisher, StandaloneStreamSubscriber
from ion.util.parameter_yaml_IO import get_param_dict

from nose.plugins.attrib import attr
import numpy as np
import gevent

important32 = np.float32('1.2345678901234567890')
important_s = important32.tostring()



@attr('INT', group='sa')
#@unittest.skip('run locally only')
class TestGranule(IonIntegrationTestCase):

    def setUp(self):
        # Start container
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2deploy.yml')
        self.pubsub_cli = PubsubManagementServiceClient()
        self.cleanup_queues = []
        self.cleanup_xp     = []

    def tearDown(self):
        for queue in self.cleanup_queues:
            xn = self.container.ex_manager.create_xn_queue(queue)
            self.container.ex_manager.delete_xn(xn)
        for exchange in self.cleanup_xp:
            xp = self.container.ex_manager.create_xp(exchange)
            self.container.ex_manager.delete_xp(xp)


    def test_granule(self):

        self.verified = gevent.event.Event()

        v32 = np.vectorize(lambda x : important32)
        
        def verify(m,r,s):
            rdt = RecordDictionaryTool.load_from_granule(m)
            print rdt
            self.assertTrue((rdt['time'] == np.arange(20)).all())
            for i in rdt['temp']:
                self.assertEquals(i.tostring(), important_s) # verifies perfect fidelity
            self.assertTrue((rdt['pressure'] == np.arange(20,dtype=np.float32)).all())
            self.verified.set()

        pdict = get_param_dict('sample_param_dict')

        stream_def = self.pubsub_cli.create_stream_definition(name='example', parameter_dictionary=pdict.dump())
        rdt = RecordDictionaryTool(stream_definition_id=stream_def)

        rdt['time']     = np.arange(20)
        rdt['temp']     = v32(np.zeros(20))
        rdt['pressure'] = np.arange(20)

        log.debug('RDT: %s', repr(rdt))
        
        stream_id, route = self.pubsub_cli.create_stream('rdt_pub', exchange_point='test', stream_definition_id=stream_def)
        subscriber_id    = self.pubsub_cli.create_subscription('rdt_sub', stream_ids=[stream_id])

        sub = StandaloneStreamSubscriber('rdt_sub', verify)
        sub.start()

        self.pubsub_cli.activate_subscription(subscriber_id)

        pub = StandaloneStreamPublisher(stream_id, route)
        pub.publish(rdt.to_granule())

        self.assertTrue(self.verified.wait(10))



            


