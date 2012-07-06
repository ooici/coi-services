from pyon.ion.stream import StreamSubscriberRegistrar
from pyon.net.endpoint import Subscriber
from interface.objects import StreamQuery
from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceClient

from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.services.dm.iingestion_management_service import IngestionManagementServiceClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient
from interface.services.sa.idata_product_management_service import DataProductManagementServiceClient
from interface.services.sa.idata_process_management_service import DataProcessManagementServiceClient
from interface.services.sa.iinstrument_management_service import InstrumentManagementServiceClient
from interface.services.sa.idata_acquisition_management_service import DataAcquisitionManagementServiceClient
from interface.services.ans.iworkflow_management_service import WorkflowManagementServiceClient

from prototype.sci_data.stream_defs import SBE37_CDM_stream_definition


from pyon.public import log


from ion.services.ans.test.test_helper import VisualizationIntegrationTestHelper

from nose.plugins.attrib import attr
import unittest, os
import imghdr
import gevent, numpy
from mock import Mock, sentinel, patch, call
from pyon.ion.granule.record_dictionary import RecordDictionaryTool
from pyon.util.containers import get_safe

from pyon.util.context import LocalContextMixin


class FakeProcess(LocalContextMixin):
    """
    A fake process used because the test case is not an ion process.
    """
    name = ''
    id=''
    process_type = ''


@attr('INT', group='as')
class TestVisualizationServiceIntegration(VisualizationIntegrationTestHelper):

    def setUp(self):
        # Start container

        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2deploy.yml')

        # Now create client to DataProductManagementService
        self.rrclient = ResourceRegistryServiceClient(node=self.container.node)
        self.damsclient = DataAcquisitionManagementServiceClient(node=self.container.node)
        self.pubsubclient =  PubsubManagementServiceClient(node=self.container.node)
        self.ingestclient = IngestionManagementServiceClient(node=self.container.node)
        self.imsclient = InstrumentManagementServiceClient(node=self.container.node)
        self.dataproductclient = DataProductManagementServiceClient(node=self.container.node)
        self.dataprocessclient = DataProcessManagementServiceClient(node=self.container.node)
        self.datasetclient =  DatasetManagementServiceClient(node=self.container.node)
        self.workflowclient = WorkflowManagementServiceClient(node=self.container.node)
        self.process_dispatcher = ProcessDispatcherServiceClient(node=self.container.node)

        self.ctd_stream_def = SBE37_CDM_stream_definition()

    def validate_messages(self, results):

        cc = self.container
        assertions = self.assertTrue

        first_salinity_values = None

        for message in results:
            rdt = RecordDictionaryTool.load_from_granule(message)
            try:
                temp = get_safe(rdt, 'temp')
                #                psd = PointSupplementStreamParser(stream_definition=self.ctd_stream_def, stream_granule=message)
                #                temp = psd.get_values('temperature')
                #                log.info(psd.list_field_names())
            except KeyError as ke:
                temp = None

            if temp is not None:
                assertions(isinstance(temp, numpy.ndarray))

                log.info( 'temperature=' + str(numpy.nanmin(temp)))





    @attr('LOCOINT')
    @patch.dict('pyon.ion.exchange.CFG', {'container':{'exchange':{'auto_register': False}}})
    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False),'Not integrated for CEI')
    @unittest.skip("Skipping for debugging ")
    def test_visualization_queue(self):

        assertions = self.assertTrue

        #The list of data product streams to monitor
        data_product_stream_ids = list()

        #Create the input data product
        ctd_stream_id, ctd_parsed_data_product_id = self.create_ctd_input_stream_and_data_product()
        data_product_stream_ids.append(ctd_stream_id)

        user_queue_name = 'user_queue'

        xq = self.container.ex_manager.create_xn_queue(user_queue_name)
        self.addCleanup(xq.delete)

        def cb(m, h):
            raise StandardError("Subscriber callback never gets called back!")


        salinity_subscription_id = self.pubsubclient.create_subscription(
            query=StreamQuery(data_product_stream_ids),
            exchange_name = user_queue_name,
            name = "user visualization queue",
        )


        subscriber_registrar = StreamSubscriberRegistrar(node=self.container.node)
        subscriber = Subscriber(from_name=(subscriber_registrar.XP, user_queue_name), callback=cb)
        subscriber.initialize()


        # after the queue has been created it is safe to activate the subscription
        self.pubsubclient.activate_subscription(subscription_id=salinity_subscription_id)

        #Start the output stream listener to monitor and collect messages
        #results = self.start_output_stream_and_listen(None, data_product_stream_ids)

        subscriber._chan.stop_consume()

        ctd_sim_pid = self.start_simple_input_stream_process(ctd_stream_id)

        gevent.sleep(10.0)  # Send some messages - don't care how many

        self.process_dispatcher.cancel_process(ctd_sim_pid) # kill the ctd simulator process - that is enough data

        msg_count,_ = subscriber._chan.get_stats()
        print 'Messages in user queue: ' + str(msg_count)

        #Validate the data from each of the messages along the way
        #self.validate_messages(results)

        subscriber._chan.start_consume()

        for x in range(msg_count):
            mo = subscriber.get_one_msg(timeout=1)
            print mo.body
            mo.ack()

        msg_count,_ = subscriber._chan.get_stats()
        print 'Messages in user queue: ' + str(msg_count)



        subscriber.close()