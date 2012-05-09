from pyon.public import Container, log, IonObject
from pyon.util.int_test import IonIntegrationTestCase
from interface.objects import CouchStorage, ProcessDefinition, StreamQuery
from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceClient

from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.services.dm.iingestion_management_service import IngestionManagementServiceClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient
from interface.services.sa.idata_product_management_service import DataProductManagementServiceClient
from interface.services.sa.idata_process_management_service import DataProcessManagementServiceClient
from interface.services.sa.iinstrument_management_service import InstrumentManagementServiceClient
from interface.services.sa.idata_acquisition_management_service import DataAcquisitionManagementServiceClient

from prototype.sci_data.stream_defs import SBE37_CDM_stream_definition
from ion.processes.data.transforms.ctd.ctd_L2_salinity import SalinityTransform
from ion.processes.data.transforms.example_double_salinity import SalinityDoubler


from pyon.public import log

from pyon.public import StreamSubscriberRegistrar

from pyon.util.int_test import IonIntegrationTestCase
from pyon.public import CFG, RT, LCS, PRED
from pyon.core.exception import BadRequest

from nose.plugins.attrib import attr

import gevent, numpy
from prototype.sci_data.stream_parser import PointSupplementStreamParser



from pyon.util.context import LocalContextMixin


class FakeProcess(LocalContextMixin):
    """
    A fake process used because the test case is not an ion process.
    """
    name = ''
    id=''
    process_type = ''


@attr('INT', group='as')
class TestVisualizationWorkflowIntegration(IonIntegrationTestCase):

    def setUp(self):
        # Start container

        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2deploy.yml')

        print 'started services'

        # Now create client to DataProductManagementService
        self.rrclient = ResourceRegistryServiceClient(node=self.container.node)
        self.damsclient = DataAcquisitionManagementServiceClient(node=self.container.node)
        self.pubsubclient =  PubsubManagementServiceClient(node=self.container.node)
        self.ingestclient = IngestionManagementServiceClient(node=self.container.node)
        self.imsclient = InstrumentManagementServiceClient(node=self.container.node)
        self.dataproductclient = DataProductManagementServiceClient(node=self.container.node)
        self.dataprocessclient = DataProcessManagementServiceClient(node=self.container.node)
        self.datasetclient =  DatasetManagementServiceClient(node=self.container.node)

        self.process_dispatcher = ProcessDispatcherServiceClient(node=self.container.node)

    def test_workflow_components(self):

        cc = self.container
        assertions = self.assertTrue


        #-------------------------------
        # Create CTD Parsed as the initial data product
        #-------------------------------
        # create a stream definition for the data from the ctd simulator
        ctd_stream_def = SBE37_CDM_stream_definition()
        ctd_stream_def_id = self.pubsubclient.create_stream_definition(container=ctd_stream_def, name='Simulated CTD data')


        print 'Creating new CDM data product with a stream definition'
        dp_obj = IonObject(RT.DataProduct,name='ctd_parsed',description='ctd stream test')
        try:
            ctd_parsed_data_product = self.dataproductclient.create_data_product(dp_obj, ctd_stream_def_id)
        except Exception as ex:
            self.fail("failed to create new data product: %s" %ex)

        print 'new ctd_parsed_data_product_id = ', ctd_parsed_data_product

        instDevice_obj = IonObject(RT.InstrumentDevice, name='SBE37IMDevice', description="SBE37IMDevice", serial_number="12345" )
        instDevice_id = self.imsclient.create_instrument_device(instrument_device=instDevice_obj)


        self.damsclient.assign_data_product(input_resource_id=instDevice_id, data_product_id=ctd_parsed_data_product)

        self.dataproductclient.activate_data_product_persistence(data_product_id=ctd_parsed_data_product, persist_data=True, persist_metadata=True)

        # Retrieve the id of the OUTPUT stream from the out Data Product
        stream_ids, _ = self.rrclient.find_objects(ctd_parsed_data_product, PRED.hasStream, None, True)
        assertions(len(stream_ids) > 0 )
        ctd_stream_id = stream_ids[0]

        ###
        ###  Setup the first transformation
        ###

        # Salinity: Data Process Definition
        log.debug("Create data process definition SalinityTransform")
        dpd_obj = IonObject(RT.DataProcessDefinition,
            name='ctd_salinity',
            description='create a salinity data product',
            module='ion.processes.data.transforms.ctd.ctd_L2_salinity',
            class_name='SalinityTransform',
            process_source='SalinityTransform source code here...')
        try:
            ctd_L2_salinity_dprocdef_id = self.dataprocessclient.create_data_process_definition(dpd_obj)
        except Excpetion as ex:
            self.fail("failed to create new SalinityTransform data process definition: %s" %ex)


        # create a stream definition for the data from the salinity Transform
        sal_stream_def_id = self.pubsubclient.create_stream_definition(container=SalinityTransform.outgoing_stream_def,  name='L2_salinity')
        self.dataprocessclient.assign_stream_definition_to_data_process_definition(sal_stream_def_id, ctd_L2_salinity_dprocdef_id )

        # Create the output data product of the transform
        log.debug("create output data product L2 Salinity")
        ctd_l2_salinity_output_dp_obj = IonObject(RT.DataProduct, name='L2_Salinity',description='transform output L2 salinity')
        ctd_l2_salinity_output_dp_id = self.dataproductclient.create_data_product(ctd_l2_salinity_output_dp_obj, sal_stream_def_id)
        self.dataproductclient.activate_data_product_persistence(data_product_id=ctd_l2_salinity_output_dp_id, persist_data=True, persist_metadata=True)


        # Create the Salinity transform data process
        log.debug("create L2_salinity data_process and start it")
        try:
            l2_salinity_all_data_process_id = self.dataprocessclient.create_data_process(ctd_L2_salinity_dprocdef_id, ctd_parsed_data_product, {'output':ctd_l2_salinity_output_dp_id})
            self.dataprocessclient.activate_data_process(l2_salinity_all_data_process_id)
        except BadRequest as ex:
            self.fail("failed to create new data process: %s" %ex)

        log.debug("test_createTransformsThenActivateInstrument: create L2_salinity data_process return")



        ###
        ###  Setup the second transformation
        ###

        # Salinity: Data Process Definition
        log.debug("Create data process definition SalinityDoublerTransform")
        dpd_obj = IonObject(RT.DataProcessDefinition,
            name='salinity_doubler',
            description='create a salinity doubler data product',
            module='ion.processes.data.transforms.example_double_salinity',
            class_name='SalinityDoubler',
            process_source='SalinityDoubler source code here...')
        try:
            salinity_doubler_dprocdef_id = self.dataprocessclient.create_data_process_definition(dpd_obj)
        except Exception as ex:
            self.fail("failed to create new SalinityDoubler data process definition: %s" %ex)



        # create a stream definition for the data from the salinity Transform
        salinity_double_stream_def_id = self.pubsubclient.create_stream_definition(container=SalinityDoubler.outgoing_stream_def,  name='SalinityDoubler')
        self.dataprocessclient.assign_stream_definition_to_data_process_definition(salinity_double_stream_def_id, salinity_doubler_dprocdef_id )

        # Create the output data product of the transform
        log.debug("create output data product SalinityDoubler")
        salinity_doubler_output_dp_obj = IonObject(RT.DataProduct, name='SalinityDoubler',description='transform output salinity doubler')
        salinity_doubler_output_dp_id = self.dataproductclient.create_data_product(salinity_doubler_output_dp_obj, salinity_double_stream_def_id)
        self.dataproductclient.activate_data_product_persistence(data_product_id=salinity_doubler_output_dp_id, persist_data=True, persist_metadata=True)


        # Create the Salinity transform data process
        log.debug("create L2_salinity data_process and start it")
        try:
            salinity_double_data_process_id = self.dataprocessclient.create_data_process(salinity_doubler_dprocdef_id, ctd_l2_salinity_output_dp_id, {'output':salinity_doubler_output_dp_id})
            self.dataprocessclient.activate_data_process(salinity_double_data_process_id)
        except BadRequest as ex:
            self.fail("failed to create new data process: %s" %ex)

        log.debug("test_createTransformsThenActivateInstrument: create L2_salinity data_process return")









        ###
        ### Start the process for producing the CTD data
        ###
        # process definition for the ctd simulator...
        producer_definition = ProcessDefinition()
        producer_definition.executable = {
            'module':'ion.processes.data.ctd_stream_publisher',
            'class':'SimpleCtdPublisher'
        }

        ctd_sim_procdef_id = self.process_dispatcher.create_process_definition(process_definition=producer_definition)

        # Start the ctd simulator to produce some data
        configuration = {
            'process':{
                'stream_id':ctd_stream_id,
                }
        }
        ctd_sim_pid = self.process_dispatcher.schedule_process(process_definition_id=ctd_sim_procdef_id, configuration=configuration)




        ## get the stream id for the transform outputs
        stream_ids, _ = self.rrclient.find_objects(ctd_l2_salinity_output_dp_id, PRED.hasStream, None, True)
        assertions(len(stream_ids) > 0 )
        sal_stream_id = stream_ids[0]

        stream_ids, _ = self.rrclient.find_objects(salinity_doubler_output_dp_id, PRED.hasStream, None, True)
        assertions(len(stream_ids) > 0 )
        sal_dbl_stream_id = stream_ids[0]


        ###
        ### Make a subscriber in the test to listen for transformed data
        ###
        salinity_subscription_id = self.pubsubclient.create_subscription(
            query=StreamQuery([ctd_stream_id, sal_stream_id,sal_dbl_stream_id]),
            exchange_name = 'salinity_test',
            name = "test salinity subscription",
        )

        pid = cc.spawn_process(name='dummy_process_for_test',
            module='pyon.ion.process',
            cls='SimpleProcess',
            config={})
        dummy_process = cc.proc_manager.procs[pid]

        subscriber_registrar = StreamSubscriberRegistrar(process=dummy_process, node=cc.node)

        result = gevent.event.AsyncResult()
        results = []
        def message_received(message, headers):
            # Heads
            log.warn(' data received!')
            results.append(message)
            if len(results) >15:
                result.set(True)

        subscriber = subscriber_registrar.create_subscriber(exchange_name='salinity_test', callback=message_received)
        subscriber.start()

        # after the queue has been created it is safe to activate the subscription
        self.pubsubclient.activate_subscription(subscription_id=salinity_subscription_id)


        # Assert that we have received data
        assertions(result.get(timeout=20))

        #Stop the transform process

        # stop the flow parse the messages...
        self.process_dispatcher.cancel_process(ctd_sim_pid) # kill the ctd simulator process - that is enough data


        first_salinity_values = None

        for message in results:

            try:
                psd = PointSupplementStreamParser(stream_definition=ctd_stream_def, stream_granule=message)
                temp = psd.get_values('temperature')
                print psd.list_field_names()
            except KeyError as ke:
                temp = None

            if temp is not None:
                assertions(isinstance(temp, numpy.ndarray))

                print 'temperature=' + str(numpy.nanmin(temp))

                first_salinity_values = None

            else:
                psd = PointSupplementStreamParser(stream_definition=SalinityTransform.outgoing_stream_def, stream_granule=message)
                print psd.list_field_names()

                # Test the handy info method for the names of fields in the stream def
                assertions('salinity' in psd.list_field_names())

                # you have to know the name of the coverage in stream def
                salinity = psd.get_values('salinity')
                print 'salinity=' + str(numpy.nanmin(salinity))

                assertions(isinstance(salinity, numpy.ndarray))

                assertions(numpy.nanmin(salinity) > 0.0) # salinity should always be greater than 0

                if first_salinity_values is None:
                    first_salinity_values = salinity.tolist()
                else:
                    second_salinity_values = salinity.tolist()
                    assertions(len(first_salinity_values) == len(second_salinity_values))
                    for idx in range(0,len(first_salinity_values)):
                        assertions(first_salinity_values[idx]*2.0 == second_salinity_values[idx])




