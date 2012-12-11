"""
@author Swarbhanu Chatterjee
@file ion/services/dm/presentation/test/event_management_test.py
@description Unit and Integration test implementations for the event management service class.
"""

from pyon.util.int_test import IonIntegrationTestCase
from pyon.util.unit_test import PyonTestCase
from pyon.util.containers import DotDict
from pyon.public import IonObject, RT, log, PRED
from pyon.core.exception import NotFound, BadRequest
from pyon.event.event import EventPublisher
from pyon.ion.stream import StandaloneStreamSubscriber
from ion.services.dm.presentation.event_management_service import EventManagementService
from ion.services.dm.utility.granule_utils import time_series_domain
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.services.dm.ievent_management_service import EventManagementServiceClient
from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient
from interface.services.sa.idata_product_management_service import DataProductManagementServiceClient
from interface.objects import PlatformEvent, EventType, ProcessDefinition
from mock import Mock, mocksignature
from nose.plugins.attrib import attr
import unittest, gevent

@attr('UNIT',group='dm')
class EventManagementTest(PyonTestCase):
    def setUp(self):


        mock_clients = self._create_service_mock('event_management')
        self.event_management = EventManagementService()
        self.event_management.clients = mock_clients
        self.mock_rr_client = self.event_management.clients.resource_registry
        self.mock_pd_client = self.event_management.clients.process_dispatcher
        self.mock_pubsub_client = self.event_management.clients.pubsub_management
        self.mock_dams_client = self.event_management.clients.data_acquisition_management

    def test_create_event_type(self):
        """
        Test creating an event
        """
        event_type = EventType(name="Test event type")
        self.mock_rr_client.create = mocksignature(self.mock_rr_client.create)
        self.mock_rr_client.create.return_value = ('event_type_id','rev_1')

        self.mock_dams_client.register_process = mocksignature(self.mock_dams_client.register_process)

        event_type_id = self.event_management.create_event_type(event_type)

        self.assertEquals(event_type_id, 'event_type_id')
        self.mock_rr_client.create.assert_called_once_with(event_type)

    def test_update_event_type(self):
        """
        Test updating an event
        """
        event_type = EventType(name="Test event type")
        self.mock_rr_client.update = mocksignature(self.mock_rr_client.update)
#        self.mock_rr_client.update.return_value = ('event_type_id','rev_1')

        self.event_management.update_event_type(event_type)
        self.mock_rr_client.update.assert_called_once_with(event_type)

    def test_read_event_type(self):
        """
        Test reading an event
        """
        event_type = Mock()
        self.mock_rr_client.read = mocksignature(self.mock_rr_client.read)
        self.mock_rr_client.read.return_value = event_type

        result = self.event_management.read_event_type(event_type_id='event_type_id')
        self.assertEquals(result, event_type)
        self.mock_rr_client.read.assert_called_once_with('event_type_id', '')


    def test_delete_event_type(self):
        """
        Test updating an event
        """
        self.mock_rr_client.delete = mocksignature(self.mock_rr_client.delete)

        self.event_management.delete_event_type(event_type_id='event_type_id')
        self.mock_rr_client.delete.assert_called_once_with('event_type_id')

    def test_create_event_process_definition(self):
        """
        Test creating an event process definition
        """
        process_definition = Mock()

        self.mock_pd_client.create_process_definition=mocksignature(self.mock_pd_client.create_process_definition)
        self.mock_pd_client.create_process_definition.return_value='procdef_id'

        self.mock_pd_client.schedule_process=mocksignature(self.mock_pd_client.schedule_process)
        self.mock_pd_client.schedule_process.return_value='pid'

        procdef_id = self.event_management.create_event_process_definition(version='version',
                                                                    module ='module',
                                                                    class_name='class_name',
                                                                    uri='uri',
                                                                    arguments='arguments',
                                                                    event_types=['t1', 't2'],
                                                                    sub_types=['t3'],
                                                                    origin_types=['or1', 'or2'])

        self.assertEquals(procdef_id, 'procdef_id')

    def test_update_event_process_definition(self):
        """
        Test updating an event process definition
        """
        process_definition_id = 'an id'
        process_def = ProcessDefinition()

        self.mock_rr_client.read = mocksignature(self.mock_rr_client.read)
        self.mock_rr_client.read.return_value = process_def

        self.mock_rr_client.update = mocksignature(self.mock_rr_client.update)

        self.event_management.update_event_process_definition(  event_process_definition_id =process_definition_id,
                                                                version='version',
                                                                event_types=['t1', 't2'],
                                                                sub_types=['t3'],
                                                                origin_types=['or1', 'or2'])

        self.mock_rr_client.update.assert_called_once_with(process_def)

    def test_read_event_process_definition(self):
        """
        Test reading an event process definition
        """
        event_process_def = Mock()
        self.mock_rr_client.read = mocksignature(self.mock_rr_client.read)
        self.mock_rr_client.read.return_value = event_process_def

        result = self.event_management.read_event_process_definition()
        self.assertEquals(result, event_process_def)

    def test_delete_event_process_definition(self):
        """
        Test deleting an event process definition
        """
        self.mock_rr_client.delete = mocksignature(self.mock_rr_client.delete)

        self.event_management.delete_event_process_definition('an id')
        self.mock_rr_client.delete.assert_called_once_with('an id')

    @unittest.skip('Not working')
    def test_create_event_process(self):
        """
        Test creating an event process
        """
        process_definition = ProcessDefinition(name='test')
        process_definition.definition = ''

        rrc = ResourceRegistryServiceClient(node = self.container.node)
        process_definition_id = rrc.create(process_definition)

        self.mock_rr_client.find_objects = Mock()
        self.mock_rr_client.find_objects.return_value = ['stream_id_1'], 'obj_assoc_1'

#        self.mock_pd_client.schedule_process = Mock()
#        self.mock_pd_client.schedule_process.return_value = 'process_id'

        self.mock_rr_client.create_association = mocksignature(self.mock_rr_client.create_association)

        pid = self.event_management.create_event_process(process_definition_id=process_definition_id,
            event_types=['type_1', 'type_2'],
            sub_types=['subtype_1', 'subtype_2'],
            origins=['or_1', 'or_2'],
            origin_types=['t1', 't2'],
            out_data_products={'conductivity': 'id1'}
        )

#        self.assertEquals(pid, 'process_id')

    @unittest.skip("The method to be tested has not yet been implemented")
    def test_update_event_process(self):
        """
        Test updating an event process
        """
        pass

    def test_read_event_process(self):
        """
        Test reading an event process
        """
        event_process = Mock()
        self.mock_rr_client.read = mocksignature(self.mock_rr_client.read)
        self.mock_rr_client.read.return_value = event_process

        result = self.event_management.read_event_process('event_process_id')
        self.assertEquals(result, event_process)

    def test_delete_event_process(self):
        """
        Test deleting an event process
        """
        self.mock_rr_client.delete = mocksignature(self.mock_rr_client.delete)
        self.event_management.delete_event_process('event_process_id')

        self.mock_rr_client.delete.assert_called_once_with('event_process_id')

    @unittest.skip("The method to be tested has not yet been implemented")
    def test_activate_event_process(self):
        """
        Test activating an event process
        """
        pass

    @unittest.skip("The method to be tested has not yet been implemented")
    def test_deactivate_event_process(self):
        """
        Test deactivating an event process
        """
        pass

    @unittest.skip("The method to be tested has not yet been implemented")
    def update_event_process_inputs(self):
        """
        Test updating event process inputs
        """
        pass


@attr('INT', group='dm')
class EventManagementIntTest(IonIntegrationTestCase):
    def setUp(self):
        super(EventManagementIntTest, self).setUp()

        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2deploy.yml')

        self.event_management = EventManagementServiceClient()
        self.rrc = ResourceRegistryServiceClient()
        self.process_dispatcher = ProcessDispatcherServiceClient()
        self.pubsub = PubsubManagementServiceClient()
        self.dataset_management = DatasetManagementServiceClient()
        self.data_product_management = DataProductManagementServiceClient()

        self.queue_cleanup = []
        self.exchange_cleanup = []

    def tearDown(self):
        for queue in self.queue_cleanup:
            xn = self.container.ex_manager.create_xn_queue(queue)
            xn.delete()
        for exchange in self.exchange_cleanup:
            xp = self.container.ex_manager.create_xp(exchange)
            xp.delete()

    def test_create_read_update_delete_event_type(self):
        """
        Test that the CRUD method for event types work correctly
        """

        event_type = EventType(name="an event type")
        event_type.origin = 'instrument_1'

        # create
        event_type_id = self.event_management.create_event_type(event_type)
        self.assertIsNotNone(event_type_id)

        # read
        read_event_type = self.event_management.read_event_type(event_type_id)
        self.assertEquals(read_event_type.name, event_type.name)
        self.assertEquals(read_event_type.origin, event_type.origin)

        #update
        read_event_type.origin = 'instrument_2'
        read_event_type.producer = 'producer'

        self.event_management.update_event_type(read_event_type)
        updated_event_type = self.event_management.read_event_type(event_type_id)

        self.assertEquals(updated_event_type.origin, 'instrument_2')
        self.assertEquals(updated_event_type.producer, 'producer')

        # delete
        self.event_management.delete_event_type(event_type_id)

        with self.assertRaises(NotFound):
            self.event_management.read_event_type(event_type_id)

    def test_create_read_update_delete_event_process_definition(self):
        """
        Test that the CRUD methods for the event process definitions work correctly
        """

        # Create
        module = 'ion.processes.data.transforms.event_alert_transform'
        class_name = 'EventAlertTransform'
        procdef_id = self.event_management.create_event_process_definition(version='ver_1',
                                                                            module=module,
                                                                            class_name=class_name,
                                                                            uri='http://hare.com',
                                                                            arguments=['arg1', 'arg2'],
                                                                            event_types=['ExampleDetectableEvent', 'type_2'],
                                                                            sub_types=['sub_type_1'])
        # Read
        read_process_def = self.event_management.read_event_process_definition(procdef_id)
        self.assertEquals(read_process_def.executable['module'], module)
        self.assertEquals(read_process_def.executable['class'], class_name)

        # Update
        self.event_management.update_event_process_definition(event_process_definition_id=procdef_id,
                                                                class_name='StreamAlertTransform',
                                                                arguments=['arg3', 'arg4'],
                                                                event_types=['event_type_new'])

        updated_event_process_def = self.event_management.read_event_process_definition(procdef_id)
        self.assertEquals(updated_event_process_def.executable['class'], 'StreamAlertTransform')
        self.assertEquals(updated_event_process_def.arguments, ['arg3', 'arg4'])
        definition = updated_event_process_def.definition
        self.assertEquals(updated_event_process_def.definition.event_types, ['event_type_new'])

        # Delete
        self.event_management.delete_event_process_definition(procdef_id)

        with self.assertRaises(NotFound):
            self.event_management.read_event_process_definition(procdef_id)

    def test_event_in_stream_out_transform(self):
        """
        Test the event-in/stream-out transform
        """

        stream_id, _ = self.pubsub.create_stream('test_stream', exchange_point='science_data')
        self.exchange_cleanup.append('science_data')

        #---------------------------------------------------------------------------------------------
        # Launch a ctd transform
        #---------------------------------------------------------------------------------------------
        # Create the process definition
        process_definition = ProcessDefinition(
            name='EventToStreamTransform',
            description='For testing an event-in/stream-out transform')
        process_definition.executable['module']= 'ion.processes.data.transforms.event_in_stream_out_transform'
        process_definition.executable['class'] = 'EventToStreamTransform'
        proc_def_id = self.process_dispatcher.create_process_definition(process_definition=process_definition)

        # Build the config
        config = DotDict()
        config.process.queue_name = 'test_queue'
        config.process.exchange_point = 'science_data'
        config.process.publish_streams.output = stream_id
        config.process.event_type = 'ExampleDetectableEvent'
        config.process.variables = ['voltage', 'temperature' ]

        # Schedule the process
        self.process_dispatcher.schedule_process(process_definition_id=proc_def_id, configuration=config)

        #---------------------------------------------------------------------------------------------
        # Create a subscriber for testing
        #---------------------------------------------------------------------------------------------

        ar_cond = gevent.event.AsyncResult()
        def subscriber_callback(m, r, s):
            ar_cond.set(m)
        sub = StandaloneStreamSubscriber('sub', subscriber_callback)
        self.addCleanup(sub.stop)
        sub_id = self.pubsub.create_subscription('subscription_cond',
            stream_ids=[stream_id],
            exchange_name='sub')
        self.pubsub.activate_subscription(sub_id)
        self.queue_cleanup.append(sub.xn.queue)
        sub.start()

        gevent.sleep(4)

        #---------------------------------------------------------------------------------------------
        # Publish an event. The transform has been configured to receive this event
        #---------------------------------------------------------------------------------------------

        event_publisher = EventPublisher("ExampleDetectableEvent")
        event_publisher.publish_event(origin = 'fake_origin', voltage = '5', temperature = '273')

        # Assert that the transform processed the event and published data on the output stream
        result_cond = ar_cond.get(timeout=10)
        self.assertTrue(result_cond)

    def test_create_read_delete_event_process(self):
        """
        Test that the CRUD methods for the event processes work correctly
        """

        #---------------------------------------------------------------------------------------------
        # Create a process definition
        #---------------------------------------------------------------------------------------------

        # Create
        module = 'ion.processes.data.transforms.event_alert_transform'
        class_name = 'EventAlertTransform'
        procdef_id = self.event_management.create_event_process_definition(version='ver_1',
            module=module,
            class_name=class_name,
            uri='http://hare.com',
            arguments=['arg1', 'arg2'],
            event_types=['ExampleDetectableEvent', 'type_2'],
            sub_types=['sub_type_1'])

        # Read
        read_process_def = self.event_management.read_event_process_definition(procdef_id)
        self.assertEquals(read_process_def.arguments, ['arg1', 'arg2'])

        #---------------------------------------------------------------------------------------------
        # Use the process definition to create a process
        #---------------------------------------------------------------------------------------------

        # Create a stream
        param_dict_id = self.dataset_management.read_parameter_dictionary_by_name('ctd_parsed_param_dict',id_only=True)
        parameter_dictionary = self.dataset_management.read_parameter_dictionary(parameter_dictionary_id=param_dict_id)

        stream_def_id = self.pubsub.create_stream_definition('cond_stream_def', parameter_dictionary_id=param_dict_id)

        tdom, sdom = time_series_domain()
        tdom, sdom = tdom.dump(), sdom.dump()

        dp_obj = IonObject(RT.DataProduct,
            name='DP1',
            description='some new dp',
            temporal_domain = tdom,
            spatial_domain = sdom)

        # Create a data product
        data_product_id = self.data_product_management.create_data_product(data_product=dp_obj, stream_definition_id=stream_def_id, parameter_dictionary=parameter_dictionary)

        output_products = {}
        output_products['conductivity'] = data_product_id

        # Create an event process
        event_process_id = self.event_management.create_event_process(  process_definition_id=procdef_id,
                                                                        event_types=['ExampleDetectableEvent','DetectionEvent'],
                                                                        sub_types=['s1', 's2'],
                                                                        origins=['or_1', 'or_2'],
                                                                        origin_types=['or_t1', 'or_t2'],
                                                                        out_data_products = output_products)

        #---------------------------------------------------------------------------------------------
        # Read the event process object and make assertions
        #---------------------------------------------------------------------------------------------                                                                out_data_products={'conductivity': data_product_id})
        event_process_obj = self.event_management.read_event_process(event_process_id=event_process_id)

        # Get the stream associated with the data product for the sake of making assertions
        stream_ids, _ = self.rrc.find_objects(data_product_id, PRED.hasStream, id_only=True)
        stream_id = stream_ids[0]

        # Assertions!
        self.assertEquals(event_process_obj.detail.output_streams['conductivity'], stream_id)
        self.assertEquals(event_process_obj.detail.event_types, ['ExampleDetectableEvent', 'DetectionEvent'])
        self.assertEquals(event_process_obj.detail.sub_types, ['s1', 's2'])
        self.assertEquals(event_process_obj.detail.origins, ['or_1', 'or_2'])
        self.assertEquals(event_process_obj.detail.origin_types, ['or_t1', 'or_t2'])

    @unittest.skip("Not yet implemented in R 2.0")
    def test_activate_deactivate_data_process(self):
        """
        Test that the activation and deactivation of event processes happens correctly
        """

        pass

    @unittest.skip("Not yet implemented in R 2.0")
    def test_update_event_process_inputs(self):
        """
        Test that the event process inputs can be correctly updated
        """

        pass

