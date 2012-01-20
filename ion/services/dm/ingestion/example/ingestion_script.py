from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.dm.itransform_management_service import TransformManagementServiceClient
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.services.dm.iingestion_management_service import IngestionManagementServiceClient
from pyon.public import IonObject, RT, log, AT

from ion.services.dm.transformation.example.transform_example import TransformExampleProducer

pubsub_client = PubsubManagementServiceClient(node=cc.node)
tms_client = TransformManagementServiceClient(node=cc.node)
rr_client = ResourceRegistryServiceClient(node=cc.node)
ingestion_client = IngestionManagementServiceClient(node = cc.node)

#########################################################################
# The code here between is only for this example. The transformation should do this part
# create a dummy stream of data
input_stream = IonObject(RT.Stream,name='input_stream', description='input stream')
input_stream.original = True
input_stream.mimetype = 'hdf'
input_stream_id = pubsub_client.create_stream(input_stream)

# create a subscription for the stream
subscription = IonObject(RT.Subscription,name='input_subscription', description='subscribe to this for input stream')
subscription.query['stream_id'] = input_stream_id
subscription.exchange_name = 'input queue'
subscription_id = pubsub_client.create_subscription(subscription)
###########################################################################

# create two transforms to listen to the stream....
# for this use an  ingestion_management_service client to create a configuration and activate it
ingestion_configuration_id = ingestion_client.create_ingestion_configuration(exchange_point_id='an_exchange_point_id', couch_storage={}, \
                    hfd_storage={},  number_of_workers=4, default_policy={})
ingestion_client.activate_ingestion_configuration(ingestion_configuration_id)

# have the client also creates transforms according to the configuration it just created.
# For this the client provides parameters necessary to create a transform and create one
subscription_id = 'a_subscription'
output_stream_id = 'an_output_stream'
listen_name = subscription.exchange_name
ingestion_client.launch_transforms(ingestion_configuration_id, subscription_id, output_stream_id, listen_name )

# At this point the transforms are going to be waiting for a message.
# Now fire off streams of data as messages in intervals
# ... Now I use the TransformExampleProducer to fire the streams

stream_Producer = TransformExampleProducer()

# At this point, I need the transforms to be able to handle the messages


# At the end of the day, the client can deactivate the ingestion_configuration
ingestion_client.deactivate_ingestion_configuration(ingestion_configuration_id)



