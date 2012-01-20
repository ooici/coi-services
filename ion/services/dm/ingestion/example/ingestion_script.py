
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.dm.itransform_management_service import TransformManagementServiceClient
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from pyon.public import IonObject, RT, log, AT

pubsub_client = PubsubManagementServiceClient(node=cc.node)
tms_client = TransformManagementServiceClient(node=cc.node)
rr_client = ResourceRegistryServiceClient(node=cc.node)

# Create a dummy input stream to the transform
ctd_output_stream = IonObject(RT.Stream,name='ctd1 output', description='output from a ctd')
ctd_output_stream.original = True
ctd_output_stream.mimetype = 'hdf'
ctd_output_stream_id = pubsub_client.create_stream(ctd_output_stream)

# Create the subscription for the input stream
ctd_subscription = IonObject(RT.Subscription,name='ctd1 subscription', description='subscribe to this if you want ctd1 data')
ctd_subscription.query['stream_id'] = ctd_output_stream_id
ctd_subscription.exchange_name = 'a queue'

process_definition = IonObject(RT.ProcessDefinition, name='transform_process_definition')
process_definition.executable = {'module': 'ion.services.dm.transformation.example.transform_example', 'class':'TransformExample'}
process_definition_id, _ = rr_client.create(process_definition)

configuration = {'process':{
    'name':'basic_transform',
    'type':'stream_process',
    'listen_name':ctd_subscription.exchange_name,
    'publish_streams':{'output':transform_output_stream_id}}}


# Launch the first transform process
transform_id = tms_client.create_transform(in_subscription_id=ctd_subscription_id,
    out_stream_id=transform_output_stream_id,
    process_definition_id=process_definition_id,
    configuration=configuration)
tms_client.activate_transform(transform_id)

# Launch the second transform
second_process_definition = IonObject(RT.ProcessDefinition, name='second_transform_definition')
second_process_definition_id, _ = rr_client.create(process_definition)
configuration = {'name':'second_transform', 'exchange_name':second_subscription.exchange_name}

second_transform_id = tms_client.create_transform( in_subscription_id=second_subscription_id,
    out_stream_id=final_output_id,
    process_definition_id=second_process_definition_id,
    configuration=configuration)
tms_client.activate_transform(second_transform_id)

