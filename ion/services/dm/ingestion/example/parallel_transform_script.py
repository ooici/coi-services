from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.dm.itransform_management_service import TransformManagementServiceClient
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from pyon.public import IonObject, RT, log, AT

pubsub_client = PubsubManagementServiceClient(node=cc.node)
tms_client = TransformManagementServiceClient(node=cc.node)
rr_client = ResourceRegistryServiceClient(node=cc.node)

# Create a dummy output stream from a 'ctd' instrument
input_stream = IonObject(RT.Stream,name='input stream', description='input stream')
input_stream.original = True
input_stream.mimetype = 'hdf'
input_stream_id = pubsub_client.create_stream(input_stream)

# Create the subscription to the ctd_output_stream
subscription = IonObject(RT.Subscription,name='input subscription', description='subscribe to this for input stream')
subscription.query['stream_id'] = input_stream_id
subscription.exchange_name = 'input queue'
subscription_id = pubsub_client.create_subscription(subscription)

# Create an output stream for the first transform
first_output_stream = IonObject(RT.Stream,name='first transform output', description='output from the transform process')
first_output_stream.original = True
first_output_stream.mimetype='raw'
first_output_stream_id = pubsub_client.create_stream(first_output_stream)

# Create an output stream for the second transform
second_output_stream = IonObject(RT.Stream,name='second transform output', description='output from the transform process')
second_output_stream.original = True
second_output_stream.mimetype='raw'
second_output_stream_id = pubsub_client.create_stream(second_output_stream)

first_process_definition = IonObject(RT.ProcessDefinition, name='first_transform_definition')
first_process_definition.executable = {'module': 'ion.services.dm.transformation.example.transform_example', 'class':'TransformExample'}
first_process_definition_id, _ = rr_client.create(first_process_definition)

configuration= {'process':{'name':'first configuration','type':"stream_process",'listen_name': 'input queue' }}

# Launch the first transform process
first_transform_id = tms_client.create_transform(in_subscription_id=subscription_id,
    out_stream_id=first_output_stream_id,
    process_definition_id=first_process_definition_id,
    configuration=configuration)
tms_client.activate_transform(first_transform_id)

# Launch the second transform
second_process_definition = IonObject(RT.ProcessDefinition, name='second_transform_definition')
second_process_definition.executable = {'module': 'ion.services.dm.transformation.example.transform_example', 'class':'TransformExample'}
second_process_definition_id, _ = rr_client.create(second_process_definition)

configuration= {'process':{'name':'second configuration','type':"stream_process",'listen_name': 'input queue'}}

second_transform_id = tms_client.create_transform( in_subscription_id=subscription_id,
    out_stream_id= second_output_stream_id,
    process_definition_id=second_process_definition_id,
    configuration=configuration)
tms_client.activate_transform(second_transform_id)

