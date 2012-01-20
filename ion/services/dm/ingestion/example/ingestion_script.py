from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.dm.itransform_management_service import TransformManagementServiceClient
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from pyon.public import IonObject, RT, log, AT

pubsub_cli = PubsubManagementServiceClient(node=cc.node)
tms_cli = TransformManagementServiceClient(node=cc.node)
rr_cli = ResourceRegistryServiceClient(node=cc.node)

# Create a dummy output stream from a 'ctd' instrument
ctd_output_stream = IonObject(RT.Stream,name='ctd1 output', description='output from a ctd')
ctd_output_stream.original = True
ctd_output_stream.mimetype = 'hdf'
ctd_output_stream_id = pubsub_cli.create_stream(ctd_output_stream)

# Create the subscription to the ctd_output_stream
ctd_subscription = IonObject(RT.Subscription,name='ctd1 subscription', description='subscribe to this if you want ctd1 data')
ctd_subscription.query['stream_id'] = ctd_output_stream_id
ctd_subscription.exchange_name = 'a queue'

ctd_subscription_id = pubsub_cli.create_subscription(ctd_subscription)

# Create an output stream for the transform
transform_output_stream = IonObject(RT.Stream,name='transform output', description='output from the transform process')
transform_output_stream.original = True
transform_output_stream.mimetype='raw'
transform_output_stream_id = pubsub_cli.create_stream(transform_output_stream)

# Create a SUBSCRIPTION to this output stream for the second transform
second_subscription = IonObject(RT.Subscription,name='second_subscription', description='the subscription to the first transforms data')
second_subscription.query['stream_id'] = transform_output_stream_id
second_subscription.exchange_name = 'final output'
second_subscription_id = pubsub_cli.create_subscription(second_subscription)

# Create a final output stream
final_output = IonObject(RT.Stream,name='final_output_stream',description='Final output')
final_output.original = True
final_output.mimetype='raw'
final_output_id = pubsub_cli.create_stream(final_output)

process_definition = IonObject(RT.ProcessDefinition, name='transform_process_definition')
process_definition.executable = {'module': 'ion.services.dm.transformation.example.transform_example', 'class':'TransformExample'}
process_definition_id, _ = rr_cli.create(process_definition)

configuration = {}


# Launch the first transform process
transform_id = tms_cli.create_transform( in_subscription_id=ctd_subscription_id,
    out_stream_id=transform_output_stream_id,
    process_definition_id=process_definition_id,
    configuration=configuration)
tms_cli.activate_transform(transform_id)

# Launch the second transform
second_process_definition = IonObject(RT.ProcessDefinition, name='second_transform_definition')
second_process_definition.executable = {'module': 'ion.services.dm.transformation.example.transform_example', 'class':'TransformExample'}
second_process_definition_id, _ = rr_cli.create(process_definition)
configuration = {}

second_transform_id = tms_cli.create_transform( in_subscription_id=second_subscription_id,
    out_stream_id= final_output_id,
    process_definition_id=second_process_definition_id,
    configuration=configuration)
tms_cli.activate_transform(second_transform_id)