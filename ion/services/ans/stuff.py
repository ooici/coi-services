from interface.objects import DataProduct, ProcessDefinition, StreamQuery
from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceClient
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.dm.itransform_management_service import TransformManagementServiceClient
from interface.services.sa.idata_product_management_service import DataProductManagementServiceClient
from prototype.sci_data.stream_defs import SBE37_CDM_stream_definition
from pyon.ion.resource import PRED




pubsub = PubsubManagementServiceClient()
dpms = DataProductManagementServiceClient()
rr_cli = ResourceRegistryServiceClient()
stream_def = SBE37_CDM_stream_definition()
tms = TransformManagementServiceClient()
pd = ProcessDispatcherServiceClient()
stream_def_id = pubsub.create_stream_definition(container=stream_def)
in_stream_id = pubsub.create_stream(stream_definition_id=stream_def_id)

in_subscription_id = pubsub.create_subscription(query=StreamQuery(stream_ids=[in_stream_id]),exchange_name='visualizer')



dp = DataProduct(name='visualization product')
dp_id = dpms.create_data_product(data_product=dp, stream_definition_id=stream_def_id)
stream_ids, garbage = rr_cli.find_objects(dp_id,PRED.hasStream, id_only=True)
stream_id = stream_ids[0]

config = {
        'data_product_id':dp_id,
        'stream_def_id':stream_def_id,
}

proc_def = ProcessDefinition()
proc_def.executable['module'] = 'ion.services.ans.visualization_service'
proc_def.executable['class'] = 'VizTransformProcForMatplotlibGraphs'
proc_def_id = pd.create_process_definition(process_definition=proc_def)
tms.create_transform(
    name='visual transform',
    in_subscription_id=in_subscription_id,
    process_definition_id=proc_def_id,
    configuration=config
)