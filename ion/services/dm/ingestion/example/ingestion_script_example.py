from interface.services.dm.iingestion_management_service import IngestionManagementServiceClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient

from pyon.public import IonObject, RT, log, AT
from pyon.ion.endpoint import ProcessPublisher
from pyon.public import log, StreamProcess

#########################################################################################
# Run this part first in the pycc container
#########################################################################################
ingestion_client = IngestionManagementServiceClient(node = cc.node)
pubsub_client = PubsubManagementServiceClient(node=cc.node)

ctd_output_stream_id = pubsub_client.create_stream(name='ctd_output_stream', original=True)

# create transforms... queues will be created in this step
ingestion_configuration_id = ingestion_client.create_ingestion_configuration(exchange_point_id='science_data', couch_storage={},\
    hdf_storage={},  number_of_workers=2, default_policy={})
# activates the transforms... so bindings will be created in this step
ingestion_client.activate_ingestion_configuration(ingestion_configuration_id)

############################################################################################
# Copy the stream_id that is there in the the binding....in rabbit broker web management
# and paste it as a value in out_stream
############################################################################################
# messages will be produced and published
id_p = cc.spawn_process('ingestion_queue', 'ion.services.dm.ingestion.ingestion_example', 'IngestionExampleProducer',\
        {'process': {'type':'stream_process', 'publish_streams':{'out_stream':ctd_output_stream_id}},'stream_producer':{'interval':4000}})
cc.proc_manager.procs['%s.%s' %(cc.id,id_p)].start()