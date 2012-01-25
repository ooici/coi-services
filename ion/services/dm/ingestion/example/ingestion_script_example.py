"""
@author Swarbhanu Chatterjee
@file ion/services/dm/ingestion/example/ingestion_script_example.py
@description Create two parallel transforms through calls to the TransformationManagementService
which subscribe to a single stream.
"""

from interface.services.dm.iingestion_management_service import IngestionManagementServiceClient
from ion.services.dm.distribution.pubsub_management_service import PubsubManagementService
from pyon.public import IonObject, RT, log, AT
from pyon.ion.endpoint import ProcessPublisher
from pyon.public import log, StreamProcess

#########################################################################################
# Run this part first in the pycc container
#########################################################################################
ingestion_client = IngestionManagementServiceClient(node = cc.node)

# create transforms... queues will be created in this step
ingestion_configuration_id = ingestion_client.create_ingestion_configuration(exchange_point_id='science_data', couch_storage={},\
    hfd_storage={},  number_of_workers=2, default_policy={})
# activates the transforms... so bindings will be created in this step
ingestion_client.activate_ingestion_configuration(ingestion_configuration_id)

############################################################################################
# Copy the stream_id that is there in the the binding....in rabbit broker web management
# and paste it as a value in out_stream
############################################################################################
# messages will be produced and published
id_p = cc.spawn_process('ingestion_queue', 'ion.services.dm.ingestion.ingestion_example', 'IngestionExampleProducer',\
        {'process':{'type':'stream_process','publish_streams':{'out_stream':'ff17005d70694aca864f76f52a03fd61'}},\
         'stream_producer':{'interval':4000}})
cc.proc_manager.procs['%s.%s' %(cc.id,id_p)].start()