"""
@author Swarbhanu Chatterjee
@file ion/services/dm/ingestion/example/ingestion_script_example.py
@description Create two parallel transforms through calls to the TransformationManagementService
which subscribe to a single stream.
"""

from interface.services.dm.itransform_management_service import TransformManagementServiceClient
from interface.services.dm.iingestion_management_service import IngestionManagementServiceClient
from pyon.public import IonObject, RT, log, AT
from pyon.ion.endpoint import ProcessPublisher
from pyon.public import log, StreamProcess
import time

tms_client = TransformManagementServiceClient(node=cc.node)
ingestion_client = IngestionManagementServiceClient(node = cc.node)

# create two transforms to listen to the stream....
# for this use an  ingestion_management_service client to create a configuration and activate it



#id_p = cc.spawn_process('myproducer', 'examples.stream.stream_producer', 'StreamProducer', {'process':{'type':"agent"},'stream_producer':{'interval':4000,'routing_key':'glider_data'}})
#id = cc.spawn_process('myconsumer', 'examples.stream.stream_consumer', 'StreamConsumer', {'process':{'type':"stream_process",'listen_name':'science.data_ingestion_worker'}})




#
#id = cc.spawn_process('myconsumer', 'ion.services.dm.ingestion.ingestion_example', 'IngestionExample', {'process':{'type':"stream_process",'listen_name':'science_data.myproducer'}})
#cc.proc_manager.procs['%s.%s' %(cc.id,id)].start()

# spawn the stream publisher process...
# this publishes messages in streams
id_p = cc.spawn_process('ingestion_queue', 'ion.services.dm.ingestion.ingestion_example', 'IngestionExampleProducer', {'process':{'type':'stream_process','publish_streams':{'out_stream':'forced'}},'stream_producer':{'interval':4000}})
cc.proc_manager.procs['%s.%s' %(cc.id,id_p)].start()

# create transforms
ingestion_configuration_id = ingestion_client.create_ingestion_configuration(exchange_point_id='science.data', couch_storage={},\
    hfd_storage={},  number_of_workers=2, default_policy={})
ingestion_client.activate_ingestion_configuration(ingestion_configuration_id)

