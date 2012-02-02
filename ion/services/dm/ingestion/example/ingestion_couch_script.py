#__author__ = 'SChatterjee'
#
#from interface.services.dm.iingestion_management_service import IngestionManagementServiceClient
#from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
#from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
#
#
#from pyon.public import IonObject, RT, log, PRED
#from pyon.ion.endpoint import ProcessPublisher
#from pyon.public import log, StreamProcess
#
##########################################################################################
## Run this part first in the pycc container
##########################################################################################
#ingestion_client = IngestionManagementServiceClient(node = cc.node)
#pubsub_client = PubsubManagementServiceClient(node=cc.node)
#
#ctd_output_stream_id = pubsub_client.create_stream(name='ctd_output_stream', original=True)
#
## create transforms... queues will be created in this step
#ingestion_configuration_id = ingestion_client.create_ingestion_configuration(exchange_point_id='science_data', couch_storage={},\
#    hdf_storage={},  number_of_workers=2, default_policy={})
## activates the transforms... so bindings will be created in this step
#ingestion_client.activate_ingestion_configuration(ingestion_configuration_id)
#
## messages produced and published
#id_p = cc.spawn_process('ingestion_queue', 'ion.services.dm.ingestion.example.blog_scraper', 'FeedStreamer',\
#        {'process': {'type':'stream_process', 'listen_name':'do_not_publish', 'publish_streams':{'out_stream':ctd_output_stream_id}},'stream_producer':{'interval':4000}})
#cc.proc_manager.procs['%s.%s' %(cc.id,id_p)].start()
