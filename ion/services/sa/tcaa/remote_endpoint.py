#!/usr/bin/env python
"""
@package ion.services.sa.tcaa.remote_endpoint
@file ion/services/sa/tcaa/remote_endpoint.py
@author Edward Hunter
@brief 2CAA Remote endpoint.
"""

__author__ = 'Edward Hunter'
__license__ = 'Apache 2.0'

# Pyon log and config objects.
from pyon.public import log
from pyon.public import CFG

import uuid
import time

# Pyon exceptions.
from pyon.core.exception import BadRequest
from pyon.core.exception import Conflict

from ion.services.sa.tcaa.r3pc import R3PCServer
from ion.services.sa.tcaa.r3pc import R3PCClient

class RemoteEndpoint(object):
    """
    """
    def __init__(self, *args, **kwargs):
        """
        For framework level code only.
        """
        super(BaseRemoteEndpoint, self).__init__(*args, **kwargs)
        
    ######################################################################    
    # Framework process lifecycle funcitons.
    ######################################################################    

    def on_init(self):
        """
        Application level initializer.
        Setup default internal values.
        """
        super(BaseRemoteEndpoint, self).on_init()
        self._server = None
        self._client = None
        self._terrestrial_host = self.CFG.terrestrial_host
        self._terrestrial_port = self.CFG.terrestrial_port
        self._platform_resource_id = self.CFG.platform_resource_id
        self._remote_port = self.CFG.remote_port
        self._link_status = TelemetryStatusType.UNAVAILABLE
        self._event_subscriber = None
        self._server_greenlet = None
        self._publisher = EventPublisher()

    def on_start(self):
        """
        Process about to be started.
        """
        super(BaseRemoteEndpoint, self).on_start()
        
        # Start the event subscriber.
        self._event_subscriber = EventSubscriber(
            event_type='PlatformTelemetryEvent',
            callback=self._consume_telemetry_event,
            origin=self._platform_resource_id)
        self._event_subscriber.start()
        self._event_subscriber._ready_event.wait(timeout=5)        
        
    def on_stop(self):
        """
        Process about to be stopped.
        """
        self._stop()
        super(BaseRemoteEndpoint, self).on_stop()
    
    def on_quit(self):
        """
        Process terminated following.
        """
        self._stop()
        super(BaseRemoteEndpoint, self).on_quit()

    ######################################################################    
    # Callbacks.
    ######################################################################    

    ######################################################################    
    # Helpers.
    ######################################################################    

    def _stop(self):
        """
        Stop sockets and subscriber.
        """
        if self._event_subscriber:
            self._event_subscriber.stop()
            self._event_subscriber = None
        if self._server:
            self._server.stop()
            self._server = None
        if self._client:
            self._client.stop()
            self._client = None
    
    ######################################################################    
    # Commands.
    ######################################################################    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    

"""
list procs from self.container.proc_manager.list_procs():
EventPersister(name=event_persister,id=Edwards-MacBook-Pro_local_12469.1,type=standalone)
ResourceRegistryService(name=resource_registry,id=Edwards-MacBook-Pro_local_12469.3,type=service)
IdentityManagementService(name=identity_management,id=Edwards-MacBook-Pro_local_12469.5,type=service)
DirectoryService(name=directory,id=Edwards-MacBook-Pro_local_12469.4,type=service)
ExchangeManagementService(name=exchange_management,id=Edwards-MacBook-Pro_local_12469.7,type=service)
PolicyManagementService(name=policy_management,id=Edwards-MacBook-Pro_local_12469.6,type=service)
AgentManagementService(name=agent_management,id=Edwards-MacBook-Pro_local_12469.9,type=service)
OrgManagementService(name=org_management,id=Edwards-MacBook-Pro_local_12469.8,type=service)
NotificationWorker(name=notification_worker_process,id=notification_worker_process4bce5f3e370b469cb87b938b04c15077,type=simple)
ScienceGranuleIngestionWorker(name=ingestion_worker_process,id=ingestion_worker_process578b8f621a2a4b83bd3b85b7bf0a2571,type=simple)
ResourceManagementService(name=resource_management,id=Edwards-MacBook-Pro_local_12469.11,type=service)
ObjectManagementService(name=object_management,id=Edwards-MacBook-Pro_local_12469.10,type=service)
ProcessDispatcherService(name=process_dispatcher,id=Edwards-MacBook-Pro_local_12469.13,type=service)
ServiceManagementService(name=service_management,id=Edwards-MacBook-Pro_local_12469.12,type=service)
PubsubManagementService(name=pubsub_management,id=Edwards-MacBook-Pro_local_12469.15,type=service)
SchedulerService(name=scheduler,id=Edwards-MacBook-Pro_local_12469.14,type=service)
IngestionManagementService(name=ingestion_management,id=Edwards-MacBook-Pro_local_12469.17,type=service)
DatasetManagementService(name=dataset_management,id=Edwards-MacBook-Pro_local_12469.16,type=service)
IndexManagementService(name=index_management,id=Edwards-MacBook-Pro_local_12469.19,type=service)
DataRetrieverService(name=data_retriever,id=Edwards-MacBook-Pro_local_12469.18,type=service)
VisualizationService(name=visualization_service,id=Edwards-MacBook-Pro_local_12469.30,type=service)
BlobIngestionWorker(name=binary_ingestion_worker_process,id=binary_ingestion_worker_processa9101fa59a6b4eefa754ea4c2725d036,type=simple)
ServiceGatewayService(name=service_gateway,id=Edwards-MacBook-Pro_local_12469.32,type=service)
NotificationWorker(name=notification_worker_process,id=notification_worker_process592a5b325b7b4ba7bfefec9e1755e2be,type=simple)
BlobIngestionWorker(name=binary_ingestion_worker_process,id=binary_ingestion_worker_process1c267e0562af442b9ad5a8e5993294e7,type=simple)
DataAcquisitionManagementService(name=data_acquisition_management,id=Edwards-MacBook-Pro_local_12469.24,type=service)
DataProductManagementService(name=data_product_management,id=Edwards-MacBook-Pro_local_12469.25,type=service)
DataProcessManagementService(name=data_process_management,id=Edwards-MacBook-Pro_local_12469.26,type=service)
InstrumentManagementService(name=instrument_management,id=Edwards-MacBook-Pro_local_12469.27,type=service)
UserNotificationService(name=user_notification,id=Edwards-MacBook-Pro_local_12469.20,type=service)
PreservationManagementService(name=preservation_management,id=Edwards-MacBook-Pro_local_12469.21,type=service)
CatalogManagementService(name=catalog_management,id=Edwards-MacBook-Pro_local_12469.22,type=service)
DiscoveryService(name=discovery,id=Edwards-MacBook-Pro_local_12469.23,type=service)
ObservatoryManagementService(name=observatory_management,id=Edwards-MacBook-Pro_local_12469.28,type=service)
WorkflowManagementService(name=workflow_management,id=Edwards-MacBook-Pro_local_12469.29,type=service)


procs by name (self.container.proc_manager.procs_by_name):
data_retriever == DataRetrieverService(name=data_retriever,id=Edwards-MacBook-Pro_local_12469.18,type=service)
agent_management == AgentManagementService(name=agent_management,id=Edwards-MacBook-Pro_local_12469.9,type=service)
event_persister == EventPersister(name=event_persister,id=Edwards-MacBook-Pro_local_12469.1,type=standalone)
identity_management == IdentityManagementService(name=identity_management,id=Edwards-MacBook-Pro_local_12469.5,type=service)
catalog_management == CatalogManagementService(name=catalog_management,id=Edwards-MacBook-Pro_local_12469.22,type=service)
pubsub_management == PubsubManagementService(name=pubsub_management,id=Edwards-MacBook-Pro_local_12469.15,type=service)
notification_worker_process == NotificationWorker(name=notification_worker_process,id=notification_worker_process592a5b325b7b4ba7bfefec9e1755e2be,type=simple)
workflow_management == WorkflowManagementService(name=workflow_management,id=Edwards-MacBook-Pro_local_12469.29,type=service)
data_acquisition_management == DataAcquisitionManagementService(name=data_acquisition_management,id=Edwards-MacBook-Pro_local_12469.24,type=service)
service_gateway == ServiceGatewayService(name=service_gateway,id=Edwards-MacBook-Pro_local_12469.32,type=service)
visualization_service == VisualizationService(name=visualization_service,id=Edwards-MacBook-Pro_local_12469.30,type=service)
resource_registry == ResourceRegistryService(name=resource_registry,id=Edwards-MacBook-Pro_local_12469.3,type=service)
data_process_management == DataProcessManagementService(name=data_process_management,id=Edwards-MacBook-Pro_local_12469.26,type=service)
policy_management == PolicyManagementService(name=policy_management,id=Edwards-MacBook-Pro_local_12469.6,type=service)
index_management == IndexManagementService(name=index_management,id=Edwards-MacBook-Pro_local_12469.19,type=service)
discovery == DiscoveryService(name=discovery,id=Edwards-MacBook-Pro_local_12469.23,type=service)
user_notification == UserNotificationService(name=user_notification,id=Edwards-MacBook-Pro_local_12469.20,type=service)
data_product_management == DataProductManagementService(name=data_product_management,id=Edwards-MacBook-Pro_local_12469.25,type=service)
ingestion_management == IngestionManagementService(name=ingestion_management,id=Edwards-MacBook-Pro_local_12469.17,type=service)
dataset_management == DatasetManagementService(name=dataset_management,id=Edwards-MacBook-Pro_local_12469.16,type=service)
scheduler == SchedulerService(name=scheduler,id=Edwards-MacBook-Pro_local_12469.14,type=service)
preservation_management == PreservationManagementService(name=preservation_management,id=Edwards-MacBook-Pro_local_12469.21,type=service)
exchange_management == ExchangeManagementService(name=exchange_management,id=Edwards-MacBook-Pro_local_12469.7,type=service)
resource_management == ResourceManagementService(name=resource_management,id=Edwards-MacBook-Pro_local_12469.11,type=service)
service_management == ServiceManagementService(name=service_management,id=Edwards-MacBook-Pro_local_12469.12,type=service)
binary_ingestion_worker_process == BlobIngestionWorker(name=binary_ingestion_worker_process,id=binary_ingestion_worker_process1c267e0562af442b9ad5a8e5993294e7,type=simple)
object_management == ObjectManagementService(name=object_management,id=Edwards-MacBook-Pro_local_12469.10,type=service)
observatory_management == ObservatoryManagementService(name=observatory_management,id=Edwards-MacBook-Pro_local_12469.28,type=service)
org_management == OrgManagementService(name=org_management,id=Edwards-MacBook-Pro_local_12469.8,type=service)
directory == DirectoryService(name=directory,id=Edwards-MacBook-Pro_local_12469.4,type=service)
ingestion_worker_process == ScienceGranuleIngestionWorker(name=ingestion_worker_process,id=ingestion_worker_process578b8f621a2a4b83bd3b85b7bf0a2571,type=simple)


"""
    