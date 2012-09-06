#!/usr/bin/env python
'''
@author Luke Campbell <LCampbell@ASAScience.com>
@file ion/processes/data/ingestion/blob_ingestion_worker.py
@date Fri Aug 17 13:27:15 EDT 2012
@brief Ingestion worker for BLOB data.
'''

from pyon.ion.transforma import TransformStreamListener
from interface.services.dm.ipreservation_management_service import PreservationManagementServiceClient
from pyon.util.log import log
class BlobIngestionWorker(TransformStreamListener):
    def __init__(self):
        super(BlobIngestionWorker,self).__init__()
    
    def recv_packet(self, msg, stream_route, stream_id):
        return self.persist(msg,stream_id)

    def persist(self, packet, stream_id):
        if not isinstance(packet, dict):
            log.error('Failed to persist packet, unknown packet format: %s', type(packet))
            return

        if not ('meta' in packet and 'body' in packet):
            log.error('Failed to persist packet, unkown packet format.')
            return
        preservation_management = PreservationManagementServiceClient()
        packet['meta'].owner_id = stream_id
        preservation_management.persist_file(file_data=packet['body'], metadata=packet['meta'])

         
        
