#!/usr/bin/env python
'''
@author Luke Campbell <LCampbell@ASAScience.com>
@file ion/processes/data/replay/binary_replay.py
@date Wed Aug 22 09:04:53 EDT 2012
@brief Replay process for binary data
'''

from interface.services.dm.ireplay_process import BaseReplayProcess
from interface.services.dm.ipreservation_management_service import PreservationManagementServiceClient
from pyon.util.log import log
from gevent.event import Event
import gevent
class BinaryReplayProcess(BaseReplayProcess):
    process_type = 'standalone'

    def on_start(self):
        super(BinaryReplayProcess,self).on_start()
        self.preservation_management = PreservationManagementServiceClient()
        self.publishing = Event()

        self.stream_id = self.CFG.get_safe('process.query.file_stream_id')
        self.file_id   = self.CFG.get_safe('process.query.file_group_id')
        self.file_name = self.CFG.get_safe('process.query.file_name')


    def execute_retrieve(self):
        owner_id = self.stream_id
        group_id = self.file_id
        name     = self.file_name

        ds = self.container.datastore_manager.get_datastore('filesystem')

        if name:
            opts = {
                'key' : [owner_id, group_id, name]
            }
        else:
            opts = {
                'start_key' : [owner_id, group_id, 0],
                'end_key'   : [owner_id, group_id, {}]
            }

        res = ds.query_view('catalog/file_by_owner', opts=opts)
        file_ids = [i['value'] for i in res]
        for file_id in file_ids:
            data, digest = self.preservation_management.read_file(file_id)
            yield data

        return

    def execute_replay(self): #pragma no cover
        if self.publishing.is_set():
            return False
        gevent.spawn(self.replay)
        return True

    def replay(self): # pragma no cover
        self.publishing.set()
        for file_packet in self.execute_retrieve():
            self.output.publish({'body':file_packet})
            log.info('Publishing file')
        self.output.publish({})
        self.publishing.clear()
        return True


                
