#!/usr/bin/env python
# -*- coding: utf8 -*- 
'''
@author Luke Campbell <LCampbell@ASAScience.com>
@file ion/clientes/data/replay/replay_client.py
@date Thu Sep 27 14:53:30 EDT 2012
@description Implementation for a replay process.
'''
from ion.services.cei.process_dispatcher_service import ProcessStateGate

from interface.services.dm.ireplay_process import ReplayProcessClient, ReplayProcessProcessClient
from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceClient, ProcessDispatcherServiceProcessClient
from interface.objects import ProcessStateEnum

class ReplayClient(object):
    '''
    Provides a wrapper interface to a Replay Process/Agent Client.

    Use this to interact with a replay process.

    First define the replay. Then, create a ReplayClient with the process id (and process if in a process/service). Then call start_replay_agent to spin up the replay process with CEI. After the client calls start_replay_agent, a client can interact with the replay process with this client.

    replay_id, process_id = data_retreiver.define_replay(dataset_id=dataset_id, stream_id=replay_stream_id)
    replay_client = ReplayClient(process_id, process=self)

    data_retriever.start_replay_agent(replay_id)


    replay_client.start_replay()
    replay_client.pause_replay()
    replay_client.resume_replay()
    '''
    replay_process_id  = ''
    process            = None
    process_started    = False
    client             = None
    
    def __init__(self, process_id, process=None):
        self.replay_process_id = process_id
        self.process = process

        if self.process:
            self.client = ReplayProcessProcessClient(process=self.process, to_name=self.replay_process_id)
        else:
            self.client = ReplayProcessClient(self.replay_process_id)


    def await_agent_ready(self, replay_timeout=5):
        '''
        Determines if the process has been started
        @param replay_timeout Time to wait before raising a timeout
        @retval               True if the process has been started
        '''
        if self.process:
            pd_cli = ProcessDispatcherServiceProcessClient(process=self.process)
        else:
            pd_cli = ProcessDispatcherServiceClient()
        process_gate = ProcessStateGate(pd_cli.read_process, self.replay_process_id, ProcessStateEnum.SPAWN)
        return process_gate.await(replay_timeout)

    def start_replay(self):
        '''
        Begins a replay
        '''
        if not self.process_started:
            self.await_agent_ready()
            self.process_started = True

        self.client.execute_replay()

    def pause_replay(self):
        '''
        Pauses a replay
        '''
        self.client.pause()

    def resume_replay(self):
        '''
        Resumes a replay after a pause
        '''
        self.client.resume()

    def stop_replay(self):
        '''
        Stops the replay
        '''
        self.client.stop()

