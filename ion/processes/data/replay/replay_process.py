#!/usr/bin/env python
'''
@author Luke Campbell <LCampbell@ASAScience.com>
@file ion/processes/data/replay/replay_process.py
@date 06/14/12 13:31
@description Implementation for a replay process.
'''


from interface.services.dm.ireplay_process import BaseReplayProcess
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient
from pyon.core.exception import BadRequest, IonException, NotFound
from pyon.core.object import IonObjectDeserializer
from pyon.core.bootstrap import get_obj_registry
from gevent.event import Event
from pyon.datastore.datastore import DataStore
import gevent
from ion.services.dm.inventory.dataset_management_service import DatasetManagementService
from ion.services.dm.utility.granule_utils import CoverageCraft

class ReplayProcessException(IonException):
    """
    Exception class for IngestionManagementService exceptions. This class inherits from IonException
    and implements the __str__() method.
    """
    def __str__(self):
        return str(self.get_status_code()) + str(self.get_error_message())


class ReplayProcess(BaseReplayProcess):
    '''
    ReplayProcess - A process spawned for the purpose of replaying data
    --------------------------------------------------------------------------------
    Configurations
    ==============
    process:
      dataset_id:      ""     # Dataset to be replayed
      delivery_format: {}     # Delivery format to be replayed back (unused for now)
      query:
        start_time: 0         # Start time (index value) to be replayed
        end_time:   0         # End time (index value) to be replayed
        parameters: []        # List of parameters to form in the granule
      

    '''
    process_type = 'standalone'

    def __init__(self, *args, **kwargs):
        super(ReplayProcess,self).__init__(*args,**kwargs)
        self.deserializer = IonObjectDeserializer(obj_registry=get_obj_registry())

    def on_start(self):
        '''
        Starts the process
        '''
        super(ReplayProcess,self).on_start()
        dsm_cli = DatasetManagementServiceClient()

        self.dataset_id      = self.CFG.get_safe('process.dataset_id', None)
        self.delivery_format = self.CFG.get_safe('process.delivery_format',{})
        self.start_time      = self.CFG.get_safe('process.query.start_time', None)
        self.end_time        = self.CFG.get_safe('process.query.end_time', None)
        self.parameters      = self.CFG.get_safe('process.query.parameters',None)
        self.publishing      = Event()

        if self.dataset_id is None:
            raise BadRequest('dataset_id not specified')

        self.dataset = dsm_cli.read_dataset(self.dataset_id)



    def execute_retrieve(self):
        '''
        execute_retrieve Executes a retrieval and returns the result 
        as a value in lieu of publishing it on a stream
        '''
        coverage = DatasetManagementService._get_coverage(self.dataset_id)
        crafter = CoverageCraft(coverage)
        #@todo: add bounds checking to ensure the dataset being retrieved is not too large
        crafter.sync_rdt_with_coverage(start_time=self.start_time,end_time=self.end_time,parameters=self.parameters)
        granule = crafter.to_granule()
        return granule

    def execute_replay(self):
        '''
        execute_replay Performs a replay and publishes the results on a stream. 
        '''
        if self.publishing.is_set():
            return False
        gevent.spawn(self.replay)
        return True

    def replay(self):
        self.publishing.set() # Minimal state, supposed to prevent two instances of the same process from replaying on the same stream
        granule = self.execute_retrieve()
        self.output.publish(granule)
        self.output.publish({})
        self.publishing.clear()
        return True

    @classmethod
    def get_last_granule(cls, container, dataset_id):
        dsm_cli = DatasetManagementServiceClient()
        dataset = dsm_cli.read_dataset(dataset_id)
        cc = container
        datastore_name = dataset.datastore_name
        view_name = dataset.view_name
        
        datastore = cc.datastore_manager.get_datastore(datastore_name, DataStore.DS_PROFILE.SCIDATA)

        opts = dict(
            start_key = [dataset_id, {}],
            end_key   = [dataset_id, 0], 
            descending = True,
            limit = 1,
            include_docs = True
        )

        results = datastore.query_view(view_name,opts=opts)
        if not results:
            raise NotFound('A granule could not be located.')
        if results[0] is None:
            raise NotFound('A granule could not be located.')
        doc = results[0].get('doc')
        if doc is None:
            return None

        ts = float(doc.get('ts_create',0))

        coverage = DatasetManagementService._get_coverage(dataset_id)
        
        black_box = CoverageCraft(coverage)
        black_box.sync_rdt_with_coverage(start_time=ts,end_time=None)
        granule = black_box.to_granule()

        return granule

    @classmethod
    def get_last_values(cls, dataset_id):
        coverage = DatasetManagementService._get_coverage(dataset_id)
        
        black_box = CoverageCraft(coverage)
        black_box.sync_rdt_with_coverage(tdoa=slice(-1,None))
        granule = black_box.to_granule()
        return granule


        




