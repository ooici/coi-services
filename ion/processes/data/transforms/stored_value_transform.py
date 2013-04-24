#!/usr/bin/env python
'''
@author Luke Campbell
@date Sun Mar 31 16:37:21 EDT 2013
'''

from ion.core.process.transform import TransformStreamListener
from ion.util.stored_values import StoredValueManager
from ion.services.dm.utility.granule import RecordDictionaryTool

import numpy as np

class StoredValueTransform(TransformStreamListener):
    '''
    Receives granules from a stream and persists the latest value in the object store

    Background:
        Platforms publish geospatial information on a separate stream from the 
        instruments. Various data products require geospatial information about
        the instrument to calculate the variables. This component persists the
        latest value in a simple data storage container where complex data
        containers can access it.
    '''
        
    def on_start(self):
        TransformStreamListener.on_start(self)
        self.document_key = self.CFG.get_safe('process.document_key')
        self.stored_value_manager = StoredValueManager(self.container)

    def recv_packet(self, msg, route, stream_id):
        rdt = RecordDictionaryTool.load_from_granule(msg)

        document = {}

        for k,v in rdt.iteritems():
            value_array = np.atleast_1d(v[:])
            if 'f' in value_array.dtype.str:
                document[k] = float(value_array[-1])
            elif 'i' in value_array.dtype.str:
                document[k] = int(value_array[-1])

        self.stored_value_manager.stored_value_cas(self.document_key, document)

