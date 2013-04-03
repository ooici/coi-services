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
    def on_start(self):
        TransformStreamListener.on_start(self)
        self.document_key = self.CFG.get_safe('process.document_key')
        self.stored_value_manager = StoredValueManager(self.container)

    def recv_packet(self, msg, route, stream_id):
        print 'Got RDT'
        rdt = RecordDictionaryTool.load_from_granule(msg)

        document = {}

        for k,v in rdt.iteritems():
            value_array = np.atleast_1d(v[:])
            print 'Some Values: ', repr(value_array)
            if 'f' in value_array.dtype.str:
                print 'Storing'
                if value_array.shape == (1,):
                    document[k] = float(value_array[0])
                else:
                    document[k] = [float(i) for i in value_array]
            elif 'i' in value_array.dtype.str:
                print 'Storing'
                if value_array.shape == (1,):
                    document[k] = int(value_array[0])
                else:
                    document[k] = [int(i) for i in value_array]

        self.stored_value_manager.stored_value_cas(self.document_key, document)

