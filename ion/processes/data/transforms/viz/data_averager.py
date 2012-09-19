

'''
@author Raj Singh
@description transforms for averaging data to create a multi-resolution tree
'''

from pyon.ion.transform import TransformFunction
from pyon.service.service import BaseService
from pyon.core.exception import BadRequest
from pyon.public import IonObject, RT, log


mr_tree_order = 4   # A quad tree maps well to the notion of time .. secs, mins, hour, days etc
var_to_skip = ['latitude', 'lat', 'longitude', 'lon', 'depth'] # The variables in this list are not supposed to be averaged

class VizTransformDataAvg(TransformFunction):

    """
    This class is used for data coming on the incoming streams.

    """


    def on_start(self):
        super(VizTransformDataAvg,self).on_start()

        #init variables
        self.leaf_nodes = {} # dictionary stores the node values till we have enough to create an average

        return

    def execute(self, granule):

        log.debug('(Data Averager transform): Received Viz Data Packet' )

        # parse the incoming data
        #@TODO: PointSupp stuff is deprecated
        psd = PointSupplementStreamParser(stream_definition=self.incoming_stream_def, stream_granule=granule)

        var_val = []
        for var_name in psd.list_field_names():
            var_val = psd.get_values(var_name)
            self.leaf_nodes[var_name] = []

            # start averaging once we have a whole set of values
            for val_idx in range(0,len(var_val),mr_tree_order):

                if (val_idx + mr_tree_order - 1) < len(var_val):
                    sum = 0
                    for i in range(mr_tree_order):
                        sum += var_val[val_idx + i]
                    avg = sum / mr_tree_order

                    self.leaf_nodes[var_name].append(avg)


        return avg_value
