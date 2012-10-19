

'''
@author Raj Singh
@description transforms for averaging data to create a multi-resolution tree
'''
from ion.core.process.transform import TransformStreamListener
from ion.services.dm.utility.granule.record_dictionary import RecordDictionaryTool
from pyon.service.service import BaseService
from pyon.core.exception import BadRequest
from pyon.public import IonObject, RT, log

#from prototype.sci_data.stream_defs import SBE37_CDM_stream_definition, SBE37_RAW_stream_definition

mr_tree_order = 4   # A quad tree maps well to the notion of time .. secs, mins, hour, days etc
var_to_skip = ['latitude', 'lat', 'longitude', 'lon', 'pressure'] # The variables in this list are not supposed to be averaged

class VizTransformDataAvg(TransformStreamListener):

    """
    This class is used for data coming on the incoming streams.

    """

    #outgoing_stream_def = incoming_stream_def = SBE37_CDM_stream_definition()

    def on_start(self):
        super(VizTransformDataAvg,self).on_start()

        #init variables
        self.leaf_nodes = {} # dictionary stores the node values till we have enough to create an average

        return

    def execute(self, granule):

        log.debug('(Data Averager transform): Received Viz Data Packet' )

        rdt = RecordDictionaryTool.load_from_granule(granule)

        var_val = []
        for var_name in rdt.iterkeys():
            var_val = rdt[var_name]
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
