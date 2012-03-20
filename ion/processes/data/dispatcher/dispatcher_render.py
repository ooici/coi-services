'''
@author Luke Campbell
@file ion/processes/data/dispatcher/dispatcher_render.py
'''
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from ion.services.ans.visualization_service import VizTransformProcForMatplotlibGraphs
from pyon.util.containers import DotDict

class DispatcherRender(VizTransformProcForMatplotlibGraphs):
    class _visualizer(object):
        def __init__(self, queue):
            self.queue = queue
        def submit_mpl_image(self, data_product_id, image_string, file_name):
            self.queue.append({
                'data_product_id':data_product_id,
                'image_string':image_string,
                'file_name':file_name
            })


    def __init__(self, data_product_id, stream_def_id):
        super(DispatcherRender, self).__init__()


        self.initDataFlag = True
        self.graph_data = {}
        self.results = list()
        self.rr_cli = ResourceRegistryServiceClient()
        self.data_product_id = data_product_id
        self.stream_def_id = stream_def_id
        # throws NotFound
        self.stream_def = self.rr_cli.read(stream_def_id)
        self.vs_cli = self._visualizer(self.results)




