'''
@author Luke Campbell
@file ion/processes/data/dispatcher/dispatcher_render.py
'''
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from ion.services.ans.visualization_service import VizTransformProcForMatplotlibGraphs
from pyon.util.containers import DotDict

class DispatcherRender(VizTransformProcForMatplotlibGraphs):
    '''
    DispatcherRender is a class that is meant to be instantiated for the sole purpose of rendering a stream into an image.
    It inherits from VizTransformProcForMatplotlibGraphs and where the process initialization would normally take place in on_start
    it is performed in __init__

    By using an instantiated object we avoid extreme overhead in message traffic between this object and the owner of the granule (Large)
    It is a simple process that is quick and to the point.

    To render a granule:
    1) Instantiate Object
      dispatcher_render = DispatchRender(visual_product_id, stream_definition_id)
    2) Process the granule for rendering
      dispatcher_render.process(granule)
    3) Retrieve queue of processed images when ready
      dispatcher_render.results

    '''
    class _visualizer(object):
        '''
        The inner class takes the place of the visualization service, so in lieu of making a service call
        the png string image is stored in a ready-to-access queue for processing by clients.
        '''
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
        # Initialization flag used by super
        self.initDataFlag = True
        self.graph_data = {}
        # A queue which will hold a list of files rendered, this allows subsequent calls to process
        self.results = list()
        self.rr_cli = ResourceRegistryServiceClient()
        self.data_product_id = data_product_id
        self.stream_def_id = stream_def_id
        # throws NotFound
        self.stream_def = self.rr_cli.read(stream_def_id)

        # Use the inner-class in lieu of a service call
        self.vs_cli = self._visualizer(self.results)




