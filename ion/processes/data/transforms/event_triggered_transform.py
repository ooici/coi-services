#!/usr/bin/env python

'''
@brief The EventTriggeredTransform transform listens for a particular kind of event. When it receives the event,
it goes into a woken up state. When it is awake it processes data according to its transform algorithm.

@author Swarbhanu Chatterjee
'''
from pyon.ion.transforma import TransformEventListener, TransformDataProcess
from pyon.ion.transforma import TransformFunction
from pyon.util.log import log
from pyon.util.containers import get_safe
from pyon.core.exception import BadRequest
from pyon.event.event import EventSubscriber, EventPublisher
from pyon.ion.stream import SimpleStreamSubscriber
from coverage_model.parameter import ParameterDictionary, ParameterContext
from coverage_model.parameter_types import QuantityType
from coverage_model.basic_types import AxisTypeEnum
from ion.core.function.transform_function import SimpleGranuleTransformFunction
from ion.services.dm.utility.granule.record_dictionary import RecordDictionaryTool

class EventTriggeredTransform(TransformEventListener, TransformDataProcess):

    def on_start(self):
        super(EventTriggeredTransform, self).on_start()

        self.queue_name = self.CFG.get_safe('process.queue_name',self.id)
        self.awake = False

        if self.CFG.process.has_key('publish_stream'):
            self.publish_stream = self.CFG.process.publish_streams.publish_stream

    def on_quit(self):
        '''
        Stop the subscriber
        '''
        super(EventTriggeredTransform, self).on_quit()

    def process_event(self, msg, headers):
        '''
        Use CEI to launch the EventTriggeredTransform
        '''

        # ------------------------------------------------------------------------------------
        # Process Spawning
        # ------------------------------------------------------------------------------------
        self.awake = True

    def recv_packet(self, packet, stream_route, stream_id):
        '''
        Only when the transform has been worken up does it process the received packets
        according to its algorithm and the result is published
        '''

        if self.awake: # if the transform is awake
            if packet == {}:
                return

            granule = TransformAlgorithm.execute([packet])
            self.publish(msg=granule, stream_id=self.publish_stream)
        else:
            return # the transform does not do anything with received packets when it is not yet awake

    def publish(self, msg, stream_id):
        self.publisher.publish(msg)


class TransformAlgorithm(SimpleGranuleTransformFunction):

    @staticmethod
    @SimpleGranuleTransformFunction.validate_inputs
    def execute(input=None, context=None, config=None, params=None, state=None):
        '''
        @param input Granule
        @retval result Granule
        '''
        rdt = RecordDictionaryTool.load_from_granule(input)

        conductivity = get_safe(rdt, 'conductivity')

        longitude = get_safe(rdt, 'lon')
        latitude = get_safe(rdt, 'lat')
        time = get_safe(rdt, 'time')
        depth = get_safe(rdt, 'depth')

        # create parameter settings
        cond_pdict = TransformAlgorithm._create_parameter()

        cond_value = (conductivity / 100000.0) - 0.5
        # build the granule for conductivity
        result = TransformAlgorithm._build_granule_settings(cond_pdict, 'conductivity', cond_value, time, latitude, longitude, depth)

        return result

    @staticmethod
    def _create_parameter():

        pdict = ParameterDictionary()

        pdict = TransformAlgorithm._add_location_time_ctxt(pdict)

        cond_ctxt = ParameterContext('conductivity', param_type=QuantityType(value_encoding=np.float32))
        cond_ctxt.uom = 'unknown'
        cond_ctxt.fill_value = 0e0
        pdict.add_context(cond_ctxt)

        return pdict

    @staticmethod
    def _add_location_time_ctxt(pdict):

        t_ctxt = ParameterContext('time', param_type=QuantityType(value_encoding=np.int64))
        t_ctxt.reference_frame = AxisTypeEnum.TIME
        t_ctxt.uom = 'seconds since 1970-01-01'
        t_ctxt.fill_value = 0x0
        pdict.add_context(t_ctxt)

        lat_ctxt = ParameterContext('lat', param_type=QuantityType(value_encoding=np.float32))
        lat_ctxt.reference_frame = AxisTypeEnum.LAT
        lat_ctxt.uom = 'degree_north'
        lat_ctxt.fill_value = 0e0
        pdict.add_context(lat_ctxt)

        lon_ctxt = ParameterContext('lon', param_type=QuantityType(value_encoding=np.float32))
        lon_ctxt.reference_frame = AxisTypeEnum.LON
        lon_ctxt.uom = 'degree_east'
        lon_ctxt.fill_value = 0e0
        pdict.add_context(lon_ctxt)

        depth_ctxt = ParameterContext('depth', param_type=QuantityType(value_encoding=np.float32))
        depth_ctxt.reference_frame = AxisTypeEnum.HEIGHT
        depth_ctxt.uom = 'meters'
        depth_ctxt.fill_value = 0e0
        pdict.add_context(depth_ctxt)

        return pdict

    @staticmethod
    def _build_granule_settings(param_dictionary=None, field_name='', value=None, time=None, latitude=None, longitude=None, depth=None):

        root_rdt = RecordDictionaryTool(param_dictionary=param_dictionary)

        root_rdt[field_name] = value

        if not time is None:
            root_rdt['time'] = time
        if not latitude is None:
            root_rdt['lat'] = latitude
        if not longitude is None:
            root_rdt['lon'] = longitude
        if not depth is None:
            root_rdt['depth'] = depth

        log.debug("CTDL1ConductivityTransform:_build_granule_settings: logging published Record Dictionary:\n %s", str(root_rdt.pretty_print()))

        return root_rdt.to_granule()
