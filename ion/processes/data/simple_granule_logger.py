
from ion.services.dm.utility.granule.record_dictionary import RecordDictionaryTool
from interface.objects import Granule
from pyon.util.log import log
from pyon.ion.transforma import TransformStreamListener

'''
To Launch:
    from pyon.util.containers import DotDict
    config = DotDict()
    config.process.log = False
    cc.spawn_process('granule_logger','ion.processes.data.simple_granule_logger','SimpleGranuleLogger',config, 'granule_logger')


'''


class SimpleGranuleLogger(TransformStreamListener):
    def on_start(self):
        self.log       = self.CFG.get_safe('process.log', False)

    def on_quit(self):
        self.subscriber.stop()

    def recv_packet(self, msg, stream_route, stream_id):
        if not isinstance(msg,Granule):
            log.warn('Received non-compatible granule in stream %s.', stream_id)
            return

        rdt = RecordDictionaryTool.load_from_granule(msg)
        if self.log:
            log.warn('Received record dictionary:\n%s', rdt.pretty_print())
        else:
            import sys
            print>>sys.stderr, 'Received record dictionary:\n%s' % rdt.pretty_print()

