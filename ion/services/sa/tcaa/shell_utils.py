
"""
bin/pycc -r res/deploy/r2deploy.yml
import ion.services.sa.tcaa.shell_utils as utils
spargs = {'type':'terrestrial'}
spargs = utils.tcaa_args(**spargs)
cc.spawn_process(**spargs)
client = utils.tcaa_client(**spargs)

ls()
Edwards-MacBook-Pro_local_3983.36: TerrestrialEndpoint(name=terrestrial_endpointremote1,id=Edwards-MacBook-Pro_local_3983.36,type=service)

TelemetryStatusType.AVAILABLE
TelemetryStatusType.UNAVAILABLE
"""


from pyon.util.context import LocalContextMixin
from ion.services.sa.tcaa.remote_endpoint import RemoteEndpointClient
from ion.services.sa.tcaa.terrestrial_endpoint import TerrestrialEndpointClient
from pyon.event.event import EventPublisher, EventSubscriber
from interface.objects import TelemetryStatusType
from pyon.public import IonObject

import gevent

import random
import string
import time

TERRESTRIAL_PLATFORM_ID = 'abc123'
REMOTE_PLATFORM_ID = 'abc123R'
TERRESTRIAL_HOST = 'localhost'
REMOTE_HOST = 'localhost'
TERRESTRIAL_PORT = 5777
REMOTE_PORT = 5888
XS_NAME = 'remote1'

def tcaa_link(up_down, **kwargs):
    """
    """
    if up_down:
        status = TelemetryStatusType.AVAILABLE
    else:
        status = TelemetryStatusType.UNAVAILABLE        
        
    platform_id = kwargs['config']['platform_resource_id']
        
    pub = EventPublisher()
    pub.publish_event(
        event_type='PlatformTelemetryEvent',
        origin=platform_id,
        status = status)
    
def tcaa_client(**kwargs):
    """
    """
    cls = kwargs['cls']
    listen_name = kwargs['name']
    
    if cls == 'TerrestrialEndpoint':
        client = TerrestrialEndpointClient(process=FakeProcess(),
                                           to_name=listen_name)
    elif cls == 'RemoteEndpoint':
        client = RemoteEndpointClient(process=FakeProcess(),
                                           to_name=listen_name)
        
    else:
        raise ValueError('Bad endpoint cls value.')    
    
    return client
    
def tcaa_args(**kwargs):
    """
    """
    cls = kwargs.get('cls', 'TerrestrialEndpoint')
    xs_name = kwargs.get('xs_name', XS_NAME)
         
    if cls == 'TerrestrialEndpoint':
        platform_id = kwargs.get('platform_id', TERRESTRIAL_PLATFORM_ID)
        this_port = kwargs.get('this_port', TERRESTRIAL_PORT)
        other_host = kwargs.get('other_host', REMOTE_HOST)
        other_port = kwargs.get('other_port', REMOTE_PORT)
        listen_name = 'terrestrial_endpoint' + xs_name
        module = 'ion.services.sa.tcaa.terrestrial_endpoint'
        
    elif cls == 'RemoteEndpoint':
        platform_id = kwargs.get('platform_id', REMOTE_PLATFORM_ID)
        this_port = kwargs.get('this_port', REMOTE_PORT)
        other_host = kwargs.get('other_host', TERRESTRIAL_HOST)
        other_port = kwargs.get('other_port', TERRESTRIAL_PORT)
        listen_name = 'remote_endpoint' + xs_name
        module = 'ion.services.sa.tcaa.remote_endpoint'
    
    else:
        raise ValueError('Bad endpoint cls value.')

    config = tcaa_config(xs_name, listen_name, platform_id, this_port,
                         other_host, other_port)

    spargs = {
        'name' : listen_name,
        'module' : module,
        'cls' : cls,
        'config' : config
    }
        
    return spargs

def tcaa_config(xs_name, listen_name, platform_id, this_port, other_host, other_port):
    """
    """
    config = {
        'other_host' : other_host,
        'other_port' : other_port,
        'this_port' : this_port,
        'platform_resource_id' : platform_id,
        'xs_name' : xs_name,
        'process' : {
            'listen_name' : listen_name
        }
    }
    return config 

def make_fake_command(size):
    """
    Build a fake command for use in tests.
    """
    random_str = ''.join([random.choice(string.ascii_lowercase) for x in xrange(size)])
    cmdstr = 'fake_cmd'
    cmd = IonObject('RemoteCommand',
                         resource_id='fake_id',
                         command='fake_cmd',
                         args=[random_str],
                         kwargs={'payload':random_str})
    return cmd

class FakeProcess(LocalContextMixin):
    """
    A fake process used because the test case is not an ion process.
    """
    name = ''
    id=''
    process_type = ''

class PlatformCommander():
    """
    """
    def __init__(self, delay, xs_name, resource_id, logfile):
        """
        """
        self._delay = delay
        self._xs_name = xs_name
        self._resource_id = resource_id
        self._logfile = logfile
        self._platform_sub = None
        self._resource_sub = None
        self._gl = None
        self._start_subscribers()
        self._te_client = TerrestrialEndpointClient(
            process=FakeProcess(),
            to_name='terrestrial_endpoint'+self._xs_name)
        
    def start(self):
        """
        """
        self._gl = gevent.spawn(self._loop)
            # Create a terrestrial client.

    def stop(self):
        """
        """
        if self._gl:
            self._gl.kill()
            self._gl.join()
            self._gl = None
    
    def done(self):
        """
        """
        self._platform_sub.stop()
        self._resource_sub.stop()
     
    def _loop(self):
        """
        """
        while True:
            gevent.sleep(self._delay)
            # send command here.
        
    def _start_subscribers(self):
        """
        """
        self._platform_sub = EventSubscriber(
            event_type='PlatformEvent',
            callback=self._consume_event,
            origin=self._xs_name)
        
        self._resource_sub = EventSubscriber(
            event_type='RemoteCommandResult',
            callback=self._consume_event,
            origin=self._resource_id)            
        
class PlatformTimer():
    """
    """
    def __init__(self, platform_id, period, uptime):
        """
        """
        self._platform_id = platform_id
        self._period = period
        self._uptime = uptime
        self._starttime = None
        self._status = TelemetryStatusType.UNAVAILABLE
        self._pub = EventPublisher()
        self._gl = None
        
    def start(self):
        """
        """
        self._gl = gevent.spawn(self._loop)
        
    def _loop(self):
        """
        """
        next_time = None
        while True:
            cur_time = time.gmtime(time.time())
            #print 'time is %s' % str(cur_time)
            (h, m, s) = (cur_time[3], cur_time[4], cur_time[5])
            if not next_time:
                next_time = self._next_time(h, m, s, self._period)
            if not self._starttime:
                if h == next_time[0] and m == next_time[1] and s >= next_time[2]:
                    next_time = self._next_time(h, m, s, self._period)
                    self._starttime = time.time()
                    print '## Telemetry UP at %s' % str(cur_time)
                    """
                    self._pub.publish_event(
                        event_type='PlatformTelemetryEvent',
                        origin=self._platform_id,
                        status = TelemetryStatusType.AVAILABLE)
                    """
            elif time.time() - self._starttime >= self._uptime:
                self._starttime = None
                print '## Telemetry DOWN at %s' % str(cur_time)
                """
                self._pub.publish_event(
                    event_type='PlatformTelemetryEvent',
                    origin=self._platform_id,
                    status = TelemetryStatusType.UNAVAILABLE)
                """
            
            gevent.sleep(1)
                        
    def stop(self):
        """
        """
        if self._gl:
            self._gl.kill()
            self._gl = None
            
    def _next_time(self, h, m, s, p):
        """
        """
        
        unit = p[-1].lower()
        period = int(p[:-1])
        
        if unit == 'h':
            h_n = ((h/period)+1)*period
            m_n = 0
            s_n = 0
        
        elif unit == 'm':
            h_n = h
            m_n = ((m/period)+1)*period
            s_n = 0
        
        elif unit == 's':
            h_n = h
            m_n = m
            s_n = ((s/period)+1)*period
        
        else:
            raise ValueError('Bad frequency unit.')
            
        if s_n >= 60:
            s_n = s_n % 60
            m_n += 1
        
        if m_n >= 60:
            m_n = m_n % 60
            h_n += 1
            
        if h_n >= 24:
            h_n = h_n % 24
            
        return (h_n, m_n, s_n)
        
"""
def testclock(p):
    
    next_time = None
    for d in range(2):
        for h in range(24):
            for m in range(60):
                for s in range(60):
                    if not next_time:
                        next_time = timetest(h, m, s, p)
                    elif h == next_time[0] and m == next_time[1] and s >= next_time[2]:
                        next_time = timetest(h, m, s, p)
                        print "## sample triggered at %i:%i:%i" % (h, m, s)

"""