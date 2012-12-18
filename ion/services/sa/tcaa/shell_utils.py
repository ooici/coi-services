#!/usr/bin/env python
"""
@package ion.services.sa.tcaa.shell_utils
@file ion/services/sa/tcaa/shell_utils.py
@author Edward Hunter
@brief Command line utilities for running 2CAA experiments interactively.
"""

__author__ = 'Edward Hunter'
__license__ = 'Apache 2.0'

"""
# Launch container shell with deploy file and force clean broker.
bin/pycc -fc -r res/deploy/r2deploy.yml

# Import the utilities.
import ion.services.sa.tcaa.shell_utils as u

tc = u.launch_terrestrial()
rc = u.launch_remote()

u.start_subscribers()

u.start_timer('2m',10)
u.start_commander(5,128)

u.stop_commander()
u.stop_timer()

u.publish_link_event(True)

cmd = u.make_fake_command(128)
tc.enqueue_command(cmd)

u.publish_link_event(False)

u.stop_subscribers()

ps()
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
import uuid
import copy
import inspect
import time

print 'Initializing shell utilities...'
cc = inspect.stack()[1][0].f_locals.get('cc', None)
if cc:
    print 'got container: %s' % str(cc)
else:
    print 'Warning: could not access container upon import.'
    
tcaa_args = {
    'terrestrial_platform_id' : 'abc123',
    'remote_platform_id' : 'abc123R',
    'terrestrial_host' : 'localhost',
    'remote_host' : 'localhost',
    'terrestrial_port' : 5777,
    'remote_port' : 5888,
    'xs_name' : 'remote1',
    'terrestrial_class' : 'TerrestrialEndpoint',
    'remote_class' : 'RemoteEndpoint',
    'terrestrial_module' : 'ion.services.sa.tcaa.terrestrial_endpoint',
    'remote_module' : 'ion.services.sa.tcaa.remote_endpoint',
    'logfile_dir' : '/tmp/'    
}

commander = None
platform_sub = None
result_sub = None
timer = None
go_time = None
logfile = None

status = TelemetryStatusType.UNAVAILABLE
queue_size = 0
requests_sent = {}
results_recv = {}
results_pending = {}
results_confirmed = {}
results_error = {}

def reset():
    """
    """
    stop_commander()
    stop_subscribers()
    stop_timer()

    global queue_size
    global status
    global requests_sent
    global results_recv
    global results_pending
    global results_confirmed
    global results_error
    
    queue_size = 0
    status = TelemetryStatusType.UNAVAILABLE
    requests_sent = {}
    results_recv = {}
    results_pending = {}
    results_confirmed = {}
    results_error = {}
    
def get_args():
    """
    """
    return copy.deepcopy(tcaa_args)

def set_args(**kwargs):
    """
    """
    global tcaa_args
    for (key, val) in kwargs.iteritems():
        if tcaa_args.has_key(key):
            tcaa_args[key] = val
            print 'updated %s=%s' % (key, str(val))

def launch_terrestrial():
    """
    """        
    listen_name = 'terrestrial_endpoint' + tcaa_args['xs_name']
    
    config = {
        'other_host' : tcaa_args['remote_host'],
        'other_port' : tcaa_args['remote_port'],
        'this_port' : tcaa_args['terrestrial_port'],
        'platform_resource_id' : tcaa_args['terrestrial_platform_id'],
        'xs_name' : tcaa_args['xs_name'],
        'process' : {
            'listen_name' : listen_name
        }
    }
    
    spargs = {
        'name' : listen_name,
        'module' : tcaa_args['terrestrial_module'],
        'cls' : tcaa_args['terrestrial_class'],
        'config' : config
    }
    
    pid = cc.spawn_process(**spargs)
    print 'terrestrial pid = %s' % pid
    
    tc = TerrestrialEndpointClient(process=FakeProcess(), to_name=listen_name)

    return tc

def launch_remote():
    """
    """
    listen_name = 'remote_endpoint' + tcaa_args['xs_name']
    
    config = {
        'other_host' : tcaa_args['terrestrial_host'],
        'other_port' : tcaa_args['terrestrial_port'],
        'this_port' : tcaa_args['remote_port'],
        'platform_resource_id' : tcaa_args['remote_platform_id'],
        'xs_name' : tcaa_args['xs_name'],
        'process' : {
            'listen_name' : listen_name
        }
    }
    
    spargs = {
        'name' : listen_name,
        'module' : tcaa_args['remote_module'],
        'cls' : tcaa_args['remote_class'],
        'config' : config
    }

    pid = cc.spawn_process(**spargs)
    print 'remote pid = %s' % pid
    
    rc = RemoteEndpointClient(process=FakeProcess(), to_name=listen_name)

    return rc

def start_subscribers():
    """
    """
    global platform_sub
    global result_sub    
    global go_time
    global logfile 
    go_time = time.time()
    loc_time = time.localtime(go_time)
    fname = '2caa_log_%d_%d_%d.txt' % (loc_time[3],loc_time[4],loc_time[5])
    fname = tcaa_args['logfile_dir'] + fname
    logfile = open(fname, 'w')
    logfile.write('%15.6f %40s %6d %6d %6d %6d %6d %6d %6d\n' %
        (0.0, 'Start', status, queue_size, len(requests_sent),
         len(results_recv), len(results_pending), len(results_confirmed),
         len(results_error)))
    
    platform_sub = EventSubscriber(
        event_type='PlatformEvent',
        callback=consume_event,
        origin=tcaa_args['xs_name']
    )
    platform_sub.start()
    
    result_sub = EventSubscriber(
        event_type='RemoteCommandResult',
        callback=consume_event,
        origin='fake_id'
    )
    result_sub.start()

def stop_subscribers():
    """
    """
    global platform_sub
    global result_sub
    global logfile
    global go_time
    
    if platform_sub:
        platform_sub.stop()
        platform_sub = None
    if result_sub:
        result_sub.stop()
        result_sub = None

    if logfile:
        logfile.write('%15.6f %40s %6d %6d %6d %6d %6d %6d %6d\n' %
            (time.time()-go_time, 'Stop', status, queue_size, len(requests_sent),
             len(results_recv), len(results_pending), len(results_confirmed),
             len(results_error)))        
        logfile.close()
        logfile = None

    go_time = None

def consume_event(evt, msg_headers):
    """
    """
    global go_time
    global queue_size
    global status
    global results_recv
    global results_pending
    global results_confirmed
    global results_error
    global logfile
    
    print 'GOT EVENT: %s' % str(evt)
    print '################################'
    if evt.type_ == 'RemoteQueueModifiedEvent':
        print 'endpoint %s queue modified, queue length= %d' % \
            (evt.origin, evt.queue_size)
        queue_size = evt.queue_size
            
    elif evt.type_ == 'RemoteCommandTransmittedEvent':
        print 'endpoint %s queue transmitted, queue length= %d' % \
            (evt.origin, evt.queue_size)
        queue_size = evt.queue_size
        
    elif evt.type_ == 'PublicPlatformTelemetryEvent':
        print 'endpoint %s link status %d' % (evt.origin, evt.status)
        status = evt.status
        
    elif evt.type_ == 'RemoteCommandResult':
        print 'received command result for %s' % evt.origin
        cmd = evt.command
        print 'command: %s' % cmd.command
        print 'args: %s' % cmd.args
        print 'kwargs: %s' % cmd.kwargs
        print 'command id: %s' % cmd.command_id
        print 'time queued: %f' % cmd.time_queued
        print 'time completed: %f' % cmd.time_completed
        print 'command result: %s' % cmd.result
        results_recv[cmd.command_id] = cmd
        try:
            sent_cmd = results_pending.pop(cmd.command_id)
            if sent_cmd.kwargs['payload'] == cmd.result:
                results_confirmed[cmd.command_id] = cmd
            else:
                results_error[cmd.command_id] = cmd
        except KeyError:
            print '## Warning: got an unpending result.'
    print '################################'
        
    if logfile:        
        logfile.write('%15.6f %40s %6d %6d %6d %6d %6d %6d %6d\n' %
            (time.time()-go_time, evt.type_, status, queue_size, len(requests_sent),
             len(results_recv), len(results_pending), len(results_confirmed),
             len(results_error)))
        
        
def publish_link_event(up_down, terrestrial_remote=2):
    """
    """
    status = TelemetryStatusType.AVAILABLE if up_down \
        else TelemetryStatusType.UNAVAILABLE
    platform_id = tcaa_args['terrestrial_platform_id'] if terrestrial_remote \
        else tcaa_args['remote_platform_id']
    
    pub = EventPublisher()
    if terrestrial_remote == 0:
        pub.publish_event(
            event_type='PlatformTelemetryEvent',
            origin=tcaa_args['terrestrial_platform_id'],
            status = status)
        
    elif terrestrial_remote == 1:
        pub.publish_event(
            event_type='PlatformTelemetryEvent',
            origin=tcaa_args['remote_platform_id'],
            status = status)
        
    elif terrestrial_remote == 2:
        pub.publish_event(
            event_type='PlatformTelemetryEvent',
            origin=tcaa_args['terrestrial_platform_id'],
            status = status)
        pub.publish_event(
            event_type='PlatformTelemetryEvent',
            origin=tcaa_args['remote_platform_id'],
            status = status)
        
    else:
        raise ValueError('terrestrial_remote must be in range [0,2].')

def start_timer(period, up_time, terrestrial_remote=2):
    """
    """
    global timer

    def _loop():
        """
        """
        start_time = None
        next_time = None
        
        while True:
            cur_time = time.gmtime(time.time())
            (h, m, s) = (cur_time[3], cur_time[4], cur_time[5])
            if not next_time:
                next_time = calc_next_time(h, m, s, period)
            if not start_time:
                if h == next_time[0] and m == next_time[1] and s >= next_time[2]:
                    next_time = calc_next_time(h, m, s, period)
                    start_time = time.time()
                    publish_link_event(True, terrestrial_remote)
                    
            elif time.time() - start_time >= up_time:
                start_time = None
                publish_link_event(False, terrestrial_remote)
            
            gevent.sleep(1)

    timer = gevent.spawn(_loop)

def stop_timer():
    """
    """
    global timer
    
    if timer:
        timer.kill()
        timer.join()
        timer = None

def calc_next_time(h, m, s, p):
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

def start_commander(delay, size):
    """
    """
    global commander
    global requests_sent
    global results_pending

    listen_name = 'terrestrial_endpoint' + tcaa_args['xs_name']

    tc = TerrestrialEndpointClient(process=FakeProcess(), to_name=listen_name)

    def _loop():
        """
        """
        while True:
            gevent.sleep(delay)
            cmd = make_fake_command(size)
            requests_sent[cmd.command_id] = cmd
            results_pending[cmd.command_id] = cmd
            tc.enqueue_command(cmd)
            #print '## sending command %s' % str(cmd)

    commander = gevent.spawn(_loop)

def stop_commander():
    """
    """
    global commander
    if commander:
        commander.kill()
        commander.join()
        commander = None

def make_fake_command(size):
    """
    Build a fake command for use in tests.
    """
    random_str = ''.join([random.choice(string.ascii_lowercase) for x in xrange(size)])
    cmdstr = 'fake_cmd'
    cmd = IonObject('RemoteCommand',
                         resource_id='fake_id',
                         command='fake_cmd',
                         command_id=str(uuid.uuid4()),
                         args=[],
                         kwargs={'payload':random_str})
    return cmd
    
class FakeProcess(LocalContextMixin):
    """
    A fake process used because the test case is not an ion process.
    """
    name = ''
    id=''
    process_type = ''

    
    