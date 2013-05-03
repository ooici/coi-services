#!/usr/bin/env python

"""
@package ion.agents.platform.rsn.test.oms_simple
@file    ion/agents/platform/rsn/test/oms_simple.py
@author  Carlos Rueda
@brief   Program that connects to the real RSN OMS enpoint to do basic
         verification of the operations. Note that VPN is required.

         USAGE: bin/python ion/agents/platform/rsn/test/oms_simple.py [uri]
         default uri: 'http://alice:1234@10.180.80.10:9021/'

         See bottom of this file for a complete execution.

@see     https://confluence.oceanobservatories.org/display/syseng/CIAD+MI+SV+CI-OMS+interface
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'


import xmlrpclib
import sys

# Main program
if __name__ == "__main__":  # pragma: no cover

    uri = "http://alice:1234@10.180.80.10:9021/"
    if len(sys.argv) == 2:
        uri = sys.argv[1]
    elif len(sys.argv) > 2:
        print "USAGE: %s [uri]" % sys.argv[0]
        print "default uri: %r" % uri
        exit()

    print '\nconnecting to %r ...' % uri
    proxy = xmlrpclib.ServerProxy(uri, allow_none=True)
    print 'connection established.'

    tried = {}

    def get_method(handler_name, method_name):
        """
        Gets the method from the proxy.
        @param handler_name  Name of the handler; can be None to indicate get
                             method directly from proxy.
        @param method_name   Method's name

        @return              callable; None if any error getting the method
        """

        # get method:
        if handler_name:
            # get handler:
            try:
                handler = getattr(proxy, handler_name)
            except Exception as e:
                print "error getting handler %s: %s: %s" % (handler_name, type(e), str(e))
                return None
            try:
                method = getattr(handler, method_name)
                return method
            except Exception as e:
                print "error method %s.%s: %s: %s" % (handler_name, method_name, type(e), str(e))
                return None
        else:
            try:
                method = getattr(proxy, method_name)
                return method
            except Exception as e:
                print "error getting proxy's method %s: %s: %s" % (method_name, type(e), str(e))
                return None

    def run(handler_name, method_name, *args):
        """
        Runs a method against the proxy.

        @param handler_name  Name of the handler
        @param method_name   Method's name
        @param args          to display method to be called.
        """
        global tried

        full_method_name = "%s.%s" % (handler_name, method_name)

        tried[full_method_name] = ""

        # get the method
        method = get_method(handler_name, method_name)
        if method is None:
            tried[full_method_name] = "could not get handler or method"
            return

        sargs = ", ".join(["%r" % a for a in args])

        sys.stdout.write("%s(%s) -> " % (full_method_name, sargs))
        sys.stdout.flush()

        # run method
        try:
            retval = method(*args)
            tried[full_method_name] = "OK"
            print "%r" % retval
        except xmlrpclib.Fault as e:
            if e.faultCode == 8001:
                retval = "Not found"
            else:
                retval = "Fault %d: %s" % (e.faultCode, e.faultString)
                # raise
                # print "Exception: %s: %s" % (type(e), str(e))
                # tried[full_method_name] = str(e)

            tried[full_method_name] = retval
            print "%s" % retval

        return retval

    print "Basic verification of the operations:\n"

    run("hello", "ping")
    run("config", 'get_platform_types')

    plat_map = run("config", 'get_platform_map')
    platform_id = "dummy_platform_id"
    if plat_map:
        platform_id = plat_map[0][0]

    run("config", 'get_platform_metadata', platform_id)
    run("attr", 'get_platform_attributes', platform_id)
    run("attr", 'get_platform_attribute_values', platform_id, {})
    run("attr", 'set_platform_attribute_values', platform_id, {})
    ports = run("config", 'get_platform_ports', platform_id)

    port_id = "dummy_port_id"
    instrument_id = "dummy_instrument_id"
    run("config", 'connect_instrument', platform_id, port_id, instrument_id, {})
    run("config", 'disconnect_instrument', platform_id, port_id, instrument_id)
    run("config", 'get_connected_instruments', platform_id, port_id)
    run("port", 'turn_on_platform_port', platform_id, port_id)
    run("port", 'turn_off_platform_port', platform_id, port_id)

    url = "dummy_url_listener"
    run("event", 'register_event_listener', url)
    run("event", 'unregister_event_listener', url)
    run("event", 'get_registered_event_listeners')

    run("config", 'get_checksum', platform_id)

    run("event", 'generate_test_event', 'ref_id', 'simulated event', platform_id, 'power')

    print("\nSummary of basic verification:")
    for full_method_name, result in sorted(tried.iteritems()):
        print("%20s %-40s: %s" % ("", full_method_name, result))

"""
$ date && bin/python ion/agents/platform/rsn/test/oms_simple.py
Wed May  1 12:20:59 PDT 2013

connecting to 'http://alice:1234@10.180.80.10:9021/' ...
connection established.
Basic verification of the operations:

hello.ping() -> 'pong'
config.get_platform_types() -> Not found
config.get_platform_map() -> [['Node1A', 'ShoreStation'], ['Node1B', 'ShoreStation'], ['ShoreStation', '']]
config.get_platform_metadata('Node1A') -> ''
attr.get_platform_attributes('Node1A') -> Fault 8002: error
attr.get_platform_attribute_values('Node1A', {}) -> Not found
attr.set_platform_attribute_values('Node1A', {}) -> Not found
config.get_platform_ports('Node1A') -> Not found
config.connect_instrument('Node1A', 'dummy_port_id', 'dummy_instrument_id', {}) -> Not found
config.disconnect_instrument('Node1A', 'dummy_port_id', 'dummy_instrument_id') -> Not found
config.get_connected_instruments('Node1A', 'dummy_port_id') -> Not found
port.turn_on_platform_port('Node1A', 'dummy_port_id') -> Fault 8002: error
port.turn_off_platform_port('Node1A', 'dummy_port_id') -> Fault 8002: error
event.register_event_listener('dummy_url_listener') -> {'dummy_url_listener': 3576425022.469944}
event.unregister_event_listener('dummy_url_listener') -> {'dummy_url_listener': 3576425022.767337}
event.get_registered_event_listeners() -> {'23422.jwl': 3576419989.9077272}
config.get_checksum('Node1A') -> Not found
event.generate_test_event('ref_id', 'simulated event', 'Node1A', 'power') -> Not found

Summary of basic verification:
                     attr.get_platform_attribute_values      : Not found
                     attr.get_platform_attributes            : Fault 8002: error
                     attr.set_platform_attribute_values      : Not found
                     config.connect_instrument               : Not found
                     config.disconnect_instrument            : Not found
                     config.get_checksum                     : Not found
                     config.get_connected_instruments        : Not found
                     config.get_platform_map                 : OK
                     config.get_platform_metadata            : OK
                     config.get_platform_ports               : Not found
                     config.get_platform_types               : Not found
                     event.generate_test_event               : Not found
                     event.get_registered_event_listeners    : OK
                     event.register_event_listener           : OK
                     event.unregister_event_listener         : OK
                     hello.ping                              : OK
                     port.turn_off_platform_port             : Fault 8002: error
                     port.turn_on_platform_port              : Fault 8002: error
"""