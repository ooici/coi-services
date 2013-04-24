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
    proxy = xmlrpclib.ServerProxy(uri)
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

        If handler_name is given, and the method on the corresponding
        handler fails, then the method is tried in the proxy directly.
        NOTE: This strategy is temporary, just while RSN indicates
        the specific set of handlers they will be using.

        @param handler_name  Name of the handler; can be None to indicate get
                             method directly from proxy.
        @param method_name   Method's name
        @param args          to display method to be called.
        """
        global tried

        tried[method_name] = ""
        print("\n-- %s --" % method_name)

        # get the method
        method = get_method(handler_name, method_name)
        if method is None:
            tried[method_name] = "could not get handler or method"
            return

        sargs = ", ".join(["%r" % a for a in args])

        if handler_name:
            sys.stdout.write("%s.%s(%s) -> " % (handler_name, method_name, sargs))
        else:
            sys.stdout.write("%s(%s) -> " % (method_name, sargs))
        sys.stdout.flush()

        # run method
        try:
            retval = method(*args)
            print "%r" % retval
            tried[method_name] = "OK"
            return retval
        except Exception as e:
            print "Exception: %s: %s" % (type(e), str(e))
            tried[method_name] = str(e)

            if handler_name:
                # try without handler:
                sys.stdout.write("\t without handler: ")
                sys.stdout.flush()
                method = get_method(None, method_name)
                if method is None:
                    return

                sys.stdout.write("%s(%s) -> " % (method_name, sargs))
                sys.stdout.flush()
                try:
                    retval = method(*args)
                    print "%r" % retval
                    tried[method_name] = "OK"
                    return retval
                except Exception as e:
                    print "Exception: %s: %s" % (type(e), str(e))

    print "\nBasic verification of the operations:"

    run("hello", "ping")
    run("config", 'get_platform_types')

    plat_map = run("config", 'get_platform_map')
    platform_id = "dummy_platform_id"
    if plat_map:
        platform_id = plat_map[0][0]

    run("config", 'get_platform_metadata', platform_id)
    run("config", 'get_platform_attributes', platform_id)
    run("config", 'get_platform_attribute_values', platform_id, {})
    run("config", 'set_platform_attribute_values', platform_id, {})
    ports = run("config", 'get_platform_ports', platform_id)

    port_id = "dummy_port_id"
    instrument_id = "dummy_instrument_id"
    run("config", 'connect_instrument', platform_id, port_id, instrument_id, {})
    run("config", 'disconnect_instrument', platform_id, port_id, instrument_id)
    run("config", 'get_connected_instruments', platform_id, port_id)
    run("config", 'turn_on_platform_port', platform_id, port_id)
    run("config", 'turn_off_platform_port', platform_id, port_id)

    url = "dummy_url_listener"
    run("config", 'register_event_listener', url, [])
    run("config", 'unregister_event_listener', url, [])
    run("config", 'get_registered_event_listeners')

    run("config", 'get_checksum', platform_id)

    print("\nSummary of basic verification:")
    for method_name, result in sorted(tried.iteritems()):
        print("%20s %-40s: %s" % ("", method_name, result))

    print("\nNote: Just couple of handlers tried while RSN indicates "
          "the specific set of handlers they will be using.\n")

"""
$ date && bin/python ion/agents/platform/rsn/test/oms_simple.py
Thu Apr 18 16:13:18 PDT 2013

connecting to 'http://alice:1234@10.180.80.10:9021/' ...
connection established.

Basic verification of the operations:

-- ping --
hello.ping() -> 'pong'

-- get_platform_types --
config.get_platform_types() -> Exception: <class 'xmlrpclib.Fault'>: <Fault 8001: 'function get_platform_types not found'>
	 without handler: get_platform_types() -> Exception: <class 'xmlrpclib.Fault'>: <Fault 8001: 'function get_platform_types not found'>

-- get_platform_map --
config.get_platform_map() -> [['Node1A', 'ShoreStation'], ['Node1B', 'ShoreStation'], ['ShoreStation', '']]

-- get_platform_metadata --
config.get_platform_metadata('Node1A') -> ''

-- get_platform_attributes --
config.get_platform_attributes('Node1A') -> Exception: <class 'xmlrpclib.Fault'>: <Fault 8001: 'function get_platform_attributes not found'>
	 without handler: get_platform_attributes('Node1A') -> Exception: <class 'xmlrpclib.Fault'>: <Fault 8001: 'function get_platform_attributes not found'>

-- get_platform_attribute_values --
config.get_platform_attribute_values('Node1A', {}) -> Exception: <class 'xmlrpclib.Fault'>: <Fault 8001: 'function get_platform_attribute_values not found'>
	 without handler: get_platform_attribute_values('Node1A', {}) -> Exception: <class 'xmlrpclib.Fault'>: <Fault 8001: 'function get_platform_attribute_values not found'>

-- set_platform_attribute_values --
config.set_platform_attribute_values('Node1A', {}) -> Exception: <class 'xmlrpclib.Fault'>: <Fault 8001: 'function set_platform_attribute_values not found'>
	 without handler: set_platform_attribute_values('Node1A', {}) -> Exception: <class 'xmlrpclib.Fault'>: <Fault 8001: 'function set_platform_attribute_values not found'>

-- get_platform_ports --
config.get_platform_ports('Node1A') -> Exception: <class 'xmlrpclib.Fault'>: <Fault 8001: 'function get_platform_ports not found'>
	 without handler: get_platform_ports('Node1A') -> Exception: <class 'xmlrpclib.Fault'>: <Fault 8001: 'function get_platform_ports not found'>

-- connect_instrument --
config.connect_instrument('Node1A', 'dummy_port_id', 'dummy_instrument_id', {}) -> Exception: <class 'xmlrpclib.Fault'>: <Fault 8001: 'function connect_instrument not found'>
	 without handler: connect_instrument('Node1A', 'dummy_port_id', 'dummy_instrument_id', {}) -> Exception: <class 'xmlrpclib.Fault'>: <Fault 8001: 'function connect_instrument not found'>

-- disconnect_instrument --
config.disconnect_instrument('Node1A', 'dummy_port_id', 'dummy_instrument_id') -> Exception: <class 'xmlrpclib.Fault'>: <Fault 8001: 'function disconnect_instrument not found'>
	 without handler: disconnect_instrument('Node1A', 'dummy_port_id', 'dummy_instrument_id') -> Exception: <class 'xmlrpclib.Fault'>: <Fault 8001: 'function disconnect_instrument not found'>

-- get_connected_instruments --
config.get_connected_instruments('Node1A', 'dummy_port_id') -> Exception: <class 'xmlrpclib.Fault'>: <Fault 8001: 'function get_connected_instruments not found'>
	 without handler: get_connected_instruments('Node1A', 'dummy_port_id') -> Exception: <class 'xmlrpclib.Fault'>: <Fault 8001: 'function get_connected_instruments not found'>

-- turn_on_platform_port --
config.turn_on_platform_port('Node1A', 'dummy_port_id') -> Exception: <class 'xmlrpclib.Fault'>: <Fault 8001: 'function turn_on_platform_port not found'>
	 without handler: turn_on_platform_port('Node1A', 'dummy_port_id') -> Exception: <class 'xmlrpclib.Fault'>: <Fault 8001: 'function turn_on_platform_port not found'>

-- turn_off_platform_port --
config.turn_off_platform_port('Node1A', 'dummy_port_id') -> Exception: <class 'xmlrpclib.Fault'>: <Fault 8001: 'function turn_off_platform_port not found'>
	 without handler: turn_off_platform_port('Node1A', 'dummy_port_id') -> Exception: <class 'xmlrpclib.Fault'>: <Fault 8001: 'function turn_off_platform_port not found'>

-- register_event_listener --
config.register_event_listener('dummy_url_listener', []) -> Exception: <class 'xmlrpclib.Fault'>: <Fault 8001: 'function register_event_listener not found'>
	 without handler: register_event_listener('dummy_url_listener', []) -> Exception: <class 'xmlrpclib.Fault'>: <Fault 8001: 'function register_event_listener not found'>

-- unregister_event_listener --
config.unregister_event_listener('dummy_url_listener', []) -> Exception: <class 'xmlrpclib.Fault'>: <Fault 8001: 'function unregister_event_listener not found'>
	 without handler: unregister_event_listener('dummy_url_listener', []) -> Exception: <class 'xmlrpclib.Fault'>: <Fault 8001: 'function unregister_event_listener not found'>

-- get_registered_event_listeners --
config.get_registered_event_listeners() -> Exception: <class 'xmlrpclib.Fault'>: <Fault 8001: 'function get_registered_event_listeners not found'>
	 without handler: get_registered_event_listeners() -> Exception: <class 'xmlrpclib.Fault'>: <Fault 8001: 'function get_registered_event_listeners not found'>

-- get_checksum --
config.get_checksum('Node1A') -> Exception: <class 'xmlrpclib.Fault'>: <Fault 8001: 'function get_checksum not found'>
	 without handler: get_checksum('Node1A') -> Exception: <class 'xmlrpclib.Fault'>: <Fault 8001: 'function get_checksum not found'>

Summary of basic verification:
                     connect_instrument                      : <Fault 8001: 'function connect_instrument not found'>
                     disconnect_instrument                   : <Fault 8001: 'function disconnect_instrument not found'>
                     get_checksum                            : <Fault 8001: 'function get_checksum not found'>
                     get_connected_instruments               : <Fault 8001: 'function get_connected_instruments not found'>
                     get_platform_attribute_values           : <Fault 8001: 'function get_platform_attribute_values not found'>
                     get_platform_attributes                 : <Fault 8001: 'function get_platform_attributes not found'>
                     get_platform_map                        : OK
                     get_platform_metadata                   : OK
                     get_platform_ports                      : <Fault 8001: 'function get_platform_ports not found'>
                     get_platform_types                      : <Fault 8001: 'function get_platform_types not found'>
                     get_registered_event_listeners          : <Fault 8001: 'function get_registered_event_listeners not found'>
                     ping                                    : OK
                     register_event_listener                 : <Fault 8001: 'function register_event_listener not found'>
                     set_platform_attribute_values           : <Fault 8001: 'function set_platform_attribute_values not found'>
                     turn_off_platform_port                  : <Fault 8001: 'function turn_off_platform_port not found'>
                     turn_on_platform_port                   : <Fault 8001: 'function turn_on_platform_port not found'>
                     unregister_event_listener               : <Fault 8001: 'function unregister_event_listener not found'>

Note: Just couple of handlers tried while RSN indicates the specific set of handlers they will be using.

"""