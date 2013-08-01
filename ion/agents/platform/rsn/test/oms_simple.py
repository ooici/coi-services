#!/usr/bin/env python

"""
@package ion.agents.platform.rsn.test.oms_simple
@file    ion/agents/platform/rsn/test/oms_simple.py
@author  Carlos Rueda
@brief   Program that connects to the real RSN OMS enpoint to do basic
         verification of the operations. Note that VPN is required.

         USAGE: python ion/agents/platform/rsn/test/oms_simple.py [uri]
         default uri: 'http://alice:1234@10.180.80.10:9021/'

@see     https://confluence.oceanobservatories.org/display/CIDev/RSN+OMS+endpoint+implementation+verification
@see     https://confluence.oceanobservatories.org/display/syseng/CIAD+MI+SV+CI-OMS+interface
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'


import xmlrpclib
import sys
import pprint

# Not importing ion.agents.platform.responses.InvalidResponse.PLATFORM_ID
# to keep this program standalone, so just copy definition:
INVALID_PLATFORM_ID = 'INVALID_PLATFORM_ID'


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

    pp = pprint.PrettyPrinter()

    def format_val(value):
        prefix = "\t\t"
        print "\n%s%s" % (prefix, pp.pformat(value).replace("\n", "\n" + prefix))

    def format_err(msg):
        prefix = "\t\t"
        print "\n%s%s" % (prefix, msg.replace("\n", "\n" + prefix))

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

    def run(full_method_name, *args):
        """
        Runs a method against the proxy.

        @param full_method_name
        @param args
        """
        global tried

        tried[full_method_name] = ""

        handler_name, method_name = full_method_name.split(".")

        # get the method
        method = get_method(handler_name, method_name)
        if method is None:
            tried[full_method_name] = "could not get handler or method"
            return

        sargs = ", ".join(["%r" % a for a in args])

        sys.stdout.write("\n%s(%s) -> " % (full_method_name, sargs))
        sys.stdout.flush()

        # run method
        retval, reterr = None, None
        try:
            retval = method(*args)
            tried[full_method_name] = "OK"
            # print "%r" % retval
            format_val(retval)
        except xmlrpclib.Fault as e:
            if e.faultCode == 8001:
                reterr = "-- NOT FOUND (fault %s)" % e.faultCode
            else:
                reterr = "-- Fault %d: %s" % (e.faultCode, e.faultString)
                # raise
                # print "Exception: %s: %s" % (type(e), str(e))
                # tried[full_method_name] = str(e)

            tried[full_method_name] = reterr
            format_err(reterr)

        return retval, reterr

    def verify_entry_in_dict(retval, reterr, entry):
        if reterr is not None:
            return retval, reterr

        if not isinstance(retval, dict):
            reterr = "-- expecting a dict with entry %r" % entry
        elif entry not in retval:
            reterr = "-- expecting a dict with entry %r" % entry
        else:
            retval = retval[entry]

        if reterr:
            tried[full_method_name] = reterr
            format_err(reterr)

        return retval, reterr

    print "Basic verification of the operations:\n"

    #----------------------------------------------------------------------
    full_method_name = "hello.ping"
    retval, reterr = run(full_method_name)
    if retval and retval.lower() != "pong":
        error = "expecting 'pong'"
        tried[full_method_name] = error
        format_err(error)

    #----------------------------------------------------------------------
    full_method_name = "config.get_platform_types"
    retval, reterr = run(full_method_name)
    if retval and not isinstance(retval, dict):
        error = "expecting a dict"
        tried[full_method_name] = error
        format_err(error)

    platform_id = "dummy_platform_id"

    #----------------------------------------------------------------------
    full_method_name = "config.get_platform_map"
    retval, reterr = run(full_method_name)
    if retval is not None:
        if isinstance(retval, list):
            if len(retval):
                if isinstance(retval[0], (tuple, list)):
                    platform_id = retval[0][0]
                else:
                    reterr = "expecting a list of tuples or lists"
            else:
                reterr = "expecting a non-empty list"
        else:
            reterr = "expecting a list"
        if reterr:
            tried[full_method_name] = reterr
            format_err(reterr)

    #----------------------------------------------------------------------
    full_method_name = "config.get_platform_metadata"
    retval, reterr = run(full_method_name, platform_id)
    retval, reterr = verify_entry_in_dict(retval, reterr, platform_id)

    #----------------------------------------------------------------------
    full_method_name = "attr.get_platform_attributes"
    retval, reterr = run(full_method_name, platform_id)
    retval, reterr = verify_entry_in_dict(retval, reterr, platform_id)

    #----------------------------------------------------------------------
    full_method_name = "attr.get_platform_attribute_values"
    retval, reterr = run(full_method_name, platform_id, [])
    retval, reterr = verify_entry_in_dict(retval, reterr, platform_id)

    #----------------------------------------------------------------------
    full_method_name = "attr.set_platform_attribute_values"
    retval, reterr = run(full_method_name, platform_id, {})
    retval, reterr = verify_entry_in_dict(retval, reterr, platform_id)

    port_id = "dummy_port_id"

    #----------------------------------------------------------------------
    full_method_name = "port.get_platform_ports"
    retval, reterr = run(full_method_name, platform_id)
    retval, reterr = verify_entry_in_dict(retval, reterr, platform_id)
    if retval is not None:
        if isinstance(retval, dict):
            if len(retval):
                port_id = retval.keys()[0]
            else:
                reterr = "empty dict of ports for platform %r" % platform_id
        else:
            reterr = "expecting a dict {%r: ...}. got: %s" % (platform_id, type(retval))
        if reterr:
            tried[full_method_name] = reterr
            format_err(reterr)

    instrument_id = "dummy_instrument_id"

    if reterr is None:
        full_method_name = "port.get_platform_ports"
        retval, reterr = run(full_method_name, "dummy_platform_id")
        orig_retval = retval
        retval, reterr = verify_entry_in_dict(retval, reterr, "dummy_platform_id")
        if retval != INVALID_PLATFORM_ID:
            reterr = "expecting dict {%r: %r}. got: %r" % (
                "dummy_platform_id", INVALID_PLATFORM_ID, orig_retval)
            tried[full_method_name] = reterr
            format_err(reterr)

    instrument_id = "dummy_instrument_id"

    #----------------------------------------------------------------------
    full_method_name = "instr.connect_instrument"
    retval, reterr = run(full_method_name, platform_id, port_id, instrument_id, {})
    retval, reterr = verify_entry_in_dict(retval, reterr, platform_id)
    retval, reterr = verify_entry_in_dict(retval, reterr, port_id)
    retval, reterr = verify_entry_in_dict(retval, reterr, instrument_id)

    connect_instrument_error = reterr

    #----------------------------------------------------------------------
    full_method_name = "instr.get_connected_instruments"
    retval, reterr = run(full_method_name, platform_id, port_id)
    retval, reterr = verify_entry_in_dict(retval, reterr, platform_id)
    retval, reterr = verify_entry_in_dict(retval, reterr, port_id)
    # note, in case of error in instr.connect_instrument, don't expect the
    # instrument_id to be reported:
    if connect_instrument_error is None:
        retval, reterr = verify_entry_in_dict(retval, reterr, instrument_id)

    #----------------------------------------------------------------------
    full_method_name = "instr.disconnect_instrument"
    retval, reterr = run(full_method_name, platform_id, port_id, instrument_id)
    retval, reterr = verify_entry_in_dict(retval, reterr, platform_id)
    retval, reterr = verify_entry_in_dict(retval, reterr, port_id)
    retval, reterr = verify_entry_in_dict(retval, reterr, instrument_id)

    #----------------------------------------------------------------------
    full_method_name = "port.turn_on_platform_port"
    retval, reterr = run(full_method_name, platform_id, port_id)

    #----------------------------------------------------------------------
    full_method_name = "port.turn_off_platform_port"
    retval, reterr = run(full_method_name, platform_id, port_id)

    #----------------------------------------------------------------------
    import socket
    url = "http://%s:12345/fake.ci.oms.event.listener.net" % socket.getfqdn()

    #----------------------------------------------------------------------
    full_method_name = "event.register_event_listener"
    retval, reterr = run(full_method_name, url)
    retval, reterr = verify_entry_in_dict(retval, reterr, url)

    #----------------------------------------------------------------------
    full_method_name = "event.get_registered_event_listeners"
    retval, reterr = run(full_method_name)
    urls = retval
    retval, reterr = verify_entry_in_dict(retval, reterr, url)

    #----------------------------------------------------------------------
    full_method_name = "event.unregister_event_listener"
    if isinstance(urls, dict):
        # this part just as a convenience to unregister listeners that were
        # left registered by some error in a prior interaction.
        prefix = "http://127.0.0.1:"  # or some other needed prefix
        for url2 in urls:
            if url2.find(prefix) >= 0:
                retval, reterr = run(full_method_name, url2)
                retval, reterr = verify_entry_in_dict(retval, reterr, url2)
                if reterr is not None:
                    break
    if reterr is None:
        retval, reterr = run(full_method_name, url)
        retval, reterr = verify_entry_in_dict(retval, reterr, url)

    #----------------------------------------------------------------------
    full_method_name = "config.get_checksum"
    retval, reterr = run(full_method_name, platform_id)

    #----------------------------------------------------------------------
    full_method_name = "event.generate_test_event"
    event = {
        'message'      : "fake event triggered from CI using OMS' generate_test_event",
        'platform_id'  : "fake_platform_id",
        'severity'     : "3",
        'group '       : "power",
    }
    retval, reterr = run(full_method_name, event)

    #######################################################################
    print("\nSummary of basic verification:")
    okeys = 0
    for full_method_name, result in sorted(tried.iteritems()):
        print("%20s %-40s: %s" % ("", full_method_name, result))
        if result == "OK":
            okeys += 1
    print("OK methods %d out of %s" % (okeys, len(tried)))
