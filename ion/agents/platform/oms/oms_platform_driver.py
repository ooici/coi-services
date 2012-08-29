#!/usr/bin/env python

"""
@package ion.agents.platform.oms.oms_platform_driver
@file    ion/agents/platform/oms/oms_platform_driver.py
@author  Carlos Rueda
@brief   Base class for OMS platform drivers.
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'


from pyon.public import log

from ion.agents.platform.platform_driver import PlatformDriver
from ion.agents.platform.exceptions import PlatformException
from ion.agents.platform.exceptions import PlatformDriverException
from ion.agents.platform.exceptions import PlatformConnectionException
from ion.agents.platform.oms.oms_resource_monitor import OmsResourceMonitor
from ion.agents.platform.oms.oms_client_factory import OmsClientFactory


class OmsPlatformDriver(PlatformDriver):
    """
    Base class for OMS platform drivers.
    """

    def __init__(self, platform_id, driver_config):
        """
        Creates an OmsPlatformDriver instance.

        @param platform_id Corresponding platform ID
        @param driver_config with required 'oms_uri' entry.
        """
        PlatformDriver.__init__(self, platform_id)

        if not 'oms_uri' in driver_config:
            raise PlatformDriverException(msg="driver_config does not indicate 'oms_uri'")

        oms_uri = driver_config['oms_uri']
        log.info("creating OmsClient instance with oms_uri=%r" % oms_uri)
        self._oms = OmsClientFactory.create_instance(oms_uri)
        log.info("OmsClient instance created: %s" % self._oms)

        # TODO set-up configuration for notification of events associated
        # with values retrieved during platform resource monitoring

        # _monitors: dict { attr_id: OmsResourceMonitor }
        self._monitors = {}

#    def set_oms_client(self, oms):
#        assert oms is not None, "set_oms_client must be called with a non-None value"
#        self._oms = oms

    def go_active(self):

        log.info("go_active: pinging...")
        try:
            retval = self._oms.hello.ping()
        except Exception, e:
            raise PlatformConnectionException("Cannot ping %s" % str(e))

        if retval is None or retval.lower() != "pong":
            raise PlatformConnectionException(
                "Unexpected ping response: %r" % retval)

        log.info("go_active completed ok. ping response: %r" % retval)

    def start_resource_monitoring(self):
        """
        Starts greenlets to periodically retrieve values of the attributes
        associated with my platform, and do corresponding event notifications.
        """
        # TODO preliminary implementation. General idea:
        # - get attributes for the platform
        # - aggregate groups of attributes according to rate of monitoring
        # - start a greenlet for each attr grouping

#        assert self._oms is not None, "set_oms_client must have been called"

        log.info("self._platform_id = %r" % self._platform_id)

        # get names of attributes associated with my platform
        attr_names = self._oms.getPlatformAttributeNames(self._platform_id)

        # get info associated with these attributes
        platAttrMap = {self._platform_id: attr_names}
        retval = self._oms.getPlatformAttributeInfo(platAttrMap)

        log.info("getPlatformAttributeInfo for %r = %s" % (self._platform_id, retval))

        if not self._platform_id in retval:
            raise PlatformException("Unexpected: response does not include "
                                    "requested platform '%s'" % self._platform_id)

        #
        # TODO attribute grouping so one single greenlet is launched for a
        # group of attributes having same or similar monitoring rate. For
        # simplicity at the moment, start a greenlet per attribute.
        #

        attr_info = retval[self._platform_id]
        for attr_name, attr_defn in attr_info.iteritems():
            if 'monitorCycleSeconds' in attr_defn:
                self._start_monitor_greenlet(attr_defn)
            else:
                log.warn("Unexpected: attribute info does not contain '%s' "
                         "for attribute '%s' in platform '%s. "
                         "attribute info = %s" % (
                         'monitorCycleSeconds', attr_name, self._platform_id, attr_defn))

    def _start_monitor_greenlet(self, attr_defn):
        assert 'attr_id' in attr_defn
        assert 'monitorCycleSeconds' in attr_defn
        resmon = OmsResourceMonitor(self._oms, self._platform_id, attr_defn,
                                    self._notify_driver_event)
        self._monitors[attr_defn['attr_id']] = resmon
        resmon.start()

    def stop_resource_monitoring(self):
        """
        Stops all the monitoring greenlets.
        """
#        assert self._oms is not None, "set_oms_client must have been called"

        log.info("self._platform_id = %r" % self._platform_id)
        for resmon in self._monitors.itervalues():
            resmon.stop()
        self._monitors.clear()

    def destroy(self):
        """
        Stops all activity done by the driver.
        """
        self.stop_resource_monitoring()
