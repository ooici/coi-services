#!/usr/bin/env python

"""
@package ion.agents.platform.oms.oms_platform_agent
@file    ion/agents/platform/oms/oms_platform_agent.py
@author  Carlos Rueda
@brief   OMS platform agent class
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'


from pyon.public import log

from ion.agents.platform.platform_agent import PlatformAgent
from ion.agents.platform.exceptions import PlatformException
from ion.agents.platform.exceptions import PlatformConnectionException
from ion.agents.platform.oms.oms_client_factory import OmsClientFactory


def get_resource_id(platform_id):
    """
    Gets the resource ID corresponding to the given platform ID.
    """
    pa_resource_id = 'oms_platform_agent_%s' % platform_id
    return pa_resource_id


class OmsPlatformAgent(PlatformAgent):
    """
    OMS platform agent class
    """
    #
    # PRELIMINARY
    #

    def _reset(self):
        PlatformAgent._reset(self)

        # self._oms created in _go_active
        self._oms = None

    def _initialize(self):
        super(OmsPlatformAgent, self)._initialize()

        driver_config = self._plat_config['driver_config']
        if not 'oms_uri' in driver_config:
            raise PlatformException("'%s' key not given in driver_config=%s"
                                    % ('oms_uri', driver_config))

    def _go_active(self):
        """
        Establish communication with external platform network and put the
        agent in IDLE state. Configuration not necessarily verified/resynced
        in this operation.
        """

        assert self._plat_config is not None, "plat_config required"
        assert 'oms_uri' in self._plat_config

        oms_uri = self._plat_config['oms_uri']
        oms = OmsClientFactory.create_instance(oms_uri)

        log.info("_go_active: pinging...")
        try:
            retval = oms.hello.ping()
        except Exception, e:
            raise PlatformConnectionException("Cannot ping %s" % str(e))

        if retval is None or retval.lower() != "pong":
            raise PlatformConnectionException(
                "Unexpected ping response: '%s'" % str(retval))

        log.info("_go_active completed ok. oms=%s" % str(oms))

        self._oms = oms

# TODO remove (this was moved to parent class in a more generic way)
#    def _create_driver(self):
#        """
#        Creates the platform driver object for this platform agent.
#
#        NOTE: currently the driver object is created directly, that is,
#        not via a spawned process (as platform agents).
#        """
#        driver_config = self._plat_config['driver_config']
#        driver_module = driver_config['dvr_mod']
#        driver_class = driver_config['dvr_cls']
#
#        assert self._platform_id is not None, "must know platform_id to create driver"
#
#        log.info('Create driver: %s', driver_config)
#
#        try:
#            module = __import__(driver_module, fromlist=[driver_class])
#            classobj = getattr(module, driver_class)
#            driver = classobj(self._platform_id)
#
#        except Exception as e:
#            # TODO log-and-throw is in general considered an anti-pattern.
#            # But we log here to see the error message!
#            msg = 'Could not import/construct driver: module=%s, class=%s' % (
#                driver_module, driver_class)
#            log.error("%s; reason=%s" % (msg, str(e)))
#            raise CannotInstantiateDriverException(msg=msg, reason=e)
#
#        driver.set_oms_client(self._oms)
#        log.info("Driver created: %s" % str(driver))
#        self._plat_driver = driver

    def _get_resource_id(self, platform_id):
        return get_resource_id(platform_id)

    def _start_resource_monitoring(self):
        """
        Starts underlying mechanisms to monitor the resources associated with
        this platform.
        Delegates to the driver.
        """
        assert self._plat_driver is not None, "_create_driver must have been called"

        self._plat_driver.start_resource_monitoring()
