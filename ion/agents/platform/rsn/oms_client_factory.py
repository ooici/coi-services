#!/usr/bin/env python

"""
@package ion.agents.platform.rsn.oms_client_factory
@file    ion/agents/platform/rsn/oms_client_factory.py
@author  Carlos Rueda
@brief   CIOMS Client factory.
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'


from pyon.public import log

from ion.agents.platform.rsn.simulator.oms_simulator import CIOMSSimulator
import xmlrpclib
import os
from gevent import Greenlet, sleep


class CIOMSClientFactory(object):
    """
    Provides a CIOMSClient implementation.
    """

    # counter of created, not-yet-destroyed instances, for debugging purposes
    _inst_count = 0

    # _sim_process, _rsn_oms: see launch_simulator and related methods
    _sim_process = None
    _rsn_oms = None

    @classmethod
    def create_instance(cls, uri=None):
        """
        Creates an CIOMSClient instance.
        Do not forget to call destroy_instance with the returned object when
        you are done with the instance.

        @param uri URI to connect to the RSN OMS server or simulator.
        If None (the default) the value of the OMS environment variable is used
        as argument. If not defined or if the resulting argument is "embsimulator"
        then an CIOMSSimulator instance is directly created and returned.
        Otherwise, the given argument (or value of the OMS environment variable)
        is used as given to try the connection with the corresponding XML/RPC
        server resolvable by that URI.
        """

        if uri is None:
            uri = os.getenv('OMS', 'embsimulator')

        if "embsimulator" == uri:
            # "embedded" simulator, so instantiate CIOMSSimulator here:
            log.debug("Using embedded CIOMSSimulator instance")
            instance = CIOMSSimulator()
        else:
            log.debug("Creating xmlrpclib.ServerProxy: uri=%s", uri)
            instance = xmlrpclib.ServerProxy(uri, allow_none=True)
            log.debug("Created xmlrpclib.ServerProxy: uri=%s", uri)

        cls._inst_count += 1
        log.debug("create_instance: _inst_count = %d", cls._inst_count)
        return instance

    @classmethod
    def destroy_instance(cls, instance):
        """
        Destroys an instance created with create_instance.
        This is mainly a convenience method to deactivate the simulator when
        run in embedded form.
        """
        cls._inst_count -= 1
        if isinstance(instance, CIOMSSimulator):
            instance._deactivate_simulator()
            log.debug("Embedded CIOMSSimulator instance destroyed")

        # else: nothing needed to do.
            
        log.debug("destroy_instance: _inst_count = %d", cls._inst_count)

    @classmethod
    def launch_simulator(cls, inactivity_period):
        """
        Utility to launch the simulator as a separate process.

        @return the new URI for a regular call to create_instance(uri).
        """

        # in case there's any ongoing simulator process:
        ex = cls.stop_launched_simulator()
        if ex:
            log.warn("[OMSim] previous process could not be stopped properly. "
                     "The next launch may fail because of potential conflict.")

        from ion.agents.platform.rsn.simulator.process_util import ProcessUtil
        cls._sim_process = ProcessUtil()
        cls._rsn_oms, uri = cls._sim_process.launch()

        log.debug("launch_simulator: launched. uri=%s", uri)

        if inactivity_period:
            cls._rsn_oms.x_exit_inactivity(inactivity_period)

            def hearbeat():
                n = 0
                while cls._sim_process:
                    sleep(1)
                    n += 1
                    if cls._sim_process and n % 20 == 0:
                        log.debug("[OMSim] heartbeat sent")
                        try:
                            cls._rsn_oms.ping()
                        except Exception:
                            pass
                log.debug("[OMSim] heartbeat ended")

            Greenlet(hearbeat).start()
            log.debug("[OMSim] called x_exit_inactivity with %s and started heartbeat",
                      inactivity_period)

        return uri

    @classmethod
    def get_rsn_oms_for_launched_simulator(cls):
        """
        Returns the CIOMSClient instance created in the last call to
        launch_simulator and that has not been stopped yet, if any.
        """
        return cls._rsn_oms

    @classmethod
    def stop_launched_simulator(cls):
        """
        Utility to stop the process launched with launch_simulator.
        The stop is attempted a couple of times in case of errors (with a few
        seconds of sleep in between).

        @return None if process seems to have been stopped properly.
                Otherwise the exception of the last attempt to stop it.
        """
        if cls._sim_process:
            sim_proc, cls._sim_process = cls._sim_process, None
            attempts = 3
            attempt = 0
            while attempt <= attempts:
                attempt += 1
                log.debug("[OMSim] stopping launched simulator (attempt=%d) ...", attempt)
                try:
                    sim_proc.stop()
                    log.debug("[OMSim] simulator process seems to have stopped properly")
                    return None

                except Exception as ex:
                    if attempt < attempts:
                        sleep(10)
                    else:
                        log.warn("[OMSim] error while stopping simulator process: %s", ex)
                        return ex
