#!/usr/bin/env python

from ion.core.bootstrap_process import BootstrapPlugin
from pyon.public import Container
from interface.objects import IngestionQueue
from interface.services.dm.iingestion_management_service import IngestionManagementServiceProcessClient
from ion.services.eoi.table_loader import ResourceParser
from pyon.util.log import log
from pyon.core.exception import BadRequest

class BootstrapEOI(BootstrapPlugin):
    """
    Bootstrap process for EOI management.
    """

    def on_initial_bootstrap(self, process, config, **kwargs):
        """
        EOI BootstrapPlugin

        Resets the geoserver datastore... 
        Performs inital parsing of the eoi datasources
        """
        using_eoi_services = config.get_safe('eoi.meta.use_eoi_services', False)
        if using_eoi_services:
            try:
                r = ResourceParser()
                r.reset()
            except Exception:
                log.error("Error resetting EOI Services")
        else:
            log.info("EOI Services are not enabled")
        
        
