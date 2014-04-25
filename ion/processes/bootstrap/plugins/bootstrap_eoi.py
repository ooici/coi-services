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
        """
        r = ResourceParser()
        try:
            r.init()
            r.reset()
        except BadRequest as e:
            if 'Eoi services not enabled' in e.message:
                log.warning("EOI Services are not enabled")
        except:
            log.error("EOI Services are disabled", exc_info=True)
        
        
