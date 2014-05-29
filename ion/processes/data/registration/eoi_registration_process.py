#!/usr/bin/env python
from pyon.ion.process import SimpleProcess
from pyon.ion.event import EventSubscriber
from pyon.public import OT, RT
import requests
from pyon.public import CFG
from pyon.util.log import log
from pyon.util.breakpoint import breakpoint

# GeoNetwork
# TODO should maybe go in the yml file
REQUEST_HARVESTER = "requestharvester"
CREATE_HARVESTER = "createharvester"
UPDATE_HARVESTER = "updateharvester"
REMOVE_HARVESTER = "removeharvester"
RESET_HARVESTER = "resetharvester"
START_HARVESTER = "startharvester"
STOP_HARVESTER = "stopharvester"
RUN_HARVESTER = "runharvester"

class EOIRegistrationProcess(SimpleProcess):

    def on_start(self):
        self.data_source_subscriber = EventSubscriber(event_type=OT.ResourceModifiedEvent,
                                                      origin_type=RT.DataSource,
                                                      callback=self._register_data_source)
        self.provider_subscriber = EventSubscriber(event_type=OT.ResourceModifiedEvent,
                                                      origin_type=RT.ExternalDataProvider,
                                                      callback=self._register_provider)
        self.data_source_subscriber.start()
        self.provider_subscriber.start()

        self.rr = self.container.resource_registry

        self.using_eoi_services = CFG.get_safe('eoi.meta.use_eoi_services', False)
        self.server = CFG.get_safe('eoi.importer_service.server', "localhost")+":"+str(CFG.get_safe('eoi.importer_service.port', 8844))

        log.info("Using geoservices="+str(self.using_eoi_services))
        if not self.using_eoi_services:
            log.warn("not using geoservices...") 

        self.importer_service_available = self.check_for_importer_service()
        if not self.importer_service_available:
            log.warn("not using importer service...")  

    def check_for_importer_service(self):
        '''
        only gets run on start, used to identify if importer service is available
        '''        
        try:
            r = requests.get(self.server+'/service=alive&name=ooi&id=ooi')
            log.info("importer service available, status code: %s", str(r.status_code))
            #alive service returned ok
            if r.status_code == 200:
                return True
            else:
                return False
        except Exception as e:
            #SERVICE IS REALLY NOT AVAILABLE
            log.warn("importer service is really not available...%s", e)
            return False    


    def _register_data_source(self, event, *args, **kwargs):        
        '''
        used to create a harvester
        '''
        if self.importer_service_available:
            obj = self.rr.read(event.origin)        
            data_fields = []
            for attrname, value in vars(obj).iteritems():           
                #generate th param list to pass to importer service using field names
                if attrname is not "contact":
                    f = attrname.replace("_", "")+"="+str(obj[attrname])
                    data_fields.append(f)

            param_list = '&'.join(data_fields)

            request_string = self.server+'/service='+CREATE_HARVESTER+"&"+param_list            
            r = requests.get(request_string)


    def _register_provider(self, event, *args, **kwargs):
        if self.importer_service_available:
            #print "provider id:", event.origin
            pass
            

    def on_quit(self):
        self.data_source_subscriber.stop()
        self.provider_subscriber.stop()

