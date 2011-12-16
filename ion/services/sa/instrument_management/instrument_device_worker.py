#!/usr/bin/env python

__author__ = 'Ian Katz'
__license__ = 'Apache 2.0'

#from pyon.core.exception import BadRequest, NotFound
from pyon.core.bootstrap import IonObject
from pyon.public import AT
from pyon.util.log import log

######
"""
now TODO


Later TODO

 - fix lifecycle states... how?
 - 

"""
######




from ion.services.sa.instrument_management.ims_worker import IMSworker

class InstrumentDeviceWorker(IMSworker):

    def _primary_object_name(self):
        return "InstrumentDevice"

    def _primary_object_label(self):
        return "instrument_device"

    ##################### INSTRUMENT LIFECYCLE METHODS

    def plan(self, name='', description='', instrument_model_id=''):
        """
        Plan an instrument: at this point, we know only its name, description, and model
        """

        #create the new resource
        new_inst_obj = IonObject("InstrumentDevice",
                                 name=name,
                                 description=description)
        instrument_device_id = self.create_one(instrument_device=new_inst_obj)

        #associate the model
        associate_success = self.RR.create_association(instrument_device_id, AT.hasModel, instrument_model_id)
        log.debug("Create hasModel Association: %s" % str(associate_success))
        
        self.RR.execute_lifecycle_transition(resource_id=instrument_device_id, 
                                                                    lcstate='PLANNED')

        return self._return_create("instrument_device_id", instrument_device_id)
        

    def acquire(self, instrument_device_id='', serialnumber='', firmwareversion='', hardwareversion=''):
        """
        When physical instrument is acquired, create all data products
        """

        #read instrument
        inst_obj = self.read(instrument_device_id=instrument_device_id)

        #update instrument with new params
        inst_obj.serialnumber     = serialnumber
        inst_obj.firmwareversion  = firmwareversion
        inst_obj.hardwareversion  = hardwareversion

        #FIXME: check this for an error
        self.update(instrument_device_id, inst_obj)

        
        #get data producer id from data acquisition management service
        pducer_id = self.DAMS.register_instrument(instrument_id=instrument_device_id)

        # associate data product with instrument
        _ = self.RR.create_association(instrument_device_id, AT.hasDataProducer, pducer_id)
        
        #get data product id from data product management service
        dpms_pduct_obj = IonObject("DataProduct", 
                                   name=str(inst_obj.name + " L0 Product"),
                                   description=str("DataProduct for " + inst_obj.name))

        dpms_pducer_obj = IonObject("DataProducer", 
                                    name=str(inst_obj.name + " L0 Producer"),
                                    description=str("DataProducer for " + inst_obj.name))

        pduct_id = self.DPMS.create_data_product(data_product=dpms_pduct_obj, data_producer=dpms_pducer_obj)


        # get data product's data produceer (via association)
        assoc_ids, _ = self.RR.find_associations(pduct_id, AT.hasDataProducer, None, True)
        # (FIXME: there should only be one assoc_id.  what error to raise?)
        # FIXME: what error to raise if there are no assoc ids?

        # instrument data producer is the parent of the data product producer
        associate_success = self.RR.create_association(pducer_id, AT.hasChildDataProducer, assoc_ids[0])
        log.debug("Create hasChildDataProducer Association: %s" % str(associate_success))
        

        self.RR.execute_lifecycle_transition(resource_id=instrument_device_id, 
                                                                    lcstate='ACQUIRED')

        #FIXME: error checking

        return self._return_update(True)


    def develop(self, instrument_device_id='', instrument_agent_id=''):
        """
        Assign an instrument agent (just the type, not the instance) to an instrument
        """
        #FIXME: only valid in 'ACQUIRED' state!

        associate_success = self.RR.create_association(instrument_device_id, AT.hasAgent, instrument_agent_id)
        log.debug("Create hasAgent Association: %s" % str(associate_success))


        self.RR.execute_lifecycle_transition(resource_id=instrument_device_id, 
                                                                    lcstate='DEVELOPED')
        #FIXME: error checking

        return self._return_update(True)


    def commission(self, instrument_device_id='', platform_device_id=''):
        #FIXME: only valid in 'DEVELOPED' state!

        #FIXME: there seems to be no association between instruments and platforms
        self.RR.execute_lifecycle_transition(resource_id=instrument_device_id, 
                                                                    lcstate='COMMISSIONED')
        return self._return_update(True)


    def decommission(self, instrument_device_id=''):
        #FIXME: only valid in 'COMMISSIONED' state!

        #FIXME: there seems to be no association between instruments and platforms
        self.RR.execute_lifecycle_transition(resource_id=instrument_device_id, 
                                                                    lcstate='DEVELOPED')

        return self._return_update(True)


    def activate(self, instrument_device_id='', instrument_agent_instance_id=''):
        """
        method docstring
        """
        #FIXME: only valid in 'COMMISSIONED' state!

        #FIXME: validate somehow

        associate_success = self.RR.create_association(instrument_device_id, AT.hasAgentInstance, instrument_agent_instance_id)
        log.debug("Create hasAgentInstance Association: %s" % str(associate_success))


        self.RR.execute_lifecycle_transition(resource_id=instrument_device_id, 
                                                                    lcstate='ACTIVE')

        self._return_activate(True)


    def deactivate(self, instrument_device_id=''):

        #FIXME: only valid in 'ACTIVE' state!

        #FIXME: remove association
        
        self.RR.execute_lifecycle_transition(resource_id=instrument_device_id, 
                                                                    lcstate='DEVELOPED')

        return self._return_update(True)
        

    def retire(self, instrument_device_id=''):
        """
        Retire an instrument
        """

        #FIXME: what happens to logical instrument, platform, etc

        self.RR.execute_lifecycle_transition(resource_id=instrument_device_id, 
                                             lcstate='RETIRED')
        
        return self._return_update(True)


