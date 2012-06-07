#!/usr/bin/env python

"""
@package  ion.services.sa.instrument.instrument_management_service
@author   Maurice Manning
@author   Ian Katz
"""


#from pyon.public import Container
from pyon.public import LCE
from pyon.public import RT, PRED
from pyon.public import CFG
from pyon.core.bootstrap import IonObject
from pyon.core.exception import Inconsistent,BadRequest, NotFound
#from pyon.datastore.datastore import DataStore
#from pyon.net.endpoint import RPCClient
from pyon.util.log import log
from ion.services.sa.instrument.flag import KeywordFlag
import os
import pwd
import gevent
import base64
import zipfile
import string
import csv
from StringIO import StringIO
import tempfile
import subprocess

from interface.objects import ProcessDefinition
from interface.objects import AttachmentType
#from interface.objects import ProcessSchedule, ProcessTarget


from ion.services.sa.instrument.instrument_agent_impl import InstrumentAgentImpl
from ion.services.sa.instrument.instrument_agent_instance_impl import InstrumentAgentInstanceImpl
from ion.services.sa.instrument.instrument_model_impl import InstrumentModelImpl
from ion.services.sa.instrument.instrument_device_impl import InstrumentDeviceImpl

from ion.services.sa.instrument.platform_agent_impl import PlatformAgentImpl
from ion.services.sa.instrument.platform_agent_instance_impl import PlatformAgentInstanceImpl
from ion.services.sa.instrument.platform_model_impl import PlatformModelImpl
from ion.services.sa.instrument.platform_device_impl import PlatformDeviceImpl

from ion.services.sa.instrument.sensor_model_impl import SensorModelImpl
from ion.services.sa.instrument.sensor_device_impl import SensorDeviceImpl

# TODO: these are for methods which may belong in DAMS/DPMS/MFMS
from ion.services.sa.instrument.data_product_impl import DataProductImpl
from ion.services.sa.instrument.data_producer_impl import DataProducerImpl

from ion.agents.port.logger_process import EthernetDeviceLogger
from ion.agents.instrument.instrument_agent import InstrumentAgentState

from interface.services.sa.iinstrument_management_service import BaseInstrumentManagementService


INSTRUMENT_AGENT_MANIFEST_FILE = "MANIFEST.csv"
 

class InstrumentManagementService(BaseInstrumentManagementService):
    """
    @brief Service to manage instrument, platform, and sensor resources, their relationships, and direct access

    """
    def on_init(self):
        #suppress a few "variable declared but not used" annoying pyflakes errors
        IonObject("Resource")

        self.override_clients(self.clients)
        self._pagent = None

    def override_clients(self, new_clients):
        """
        Replaces the service clients with a new set of them... and makes sure they go to the right places
        """

        #shortcut names for the import sub-services
        # we hide these behind checks even though we expect them so that
        # the resource_impl_metatests will work
        if hasattr(self.clients, "resource_registry"):
            self.RR    = self.clients.resource_registry

        if hasattr(self.clients, "data_acquisition_management"):
            self.DAMS  = self.clients.data_acquisition_management

        if hasattr(self.clients, "data_product_management"):
            self.DPMS  = self.clients.data_product_management

        if hasattr(self.clients, "pubsub_management"):
            self.PSMS = self.clients.pubsub_management

        #farm everything out to the impls

        self.instrument_agent           = InstrumentAgentImpl(self.clients)
        self.instrument_agent_instance  = InstrumentAgentInstanceImpl(self.clients)
        self.instrument_model           = InstrumentModelImpl(self.clients)
        self.instrument_device          = InstrumentDeviceImpl(self.clients)

        self.platform_agent           = PlatformAgentImpl(self.clients)
        self.platform_agent_instance  = PlatformAgentInstanceImpl(self.clients)
        self.platform_model           = PlatformModelImpl(self.clients)
        self.platform_device          = PlatformDeviceImpl(self.clients)

        self.sensor_model    = SensorModelImpl(self.clients)
        self.sensor_device   = SensorDeviceImpl(self.clients)

        #TODO: may not belong in this service
        self.data_product        = DataProductImpl(self.clients)
        self.data_producer       = DataProducerImpl(self.clients)




    ##########################################################################
    #
    # INSTRUMENT AGENT INSTANCE
    #
    ##########################################################################

    def create_instrument_agent_instance(self, instrument_agent_instance=None, instrument_agent_id="", instrument_device_id=""):
        """
        create a new instance
        @param instrument_agent_instance the object to be created as a resource
        @retval instrument_agent_instance_id the id of the new object
        @throws BadRequest if the incoming _id field is set
        @throws BadReqeust if the incoming name already exists
        """

        instrument_agent_obj = self.read_instrument_agent(instrument_agent_id)

        instrument_device_obj = self.read_instrument_device(instrument_device_id)

        instrument_agent_instance_id = self.instrument_agent_instance.create_one(instrument_agent_instance)

        self.assign_instrument_agent_to_instrument_agent_instance(instrument_agent_id, instrument_agent_instance_id)


        self.assign_instrument_agent_instance_to_instrument_device(instrument_agent_instance_id, instrument_device_id)
        log.debug("create_instrument_agent_instance: device %s now connected to instrument agent instance %s (L4-CI-SA-RQ-363)", str(instrument_device_id),  str(instrument_agent_instance_id))

        return instrument_agent_instance_id

    def update_instrument_agent_instance(self, instrument_agent_instance=None):
        """
        update an existing instance
        @param instrument_agent_instance the object to be created as a resource
        @retval success whether we succeeded
        @throws BadRequest if the incoming _id field is not set
        @throws BadReqeust if the incoming name already exists
        """
        return self.instrument_agent_instance.update_one(instrument_agent_instance)


    def read_instrument_agent_instance(self, instrument_agent_instance_id=''):
        """
        fetch a resource by ID
        @param instrument_agent_instance_id the id of the object to be fetched
        @retval InstrumentAgentInstance resource
        """
        return self.instrument_agent_instance.read_one(instrument_agent_instance_id)

    def delete_instrument_agent_instance(self, instrument_agent_instance_id=''):
        """
        delete a resource, including its history (for less ominous deletion, use retire)
        @param instrument_agent_instance_id the id of the object to be deleted
        @retval success whether it succeeded

        """

        self.instrument_agent_instance._unlink_all_subjects_by_association_type(PRED.hasInstance, instrument_agent_instance_id)
        self.instrument_agent_instance._unlink_all_subjects_by_association_type(PRED.hasAgentInstance, instrument_agent_instance_id)

        self.instrument_agent_instance.delete_one(instrument_agent_instance_id)

        return

    def start_instrument_agent_instance(self, instrument_agent_instance_id=''):
        """
        Agent instance must first be created and associated with a instrument device
        Launch the instument agent instance and return the id
        """

        instrument_agent_instance_obj = self.RR.read(instrument_agent_instance_id)

        #retrieve the associated instrument device
        inst_device_ids, _ = self.RR.find_subjects(RT.InstrumentDevice, PRED.hasAgentInstance, instrument_agent_instance_id, True)
        if not inst_device_ids:
            raise NotFound("No Instrument Device attached to this Instrument Agent Instance " + str(instrument_agent_instance_id))
        if len(inst_device_ids) > 1:
            raise BadRequest("Instrument Agent Instance should only have ONE Instrument Device" + str(instrument_agent_instance_id))
        instrument_device_id = inst_device_ids[0]
        log.debug("start_instrument_agent_instance: device is %s connected to instrument agent instance %s (L4-CI-SA-RQ-363)", str(instrument_device_id),  str(instrument_agent_instance_id))

        #retrieve the instrument model
        model_ids, _ = self.RR.find_objects(instrument_device_id, PRED.hasModel, RT.InstrumentModel, True)
        if not model_ids:
            raise NotFound("No Instrument Model  attached to this Instrument Device " + str(instrument_device_id))

        instrument_model_id = model_ids[0]
        log.debug("activate_instrument:instrument_model %s"  +  str(instrument_model_id))


        #retrieve the associated instrument agent
        agent_ids, _ = self.RR.find_subjects(RT.InstrumentAgent, PRED.hasModel, instrument_model_id, True)
        if not agent_ids:
            raise NotFound("No Instrument Agent  attached to this Instrument Model " + str(instrument_model_id))

        instrument_agent_id = agent_ids[0]
        log.debug("Got instrument agent '%s'" % instrument_agent_id)


        #retrieve the associated process definition
        process_def_ids, _ = self.RR.find_objects(instrument_agent_id, PRED.hasProcessDefinition, RT.ProcessDefinition, True)
        if not process_def_ids:
            raise NotFound("No Process Definition  attached to this Instrument Agent " + str(instrument_agent_id))
        if len(process_def_ids) > 1:
            raise BadRequest("Instrument Agent should only have ONE Process Definition" + str(instrument_agent_id))

        process_definition_id = process_def_ids[0]
        log.debug("activate_instrument: agent process definition %s"  +  str(process_definition_id))

        # retrieve the process definition information
        process_def_obj = self.RR.read(process_definition_id)
        if not process_def_obj:
            raise NotFound("ProcessDefinition %s does not exist" % process_definition_id)


        out_streams = {}
        #retrieve the output products
        data_product_ids, _ = self.RR.find_objects(instrument_device_id, PRED.hasOutputProduct, RT.DataProduct, True)
        if not data_product_ids:
            raise NotFound("No output Data Products attached to this Instrument Device " + str(instrument_device_id))

        for product_id in data_product_ids:
            stream_ids, _ = self.RR.find_objects(product_id, PRED.hasStream, RT.Stream, True)

            log.debug("activate_instrument:output stream ids: %s"  +  str(stream_ids))
            #One stream per product ...for now.
            if not stream_ids:
                raise NotFound("No Stream  attached to this Data Product " + str(product_id))
            if len(stream_ids) > 1:
                raise Inconsistent("Data Product should only have ONE Stream" + str(product_id))

            # retrieve the stream
            stream_obj = self.RR.read(stream_ids[0])
            if not stream_obj:
                raise NotFound("Stream %s does not exist" % stream_ids[0])

#            log.debug("activate_instrument:output stream name: %s"  +  str(stream_obj.name))
#            log.debug("activate_instrument:output stream name find parsed %s", str(stream_obj.name.lower().find('parsed')) )
#            log.debug("activate_instrument:output stream name find raw %s", str(stream_obj.name.lower().find('raw')) )

            #todo  - Replace this hack: look in the data product name for 'raw' or 'parsed'

            if stream_obj.name.lower().find('parsed') > -1 :
                out_streams['ctd_parsed'] = stream_ids[0]
                log.debug("activate_instrument:ctd_parsed %s ", str(stream_ids[0]) )
            elif stream_obj.name.lower().find('raw') > -1:
                out_streams['ctd_raw'] = stream_ids[0]
                log.debug("activate_instrument:ctd_raw %s ", str(stream_ids[0]) )
            else:
                raise NotFound("Stream %s is not CTD raw or parsed" % stream_obj.name)


        # Start port agent, add stop to cleanup.
        self._start_pagent(instrument_agent_instance_obj)

        # todo: this is hardcoded to the SBE37 model; need to abstract the driver configuration when more instruments are coded
        # todo: how to tell which prod is raw and which is parsed? Check the name?
        #stream_config = {"ctd_raw":out_streams["ctd_raw"], "ctd_parsed":out_streams["ctd_parsed"]}
        log.debug("activate_instrument:output stream config: %s"  +  str(out_streams))
        # Driver configuration.
#        driver_config = {
#            'svr_addr': instrument_agent_instance_obj.svr_addr,
#            'cmd_port': int(instrument_agent_instance_obj.cmd_port),
#            'evt_port':int(instrument_agent_instance_obj.evt_port),
#            'dvr_mod': instrument_agent_instance_obj.driver_module,
#            'dvr_cls': instrument_agent_instance_obj.driver_class,
#            'comms_config': {
#                SBE37Channel.CTD: {
#                    'method':instrument_agent_instance_obj.comms_method,
#                    'device_addr': instrument_agent_instance_obj.comms_device_address,
#                    'device_port': int(instrument_agent_instance_obj.comms_device_port),
#                    'server_addr': instrument_agent_instance_obj.comms_server_address,
#                    'server_port': int(instrument_agent_instance_obj.comms_server_port)
#                    }
#                }
#            }

#        driver_config = {
#            'svr_addr': instrument_agent_instance_obj.svr_addr,
#            'cmd_port': int(instrument_agent_instance_obj.cmd_port),
#            'evt_port':int(instrument_agent_instance_obj.evt_port),
#            'dvr_mod': instrument_agent_instance_obj.driver_module,
#            'dvr_cls': instrument_agent_instance_obj.driver_class,
#            'comms_config': {
#                    'addr': instrument_agent_instance_obj.comms_device_address,
#                    'port': int(instrument_agent_instance_obj.comms_device_port),
#                }
#            }

        # Create agent config.
        instrument_agent_instance_obj.agent_config = {
            'driver_config' : instrument_agent_instance_obj.driver_config,
            'stream_config' : out_streams,
            'agent'         : {'resource_id': instrument_device_id}
        }

        log.debug("activate_instrument: agent_config %s ", str(instrument_agent_instance_obj.agent_config))

        pid = self.clients.process_dispatcher.schedule_process(process_definition_id=process_definition_id,
                                                               schedule=None,
                                                               configuration=instrument_agent_instance_obj.agent_config)
        log.debug("activate_instrument: schedule_process %s", pid)


        # add the process id and update the resource
        instrument_agent_instance_obj.agent_process_id = pid
        self.update_instrument_agent_instance(instrument_agent_instance_obj)

        return

    def _start_pagent(self, instrument_agent_instance=None):
        """
        Construct and start the port agent.
        """

        # Create port agent object.
        this_pid = os.getpid()
        self._pagent = EthernetDeviceLogger.launch_process(
            instrument_agent_instance.comms_device_address,
            int(instrument_agent_instance.comms_device_port),
            instrument_agent_instance.port_agent_work_dir,
            instrument_agent_instance.port_agent_delimeter,
            this_pid)


        # Get the pid and port agent server port number.
        pid = self._pagent.get_pid()
        while not pid:
            gevent.sleep(.1)
            pid = self._pagent.get_pid()
        port = self._pagent.get_port()
        while not port:
            gevent.sleep(.1)
            port = self._pagent.get_port()

        # Configure driver to use port agent port number.
        instrument_agent_instance.driver_config['comms_config'] = {
            'addr' : 'localhost',
            'port' : port
        }

        # Report.
        log.info('Started port agent pid %d listening at port %d.', pid, port)


    def stop_instrument_agent_instance(self, instrument_agent_instance_id=''):
        """
        Deactivate the instrument agent instance
        """
        instrument_agent_instance_obj = self.RR.read(instrument_agent_instance_id)

        # Cancels the execution of the given process id.
        self.clients.process_dispatcher.cancel_process(instrument_agent_instance_obj.agent_process_id)
        
        instrument_agent_instance_obj.agent_process_id = None

        self.RR.update(instrument_agent_instance_obj)

        """
        Stop the port agent.
        """
#        if self._pagent:
#            pid = self._pagent.get_pid()
#            if pid:
#                log.info('Stopping pagent pid %i.', pid)
#                self._pagent.stop()
#            else:
#                log.warning('No port agent running.')

        return


    def find_instrument_agent_instances(self, filters=None):
        """

        """
        return self.instrument_agent_instance.find_some(filters)




    ##########################################################################
    #
    # INSTRUMENT AGENT
    #
    ##########################################################################

    def create_instrument_agent(self, instrument_agent=None):
        """
        create a new instance
        @param instrument_agent the object to be created as a resource
        @retval instrument_agent_id the id of the new object
        @throws BadRequest if the incoming _id field is set
        @throws BadReqeust if the incoming name already exists
        """
        instrument_agent_id = self.instrument_agent.create_one(instrument_agent)

        # Create the process definition to launch the agent
        process_definition = ProcessDefinition()
        process_definition.executable['module']='ion.agents.instrument.instrument_agent'
        process_definition.executable['class'] = 'InstrumentAgent'
        process_definition_id = self.clients.process_dispatcher.create_process_definition(process_definition=process_definition)
        log.debug("create_instrument_agent: create_process_definition id %s"  +  str(process_definition_id))

        #associate the agent and the process def
        self.RR.create_association(instrument_agent_id,  PRED.hasProcessDefinition, process_definition_id)

        return instrument_agent_id

    def update_instrument_agent(self, instrument_agent=None):
        """
        update an existing instance
        @param instrument_agent the object to be created as a resource
        @retval success whether we succeeded
        @throws BadRequest if the incoming _id field is not set
        @throws BadReqeust if the incoming name already exists
        """
        return self.instrument_agent.update_one(instrument_agent)


    def read_instrument_agent(self, instrument_agent_id=''):
        """
        fetch a resource by ID
        @param instrument_agent_id the id of the object to be fetched
        @retval InstrumentAgent resource
        """
        return self.instrument_agent.read_one(instrument_agent_id)

    def delete_instrument_agent(self, instrument_agent_id=''):
        """
        delete a resource, including its history (for less ominous deletion, use retire)
        @param instrument_agent_id the id of the object to be deleted
        @retval success whether it succeeded

        """
        #retrieve the associated process definition
        process_def_ids, _ = self.clients.resource_registry.find_objects(instrument_agent_id, PRED.hasProcessDefinition, RT.ProcessDefinition, True)
        if not process_def_ids:
            raise NotFound("No Process Definition  attached to this Instrument Agent " + str(instrument_agent_id))
        if len(process_def_ids) > 1:
            raise BadRequest("Instrument Agent should only have ONE Process Definition" + str(instrument_agent_id))

        assoc_ids, _ = self.clients.resource_registry.find_associations(instrument_agent_id, PRED.hasProcessDefinition, RT.ProcessDefinition, True)
        self.clients.resource_registry.delete_association(assoc_ids[0])

        self.clients.process_dispatcher.delete_process_definition(process_def_ids[0])

        return self.instrument_agent.delete_one(instrument_agent_id)


    def register_instrument_agent(self, instrument_agent_id='', agent_egg='', qa_documents=''):
        """
        register an instrument driver by putting it in a web-accessible location
        @instrument_agent_id the agent receiving the driver
        @agent_egg a base64-encoded egg file
        @qa_documents a base64-encoded zip file containing a MANIFEST.csv file 

        MANIFEST.csv fields:
         - filename
         - name
         - description
         - content_type
         - keywords
        """
        
        def zip_of_b64(b64_data, title):

            log.debug("decoding base64 zipfile for %s" % title)
            try:
                zip_str  = base64.decodestring(b64_data)
            except:
                raise BadRequest("could not base64 decode supplied %s" % title)

            log.debug("opening zipfile for %s" % title)
            try:
                zip_file = StringIO(zip_str)
                zip_obj  = zipfile.ZipFile(zip_file)
            except:
                raise BadRequest("could not parse zipfile contained in %s" % title)

            return zip_obj

        # retrieve the resource
        log.debug("reading inst agent resource (for proof of existence)")
        instrument_agent_obj = self.instrument_agent.read_one(instrument_agent_id)


        #process the input files (base64-encoded zips)
        qa_zip_obj  = zip_of_b64(qa_documents, "qa_documents")
        egg_zip_obj = zip_of_b64(agent_egg, "agent_egg")


        #parse the manifest file
        if not INSTRUMENT_AGENT_MANIFEST_FILE in qa_zip_obj.namelist():
            raise BadRequest("provided qa_documents zipfile lacks manifest CSV file called %s" % 
                             INSTRUMENT_AGENT_MANIFEST_FILE)

        log.debug("extracting manifest csv file")
        csv_contents = qa_zip_obj.read(INSTRUMENT_AGENT_MANIFEST_FILE)

        log.debug("parsing manifest csv file")
        try:
            dialect = csv.Sniffer().sniff(csv_contents)
        except csv.Error:
            dialect = csv.excel
        except Exception as e:
            raise BadRequest("%s - %s" % (str(type(e)), str(e.args)))
        csv_reader = csv.DictReader(StringIO(csv_contents), dialect=dialect)

        #validate fields in manifest file
        log.debug("validing manifest csv file")
        for f in ["filename", "name", "description", "content_type", "keywords"]:
            if not f in csv_reader.fieldnames:
                raise BadRequest("Manifest file %s missing required field %s" % 
                                 (INSTRUMENT_AGENT_MANIFEST_FILE, f))


        #validate egg
        log.debug("validating egg")
        if not "EGG-INFO/PKG-INFO" in egg_zip_obj.namelist():
            raise BadRequest("no PKG-INFO found in egg; found %s" % str(egg_zip_obj.namelist()))

        log.debug("processing driver")
        pkg_info_data = {}
        pkg_info = egg_zip_obj.read("EGG-INFO/PKG-INFO")
        for l in pkg_info.splitlines():
            log.debug("Reading %s" % l)
            tmp = l.partition(": ")
            pkg_info_data[tmp[0]] = tmp[2]

        for f in ["Name", "Version"]:
            if not f in pkg_info_data:
                raise BadRequest("Agent egg's PKG-INFO did not include a field called '%s'" % f)

        #determine egg name
        egg_filename = "%s-%s-py2.7.egg" % (pkg_info_data["Name"].replace("-", "_"), pkg_info_data["Version"])
        log.debug("Egg filename is '%s'" % egg_filename)

        egg_url = "http://%s%s/%s" % (CFG.service.instrument_management.driver_release_host,
                                      CFG.service.instrument_management.driver_release_directory,
                                      egg_filename)

        egg_urlfile = "%s v%s.url" % (pkg_info_data["Name"].replace("-", "_"), pkg_info_data["Version"])




        #create attachment resources for each document in the zip
        log.debug("creating attachment objects")
        attachments = []
        for row in csv_reader:
            att_name = row["filename"]
            att_desc = row["description"]
            att_content_type = row["content_type"]
            att_keywords = string.split(row["keywords"], ",")

            if not att_name in qa_zip_obj.namelist():
                raise BadRequest("Manifest refers to a file called '%s' which is not in the zip" % att_name)

            attachments.append(IonObject(RT.Attachment,
                                         name=att_name,
                                         description=att_desc,
                                         content=qa_zip_obj.read(att_name), 
                                         content_type=att_content_type,
                                         keywords=att_keywords,
                                         attachment_type=AttachmentType.BLOB))
            
        log.debug("Sanity checking manifest vs zip file")
        if len(qa_zip_obj.namelist()) - 1 > len(attachments):
            log.warn("There were %d files in the zip but only %d in the manifest" % 
                     (len(qa_zip_obj.namelist()) - 1, len(attachments)))
            

        

        #move output egg to another directory / upload it somewhere
        #TODO: change cfg_ to CFG.x.

        cfg_host        = CFG.service.instrument_management.driver_release_host #'amoeaba.ucsd.edu'
        cfg_remotepath  = CFG.service.instrument_management.driver_release_directory #'/var/www/release'
        cfg_user        = pwd.getpwuid(os.getuid())[0]

        log.debug("creating tempfile for egg output")
        f_handle, tempfilename = tempfile.mkstemp()
        log.debug("writing egg data to disk at '%s'" % tempfilename)
        os.write(f_handle, base64.decodestring(agent_egg))

        remotefilename = "%s@%s:%s/%s" % (cfg_user, 
                                          cfg_host, 
                                          cfg_remotepath, 
                                          egg_filename)

        log.debug("executing scp: '%s' to %s" % (tempfilename, remotefilename))
        scp_retval = subprocess.call(["scp", "-q", "-o", "PasswordAuthentication=no", 
                                      tempfilename, remotefilename])
        
        if 0 != scp_retval:
            raise BadRequest("Secure copy to %s:%s failed" % (cfg_host, cfg_remotepath))

        log.debug("removing tempfile at '%s'" % tempfilename)
        os.unlink(tempfilename)


        #now we can do the ION side of things

        #make an attachment for the url
        attachments.append(IonObject(RT.Attachment,
                                     name=egg_urlfile,
                                     description="url to egg",
                                     content="[InternetShortcut]\nURL=%s" % egg_url,
                                     content_type="text/url",
                                     keywords=[KeywordFlag.EGG_URL],
                                     attachment_type=AttachmentType.ASCII))

        #insert all attachments
        log.debug("inserting attachments")
        for att in attachments:
            self.RR.create_attachment(instrument_agent_id, att)

        #updates the state of this InstAgent to integrated
        log.debug("firing life cycle event: integrate")
        self.instrument_agent.advance_lcs(instrument_agent_id, LCE.INTEGRATE)


        return



    ##########################################################################
    #
    # INSTRUMENT MODEL
    #
    ##########################################################################

    def create_instrument_model(self, instrument_model=None):
        """
        create a new instance
        @param instrument_model the object to be created as a resource
        @retval instrument_model_id the id of the new object
        @throws BadRequest if the incoming _id field is set
        @throws BadReqeust if the incoming name already exists
        """
        return self.instrument_model.create_one(instrument_model)

    def update_instrument_model(self, instrument_model=None):
        """
        update an existing instance
        @param instrument_model the object to be created as a resource
        @retval success whether we succeeded
        @throws BadRequest if the incoming _id field is not set
        @throws BadReqeust if the incoming name already exists
        """
        return self.instrument_model.update_one(instrument_model)


    def read_instrument_model(self, instrument_model_id=''):
        """
        fetch a resource by ID
        @param instrument_model_id the id of the object to be fetched
        @retval InstrumentModel resource
        """
        return self.instrument_model.read_one(instrument_model_id)

    def delete_instrument_model(self, instrument_model_id=''):
        """
        delete a resource, including its history (for less ominous deletion, use retire)
        @param instrument_model_id the id of the object to be deleted
        @retval success whether it succeeded

        """
        return self.instrument_model.delete_one(instrument_model_id)





    ##########################################################################
    #
    # PHYSICAL INSTRUMENT
    #
    ##########################################################################


    def create_instrument_device(self, instrument_device=None):
        """
        create a new instance
        @param instrument_device the object to be created as a resource
        @retval instrument_device_id the id of the new object
        @throws BadRequest if the incoming _id field is set
        @throws BadReqeust if the incoming name already exists
        """
        instrument_device_id = self.instrument_device.create_one(instrument_device)

        #register the instrument as a data producer
        self.DAMS.register_instrument(instrument_device_id)

        return instrument_device_id

    def update_instrument_device(self, instrument_device=None):
        """
        update an existing instance
        @param instrument_device the object to be created as a resource
        @retval success whether we succeeded
        @throws BadRequest if the incoming _id field is not set
        @throws BadReqeust if the incoming name already exists
        """
        return self.instrument_device.update_one(instrument_device)


    def read_instrument_device(self, instrument_device_id=''):
        """
        fetch a resource by ID
        @param instrument_device_id the id of the object to be fetched
        @retval InstrumentDevice resource

        """
        return self.instrument_device.read_one(instrument_device_id)

    def delete_instrument_device(self, instrument_device_id=''):
        """
        delete a resource, including its history (for less ominous deletion, use retire)
        @param instrument_device_id the id of the object to be deleted
        @retval success whether it succeeded

        """
        return self.instrument_device.delete_one(instrument_device_id)



    ##
    ##
    ##  DIRECT ACCESS
    ##
    ##

    def request_direct_access(self, instrument_device_id=''):
        """

        """

        # determine whether id is for physical or logical instrument
        # look up instrument if not

        # Validate request; current instrument state, policy, and other

        # Retrieve and save current instrument settings

        # Request DA channel, save reference

        # Return direct access channel
        raise NotImplementedError()
        pass

    def stop_direct_access(self, instrument_device_id=''):
        """

        """
        # Return Value
        # ------------
        # {success: true}
        #
        raise NotImplementedError()
        pass







    ##########################################################################
    #
    # PLATFORM AGENT INSTANCE
    #
    ##########################################################################

    def create_platform_agent_instance(self, platform_agent_instance=None):
        """
        create a new instance
        @param platform_agent_instance the object to be created as a resource
        @retval platform_agent_instance_id the id of the new object
        @throws BadRequest if the incoming _id field is set
        @throws BadReqeust if the incoming name already exists
        """
        return self.platform_agent_instance.create_one(platform_agent_instance)

    def update_platform_agent_instance(self, platform_agent_instance=None):
        """
        update an existing instance
        @param platform_agent_instance the object to be created as a resource
        @retval success whether we succeeded
        @throws BadRequest if the incoming _id field is not set
        @throws BadReqeust if the incoming name already exists
        """
        return self.platform_agent_instance.update_one(platform_agent_instance)


    def read_platform_agent_instance(self, platform_agent_instance_id=''):
        """
        fetch a resource by ID
        @param platform_agent_instance_id the id of the object to be fetched
        @retval PlatformAgentInstance resource
        """
        return self.platform_agent_instance.read_one(platform_agent_instance_id)

    def delete_platform_agent_instance(self, platform_agent_instance_id=''):
        """
        delete a resource, including its history (for less ominous deletion, use retire)
        @param platform_agent_instance_id the id of the object to be deleted
        @retval success whether it succeeded

        """
        return self.platform_agent_instance.delete_one(platform_agent_instance_id)





    ##########################################################################
    #
    # PLATFORM AGENT
    #
    ##########################################################################


    def create_platform_agent(self, platform_agent=None):
        """
        create a new instance
        @param platform_agent the object to be created as a resource
        @retval platform_agent_id the id of the new object
        @throws BadRequest if the incoming _id field is set
        @throws BadReqeust if the incoming name already exists
        """
        return self.platform_agent.create_one(platform_agent)

    def update_platform_agent(self, platform_agent=None):
        """
        update an existing instance
        @param platform_agent the object to be created as a resource
        @retval success whether we succeeded
        @throws BadRequest if the incoming _id field is not set
        @throws BadReqeust if the incoming name already exists

        """
        return self.platform_agent.update_one(platform_agent)


    def read_platform_agent(self, platform_agent_id=''):
        """
        fetch a resource by ID
        @param platform_agent_id the id of the object to be fetched
        @retval PlatformAgent resource

        """
        return self.platform_agent.read_one(platform_agent_id)

    def delete_platform_agent(self, platform_agent_id=''):
        """
        delete a resource, including its history (for less ominous deletion, use retire)
        @param platform_agent_id the id of the object to be deleted
        @retval success whether it succeeded

        """
        return self.platform_agent.delete_one(platform_agent_id)


    ##########################################################################
    #
    # PLATFORM MODEL
    #
    ##########################################################################


    def create_platform_model(self, platform_model=None):
        """
        create a new instance
        @param platform_model the object to be created as a resource
        @retval platform_model_id the id of the new object
        @throws BadRequest if the incoming _id field is set
        @throws BadReqeust if the incoming name already exists
        """
        return self.platform_model.create_one(platform_model)

    def update_platform_model(self, platform_model=None):
        """
        update an existing instance
        @param platform_model the object to be created as a resource
        @retval success whether we succeeded
        @throws BadRequest if the incoming _id field is not set
        @throws BadReqeust if the incoming name already exists
        """
        return self.platform_model.update_one(platform_model)


    def read_platform_model(self, platform_model_id=''):
        """
        fetch a resource by ID
        @param platform_model_id the id of the object to be fetched
        @retval PlatformModel resource

        """
        return self.platform_model.read_one(platform_model_id)

    def delete_platform_model(self, platform_model_id=''):
        """
        delete a resource, including its history (for less ominous deletion, use retire)
        @param platform_model_id the id of the object to be deleted
        @retval success whether it succeeded

        """
        return self.platform_model.delete_one(platform_model_id)



    ##########################################################################
    #
    # PHYSICAL PLATFORM
    #
    ##########################################################################



    def create_platform_device(self, platform_device=None):
        """
        create a new instance
        @param platform_device the object to be created as a resource
        @retval platform_device_id the id of the new object
        @throws BadRequest if the incoming _id field is set
        @throws BadReqeust if the incoming name already exists
        """
        return self.platform_device.create_one(platform_device)

    def update_platform_device(self, platform_device=None):
        """
        update an existing instance
        @param platform_device the object to be created as a resource
        @retval success whether we succeeded
        @throws BadRequest if the incoming _id field is not set
        @throws BadReqeust if the incoming name already exists

        """
        return self.platform_device.update_one(platform_device)


    def read_platform_device(self, platform_device_id=''):
        """
        fetch a resource by ID
        @param platform_device_id the id of the object to be fetched
        @retval PlatformDevice resource

        """
        return self.platform_device.read_one(platform_device_id)

    def delete_platform_device(self, platform_device_id=''):
        """
        delete a resource, including its history (for less ominous deletion, use retire)
        @param platform_device_id the id of the object to be deleted
        @retval success whether it succeeded

        """
        return self.platform_device.delete_one(platform_device_id)





    ##########################################################################
    #
    # SENSOR MODEL
    #
    ##########################################################################


    def create_sensor_model(self, sensor_model=None):
        """
        create a new instance
        @param sensor_model the object to be created as a resource
        @retval sensor_model_id the id of the new object
        @throws BadRequest if the incoming _id field is set
        @throws BadReqeust if the incoming name already exists
        """
        return self.sensor_model.create_one(sensor_model)

    def update_sensor_model(self, sensor_model=None):
        """
        update an existing instance
        @param sensor_model the object to be created as a resource
        @retval success whether we succeeded
        @throws BadRequest if the incoming _id field is not set
        @throws BadReqeust if the incoming name already exists

        """
        return self.sensor_model.update_one(sensor_model)


    def read_sensor_model(self, sensor_model_id=''):
        """
        fetch a resource by ID
        @param sensor_model_id the id of the object to be fetched
        @retval SensorModel resource

        """
        return self.sensor_model.read_one(sensor_model_id)

    def delete_sensor_model(self, sensor_model_id=''):
        """
        delete a resource, including its history (for less ominous deletion, use retire)
        @param sensor_model_id the id of the object to be deleted
        @retval success whether it succeeded

        """
        return self.sensor_model.delete_one(sensor_model_id)



    ##########################################################################
    #
    # PHYSICAL SENSOR
    #
    ##########################################################################



    def create_sensor_device(self, sensor_device=None):
        """
        create a new instance
        @param sensor_device the object to be created as a resource
        @retval sensor_device_id the id of the new object
        @throws BadRequest if the incoming _id field is set
        @throws BadReqeust if the incoming name already exists
        """
        return self.sensor_device.create_one(sensor_device)

    def update_sensor_device(self, sensor_device=None):
        """
        update an existing instance
        @param sensor_device the object to be created as a resource
        @retval success whether we succeeded
        @throws BadRequest if the incoming _id field is not set
        @throws BadReqeust if the incoming name already exists

        """
        return self.sensor_device.update_one(sensor_device)


    def read_sensor_device(self, sensor_device_id=''):
        """
        fetch a resource by ID
        @param sensor_device_id the id of the object to be fetched
        @retval SensorDevice resource

        """
        return self.sensor_device.read_one(sensor_device_id)

    def delete_sensor_device(self, sensor_device_id=''):
        """
        delete a resource, including its history (for less ominous deletion, use retire)
        @param sensor_device_id the id of the object to be deleted
        @retval success whether it succeeded

        """
        return self.sensor_device.delete_one(sensor_device_id)



    ##########################################################################
    #
    # ASSOCIATIONS
    #
    ##########################################################################


    def assign_instrument_model_to_instrument_device(self, instrument_model_id='', instrument_device_id=''):
        self.instrument_device.link_model(instrument_device_id, instrument_model_id)

    def unassign_instrument_model_from_instrument_device(self, instrument_model_id='', instrument_device_id=''):
        self.instrument_device.unlink_model(instrument_device_id, instrument_model_id)

    def assign_instrument_model_to_instrument_agent(self, instrument_model_id='', instrument_agent_id=''):
        self.instrument_agent.link_model(instrument_agent_id, instrument_model_id)

    def unassign_instrument_model_from_instrument_agent(self, instrument_model_id='', instrument_agent_id=''):
        self.instrument_agent.unlink_model(instrument_agent_id, instrument_model_id)

    def assign_platform_model_to_platform_agent(self, platform_model_id='', platform_agent_id=''):
        self.platform_agent.link_model(platform_agent_id, platform_model_id)

    def unassign_platform_model_from_platform_agent(self, platform_model_id='', platform_agent_id=''):
        self.platform_agent.unlink_model(platform_agent_id, platform_model_id)

    def assign_sensor_model_to_sensor_device(self, sensor_model_id='', sensor_device_id=''):
        self.sensor_device.link_model(sensor_device_id, sensor_model_id)

    def unassign_sensor_model_from_sensor_device(self, sensor_model_id='', sensor_device_id=''):
        self.sensor_device.unlink_model(sensor_device_id, sensor_model_id)

    def assign_platform_model_to_platform_device(self, platform_model_id='', platform_device_id=''):
        self.platform_device.link_model(platform_device_id, platform_model_id)

    def unassign_platform_model_from_platform_device(self, platform_model_id='', platform_device_id=''):
        self.platform_device.unlink_model(platform_device_id, platform_model_id)

    def assign_instrument_device_to_platform_device(self, instrument_device_id='', platform_device_id=''):
        self.platform_device.link_device(platform_device_id, instrument_device_id)

    def unassign_instrument_device_from_platform_device(self, instrument_device_id='', platform_device_id=''):
        self.platform_device.unlink_device(platform_device_id, instrument_device_id)

    def assign_platform_agent_to_platform_agent_instance(self, platform_agent_id='', platform_agent_instance_id=''):
        self.platform_agent_instance.link_agent_definition(platform_agent_instance_id, platform_agent_id)

    def unassign_platform_agent_from_platform_agent_instance(self, platform_agent_id='', platform_agent_instance_id=''):
        self.platform_agent_instance.unlink_agent_definition(platform_agent_instance_id, platform_agent_id)

    def assign_instrument_agent_to_instrument_agent_instance(self, instrument_agent_id='', instrument_agent_instance_id=''):
        self.instrument_agent_instance.link_agent_definition(instrument_agent_instance_id, instrument_agent_id)

    def unassign_instrument_agent_from_instrument_agent_instance(self, instrument_agent_id='', instrument_agent_instance_id=''):
        self.instrument_agent_instance.unlink_agent_definition(instrument_agent_instance_id, instrument_agent_id)

    def assign_instrument_agent_instance_to_instrument_device(self, instrument_agent_instance_id='', instrument_device_id=''):
        self.instrument_device.link_agent_instance(instrument_device_id, instrument_agent_instance_id)

    def unassign_instrument_agent_instance_from_instrument_device(self, instrument_agent_instance_id='', instrument_device_id=''):
        self.instrument_device.unlink_agent_instance(instrument_device_id, instrument_agent_instance_id)

    def assign_platform_agent_instance_to_platform_device(self, platform_agent_instance_id='', platform_device_id=''):
        self.platform_agent.link_device_instance(platform_device_id, platform_agent_instance_id)

    def unassign_platform_agent_instance_from_platform_device(self, platform_agent_instance_id='', platform_device_id=''):
        self.platform_agent.unlink_device_instance(platform_device_id, platform_agent_instance_id)

    def assign_sensor_device_to_instrument_device(self, sensor_device_id='', instrument_device_id=''):
        self.instrument_device.link_device(instrument_device_id, sensor_device_id)

    def unassign_sensor_device_from_instrument_device(self, sensor_device_id='', instrument_device_id=''):
        self.instrument_device.unlink_device(instrument_device_id, sensor_device_id)


    ##########################################################################
    #
    # DEPLOYMENTS
    #
    ##########################################################################



    def deploy_instrument_device(self, instrument_device_id='', deployment_id=''):
        self.instrument_device.link_deployment(instrument_device_id, deployment_id)

    def undeploy_instrument_device(self, instrument_device_id='', deployment_id=''):
        self.instrument_device.unlink_deployment(instrument_device_id, deployment_id)

    def deploy_platform_device(self, platform_device_id='', deployment_id=''):
        self.platform_device.link_deployment(platform_device_id, deployment_id)

    def undeploy_platform_device(self, platform_device_id='', deployment_id=''):
        self.platform_device.unlink_deployment(platform_device_id, deployment_id)




    ############################
    #
    #  ASSOCIATION FIND METHODS
    #
    ############################


    def find_instrument_model_by_instrument_device(self, instrument_device_id=''):
        return self.instrument_device.find_stemming_model(instrument_device_id)

    def find_instrument_device_by_instrument_model(self, instrument_model_id=''):
        return self.instrument_device.find_having_model(instrument_model_id)

    def find_platform_model_by_platform_device(self, platform_device_id=''):
        return self.platform_device.find_stemming_model(platform_device_id)

    def find_platform_device_by_platform_model(self, platform_model_id=''):
        return self.platform_device.find_having_model(platform_model_id)

    def find_instrument_model_by_instrument_agent(self, instrument_agent_id=''):
        return self.instrument_agent.find_stemming_model(instrument_agent_id)

    def find_instrument_agent_by_instrument_model(self, instrument_model_id=''):
        return self.instrument_agent.find_having_model(instrument_model_id)

    def find_instrument_device_by_instrument_agent_instance(self, instrument_agent_instance_id=''):
        return self.instrument_device.find_having_agent_instance(instrument_agent_instance_id)

    def find_instrument_agent_instance_by_instrument_device(self, instrument_device_id=''):
        return self.instrument_device.find_stemming_agent_instance(instrument_device_id)

    def find_instrument_device_by_platform_device(self, platform_device_id=''):
        return self.platform_device.find_stemming_device(platform_device_id)

    def find_platform_device_by_instrument_device(self, instrument_device_id=''):
        return self.platform_device.find_having_device(instrument_device_id)

    def find_instrument_device_by_logical_instrument(self, logical_instrument_id=''):
        return self.instrument_device.find_having_assignment(logical_instrument_id)

    def find_logical_instrument_by_instrument_device(self, instrument_device_id=''):
        return self.instrument_device.find_stemming_assignment(instrument_device_id)

    def find_platform_device_by_logical_platform(self, logical_platform_id=''):
        return self.platform_device.find_having_assignment(logical_platform_id)

    def find_logical_platform_by_platform_device(self, platform_device_id=''):
        return self.platform_device.find_stemming_assignment(platform_device_id)


    def find_data_product_by_instrument_device(self, instrument_device_id=''):
        return self.instrument_device.find_stemming_output_product(instrument_device_id)

    def find_instrument_device_by_data_product(self, data_product_id=''):
        return self.instrument_device.find_having_output_product(data_product_id)


    ############################
    #
    #  SPECIALIZED FIND METHODS
    #
    ############################

    def find_data_product_by_platform_device(self, platform_device_id=''):
        ret = []
        for i in self.find_instrument_device_by_platform_device(platform_device_id):
            data_products = self.find_data_product_by_instrument_device(i)
            for d in data_products:
                if not d in ret:
                    ret.append(d)

        return ret
        


    ############################
    #
    #  LIFECYCLE TRANSITIONS
    #
    ############################


    def execute_instrument_agent_lifecycle(self, instrument_agent_id="", lifecycle_event=""):
       """
       declare a instrument_agent to be in a given state
       @param instrument_agent_id the resource id
       """
       return self.instrument_agent.advance_lcs(instrument_agent_id, lifecycle_event)

    def execute_instrument_agent_instance_lifecycle(self, instrument_agent_instance_id="", lifecycle_event=""):
       """
       declare a instrument_agent_instance to be in a given state
       @param instrument_agent_instance_id the resource id
       """
       return self.instrument_agent_instance.advance_lcs(instrument_agent_instance_id, lifecycle_event)

    def execute_instrument_model_lifecycle(self, instrument_model_id="", lifecycle_event=""):
       """
       declare a instrument_model to be in a given state
       @param instrument_model_id the resource id
       """
       return self.instrument_model.advance_lcs(instrument_model_id, lifecycle_event)

    def execute_instrument_device_lifecycle(self, instrument_device_id="", lifecycle_event=""):
       """
       declare an instrument_device to be in a given state
       @param instrument_device_id the resource id
       """
       return self.instrument_device.advance_lcs(instrument_device_id, lifecycle_event)

    def execute_platform_agent_lifecycle(self, platform_agent_id="", lifecycle_event=""):
       """
       declare a platform_agent to be in a given state
       @param platform_agent_id the resource id
       """
       return self.platform_agent.advance_lcs(platform_agent_id, lifecycle_event)

    def execute_platform_agent_instance_lifecycle(self, platform_agent_instance_id="", lifecycle_event=""):
       """
       declare a platform_agent_instance to be in a given state
       @param platform_agent_instance_id the resource id
       """
       return self.platform_agent_instance.advance_lcs(platform_agent_instance_id, lifecycle_event)

    def execute_platform_model_lifecycle(self, platform_model_id="", lifecycle_event=""):
       """
       declare a platform_model to be in a given state
       @param platform_model_id the resource id
       """
       return self.platform_model.advance_lcs(platform_model_id, lifecycle_event)

    def execute_platform_device_lifecycle(self, platform_device_id="", lifecycle_event=""):
       """
       declare a platform_device to be in a given state
       @param platform_device_id the resource id
       """
       return self.platform_device.advance_lcs(platform_device_id, lifecycle_event)

    def execute_sensor_model_lifecycle(self, sensor_model_id="", lifecycle_event=""):
       """
       declare a sensor_model to be in a given state
       @param sensor_model_id the resource id
       """
       return self.sensor_model.advance_lcs(sensor_model_id, lifecycle_event)

    def execute_sensor_device_lifecycle(self, sensor_device_id="", lifecycle_event=""):
       """
       declare a sensor_device to be in a given state
       @param sensor_device_id the resource id
       """
       return self.sensor_device.advance_lcs(sensor_device_id, lifecycle_event)

