#!/usr/bin/env python

"""
@package  ion.services.sa.instrument.instrument_management_service
@author   Maurice Manning
@author   Ian Katz
"""


#from pyon.public import Container
from pyon.agent.agent import ResourceAgentClient
from pyon.public import LCE
from pyon.public import RT, PRED, OT
from pyon.public import CFG
from pyon.core.bootstrap import IonObject
from pyon.core.exception import Inconsistent,BadRequest, NotFound
#from pyon.datastore.datastore import DataStore
#from pyon.net.endpoint import RPCClient
from pyon.ion.resource import ExtendedResourceContainer
from pyon.util.log import log
from pyon.util.ion_time import IonTime
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
import signal

from interface.objects import ProcessDefinition
from interface.objects import AttachmentType

from coverage_model.parameter import ParameterDictionary, ParameterContext

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
from ion.services.sa.product.data_product_impl import DataProductImpl
from ion.services.sa.instrument.data_producer_impl import DataProducerImpl

from ion.agents.port.logger_process import EthernetDeviceLogger
from ion.agents.port.port_agent_process import PortAgentProcess

from interface.services.sa.iinstrument_management_service import BaseInstrumentManagementService

from interface.objects import ComputedValueAvailability

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
        self.extended_resource_handler = ExtendedResourceContainer(self)

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

        if hasattr(self.clients, "data_retriever"):
            self.DRS = self.clients.data_retriever

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

        #validate inputs
        self.read_instrument_agent(instrument_agent_id)
        self.read_instrument_device(instrument_device_id)

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

        self.instrument_agent_instance._unlink_all_subjects_by_association_type(PRED.hasAgentInstance,
                                                                                instrument_agent_instance_id)

        self.instrument_agent_instance.advance_lcs(instrument_agent_instance_id, LCE.RETIRE)
        #self.instrument_agent_instance.delete_one(instrument_agent_instance_id)

        return

    # TODO: TEMP!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    def _create_parameter(self, name):

        pdict = ParameterDictionary()

        pdict = self._add_location_time_ctxt(pdict)

        if name == 'ctd':
            cond_ctxt = ParameterContext('conductivity', param_type=QuantityType(value_encoding=np.float32))
            cond_ctxt.uom = 'unknown'
            cond_ctxt.fill_value = 0e0
            pdict.add_context(cond_ctxt)

            pres_ctxt = ParameterContext('pressure', param_type=QuantityType(value_encoding=np.float32))
            pres_ctxt.uom = 'Pascal'
            pres_ctxt.fill_value = 0x0
            pdict.add_context(pres_ctxt)

            temp_ctxt = ParameterContext('temp', param_type=QuantityType(value_encoding=np.float32))
            temp_ctxt.uom = 'degree_Celsius'
            temp_ctxt.fill_value = 0e0
            pdict.add_context(temp_ctxt)

        elif name == "raw":
            raw_ctxt = ParameterContext('raw', param_type=QuantityType(value_encoding=np.int64))
            raw_ctxt.reference_frame = 'unknown'
            raw_ctxt.uom = 'bytes'
            raw_ctxt.fill_value = 0e0
            pdict.add_context(raw_ctxt)

        return pdict

    # TODO: TEMP!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    def _add_location_time_ctxt(self, pdict):

        t_ctxt = ParameterContext('time', param_type=QuantityType(value_encoding=np.int64))
        t_ctxt.reference_frame = AxisTypeEnum.TIME
        t_ctxt.uom = 'seconds since 1970-01-01'
        t_ctxt.fill_value = 0x0
        pdict.add_context(t_ctxt)

        lat_ctxt = ParameterContext('lat', param_type=QuantityType(value_encoding=np.float32))
        lat_ctxt.reference_frame = AxisTypeEnum.LAT
        lat_ctxt.uom = 'degree_north'
        lat_ctxt.fill_value = 0e0
        pdict.add_context(lat_ctxt)

        lon_ctxt = ParameterContext('lon', param_type=QuantityType(value_encoding=np.float32))
        lon_ctxt.reference_frame = AxisTypeEnum.LON
        lon_ctxt.uom = 'degree_east'
        lon_ctxt.fill_value = 0e0
        pdict.add_context(lon_ctxt)

        depth_ctxt = ParameterContext('depth', param_type=QuantityType(value_encoding=np.float32))
        depth_ctxt.reference_frame = AxisTypeEnum.HEIGHT
        depth_ctxt.uom = 'meters'
        depth_ctxt.fill_value = 0e0
        pdict.add_context(depth_ctxt)

        return pdict



    def start_instrument_agent_instance(self, instrument_agent_instance_id=''):
        """
        Agent instance must first be created and associated with a instrument device
        Launch the instument agent instance and return the id
        """

        instrument_agent_instance_obj = self.clients.resource_registry.read(instrument_agent_instance_id)

        log.debug("activate_instrument: initial agent_config %s ", str(instrument_agent_instance_obj))

        #if there is a agent pid then assume that a drive is already started
        if instrument_agent_instance_obj.agent_process_id:
            raise BadRequest("Instrument Agent Instance already running for this device pid: %s" %
                             str(instrument_agent_instance_obj.agent_process_id))

        #retrieve the associated instrument device
        inst_device_objs = self.instrument_device.find_having_agent_instance(instrument_agent_instance_id)
        if 1 != len(inst_device_objs):
            raise BadRequest("Expected 1 InstrumentDevice attached to  InstrumentAgentInstance '%s', got %d" %
                             (str(instrument_agent_instance_id), len(inst_device_objs)))
        instrument_device_id = inst_device_objs[0]._id
        log.debug("start_instrument_agent_instance: device is %s connected to instrument agent instance %s (L4-CI-SA-RQ-363)", str(instrument_device_id),  str(instrument_agent_instance_id))

        #retrieve the instrument model
        model_objs = self.instrument_device.find_stemming_model(instrument_device_id)
        if 1 != len(model_objs):
            raise BadRequest("Expected 1 InstrumentDevice attached to  InstrumentAgentInstance '%s', got %d" %
                             (str(instrument_device_id), len(model_objs)))
        instrument_model_id = model_objs[0]
        log.debug("activate_instrument:instrument_model %s" % str(instrument_model_id))

        #retrive the stream info for this model
        streams_dict = model_objs[0].custom_attributes['streams']
        if not streams_dict:
            raise BadRequest("Device model does not contain stream configuation used in launching the agent. Model: '%s", str(model_objs[0]) )

        #retrieve the associated instrument agent
        agent_objs = self.instrument_agent.find_having_model(instrument_model_id)
        if 1 != len(agent_objs):
            raise BadRequest("Expected 1 InstrumentAgent attached to InstrumentModel '%s', got %d" %
                           (str(instrument_model_id), len(agent_objs)))
        instrument_agent_id = agent_objs[0]._id
        log.debug("Got instrument agent '%s'" % instrument_agent_id)


        #retrieve the associated process definition
        #todo: this association is not in the diagram... is it ok?
        process_def_ids, _ = self.clients.resource_registry.find_objects(instrument_agent_id,
                                                                         PRED.hasProcessDefinition,
                                                                         RT.ProcessDefinition,
                                                                         True)
        if 1 != len(process_def_ids):
            raise BadRequest("Expected 1 ProcessDefinition attached to InstrumentAgent '%s', got %d" %
                           (str(instrument_agent_id), len(process_def_ids)))


        process_definition_id = process_def_ids[0]
        log.debug("activate_instrument: agent process definition %s"  +  str(process_definition_id))

        # retrieve the process definition information
        process_def_obj = self.clients.resource_registry.read(process_definition_id)
        if not process_def_obj:
            raise NotFound("ProcessDefinition %s does not exist" % process_definition_id)

        out_streams = {}
        out_streams_and_param_dicts = {}
        #retrieve the output products
        data_product_ids, _ = self.clients.resource_registry.find_objects(instrument_device_id,
                                                                          PRED.hasOutputProduct,
                                                                          RT.DataProduct,
                                                                          True)
        if not data_product_ids:
            raise NotFound("No output Data Products attached to this Instrument Device " + str(instrument_device_id))

        for product_id in data_product_ids:
            stream_ids, _ = self.clients.resource_registry.find_objects(product_id, PRED.hasStream, RT.Stream, True)

            log.debug("activate_instrument:output stream ids: %s"  +  str(stream_ids))
            #One stream per product ...for now.
            if not stream_ids:
                raise NotFound("No Stream  attached to this Data Product " + str(product_id))
            if len(stream_ids) > 1:
                raise Inconsistent("Data Product should only have ONE Stream" + str(product_id))

            #get the  parameter dictionary for this stream
            dataset_ids, _ = self.clients.resource_registry.find_objects(product_id, PRED.hasDataset, RT.Dataset, True)
            #One data set per product ...for now.
            if not dataset_ids:
                raise NotFound("No Dataset attached to this Data Product " + str(product_id))
            if len(dataset_ids) > 1:
                raise Inconsistent("Data Product should only have ONE Dataset" + str(product_id))

            dataset_param_dict_flat = self.clients.dataset_management.get_dataset_parameters(dataset_ids[0])

            out_streams_and_param_dicts[stream_ids[0]] = dataset_param_dict_flat

            # retrieve the stream
            stream_obj = self.clients.resource_registry.read(stream_ids[0])
            if not stream_obj:
                raise NotFound("Stream %s does not exist" % stream_ids[0])

            #todo  - Replace this hack: look in the data product name for 'raw' or 'parsed'

            if stream_obj.name.lower().find('parsed') > -1 :
                out_streams['parsed'] = stream_ids[0]
                log.debug("activate_instrument:parsed %s ", str(stream_ids[0]) )
            elif stream_obj.name.lower().find('raw') > -1:
                out_streams['raw'] = stream_ids[0]
                log.debug("activate_instrument:raw %s ", str(stream_ids[0]) )
            else:
                raise NotFound("Stream %s is not CTD raw or parsed" % stream_obj.name)
        log.debug("activate_instrument:output stream config: %s"  +  str(out_streams))

        # todo: use parameter dictionary repository
        # Create parameter dictionaries for both parsed and raw
        ctd_param_dict = self._create_parameter('ctd')
        raw_param_dict = self._create_parameter('raw')


        #loop thru the defined streams for this device model and construct the stream config object
        stream_config_too = {}
        for stream_tag in streams_dict.iterkeys():
            log.debug("Model stream config: stream tag:   %s param dict name: %s", str(stream_tag), str(streams_dict[stream_tag]) )

            model_param_dict = ParameterDictionary.load_from_tag(out_streams_and_param_dicts[streams_dict[stream_tag]])

            # inflate the param dict and compare it against the param dicts that are attached to the data products for this device
            param_dict_for_model = ParameterDictionary.load(streams_dict[stream_tag])
            for product_stream_id in out_streams_and_param_dicts.iterkeys():
                product_param_dict = ParameterDictionary.load(out_streams_and_param_dicts[product_stream_id])
                if product_param_dict == model_param_dict:
                    #get the streamroute object from pubsub by passing the stream_id
                    stream_route = self.clients.pubsub_management.get_stream_route_for_stream(stream_id=product_stream_id, exchange_point='science_data')
                    stream_config_too[stream_tag] = {'stream_route': stream_route, 'parameter_dictionary':out_streams_and_param_dicts[product_stream_id]}

            if len(streams_dict) != len(stream_config_too):
                raise Inconsistent("Stream configuration for agent is not valid: " + str(stream_config_too))

        stream_info = {
            'parsed' : { out_streams['parsed']:ctd_param_dict.dump() },
            'raw' : { out_streams['raw']:raw_param_dict.dump()}
        }

        self._start_pagent(instrument_agent_instance_id)
        instrument_agent_instance_obj = self.read_instrument_agent_instance(instrument_agent_instance_id)

        # Create driver config.
        instrument_agent_instance_obj.driver_config = {
            'dvr_mod' : instrument_agent_instance_obj.driver_module,
            'dvr_cls' : instrument_agent_instance_obj.driver_class,
            'workdir' : '/tmp/',
            'process_type' : ('ZMQPyClassDriverLauncher',),
            'comms_config' : instrument_agent_instance_obj.driver_config['comms_config'],
            'pagent_pid' : instrument_agent_instance_obj.driver_config['pagent_pid']
        }

        # Create agent config.
        instrument_agent_instance_obj.agent_config = {
            'driver_config' : instrument_agent_instance_obj.driver_config,
            'stream_config' : stream_info,
            'agent'         : {'resource_id': instrument_device_id}
        }

        log.debug("activate_instrument: agent_config %s ", str(instrument_agent_instance_obj.agent_config))

        process_id, process_definition, schedule, configuration = self.clients.process_dispatcher.schedule_process(process_definition_id=process_definition_id,
                                                               schedule=None,
                                                               configuration=instrument_agent_instance_obj.agent_config)
        log.debug("activate_instrument: schedule_process %s", process_id)

        #update the producer context for provenance
        #todo: should get the time from process dispatcher
        producer_obj = self._get_instrument_producer(instrument_device_id)
        if producer_obj.producer_context.type_ == OT.InstrumentProducerContext :
            producer_obj.producer_context.activation_time =  IonTime().to_string()
            producer_obj.producer_context.execution_configuration = configuration
            self.clients.resource_registry.update(producer_obj)

        # add the process id and update the resource
        instrument_agent_instance_obj.agent_process_id = process_id
        self.update_instrument_agent_instance(instrument_agent_instance_obj)

        return


    def _start_pagent(self, instrument_agent_instance_id=None):
        """
        Construct and start the port agent.
        """
        instrument_agent_instance_obj = self.read_instrument_agent_instance(instrument_agent_instance_id)

        config = { 'device_addr' : instrument_agent_instance_obj.comms_device_address,
                   'device_port' : instrument_agent_instance_obj.comms_device_port,
                   'working_dir' : instrument_agent_instance_obj.port_agent_work_dir,
                   'delimiter' : instrument_agent_instance_obj.port_agent_delimeter  }
        self._pagent = PortAgentProcess.launch_process(config, timeout = 60, test_mode = True)
        pid = self._pagent.get_pid()
        port = self._pagent.get_data_port()
        log.debug("IMS: port agent pid: %d ", pid)

        # Configure driver to use port agent port number.
        instrument_agent_instance_obj.driver_config['comms_config'] = {
            'addr' : 'localhost', #TODO: should this be FQDN?
            'port' : port
        }
        instrument_agent_instance_obj.driver_config['pagent_pid'] = pid
        self.update_instrument_agent_instance(instrument_agent_instance_obj)


    def stop_instrument_agent_instance(self, instrument_agent_instance_id=''):
        """
        Deactivate the instrument agent instance
        """
        instrument_agent_instance_obj = self.clients.resource_registry.read(instrument_agent_instance_id)

        instrument_device_ids, _ = self.clients.resource_registry.find_subjects(subject_type=RT.InstrumentDevice, predicate=PRED.hasAgentInstance,
                                                                          object=instrument_agent_instance_id, id_only=True)
        if not instrument_device_ids:
            raise NotFound("No Instrument Device resource associated with this Instrument Agent Instance: %s", str(instrument_agent_instance_id) )

        # Cancels the execution of the given process id.
        self.clients.process_dispatcher.cancel_process(instrument_agent_instance_obj.agent_process_id)

        port_agent_pid = instrument_agent_instance_obj.driver_config['pagent_pid']


        #Stop the port agent.
        log.debug("IMS:stop_instrument_agent_instance stop pagent  %s ", str(port_agent_pid))
        if port_agent_pid:
            os.kill(port_agent_pid, signal.SIGTERM)

        #reset the process ids.
        instrument_agent_instance_obj.agent_process_id = None
        instrument_agent_instance_obj.driver_config['pagent_pid'] = None
        self.clients.resource_registry.update(instrument_agent_instance_obj)

        #update the producer context for provenance
        producer_obj = self._get_instrument_producer(instrument_device_ids[0])
        if producer_obj.producer_context.type_ == OT.InstrumentProducerContext :
            producer_obj.producer_context.deactivation_time =  IonTime().to_string()
            self.clients.resource_registry.update(producer_obj)

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
        self.clients.resource_registry.create_association(instrument_agent_id,  PRED.hasProcessDefinition, process_definition_id)

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

        self.instrument_agent.advance_lcs(instrument_agent_id, LCE.RETIRE)
        #return self.instrument_agent.delete_one(instrument_agent_id)


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
        self.instrument_agent.read_one(instrument_agent_id)


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

        cfg_host        = CFG.service.instrument_management.driver_release_host      #'amoeaba.ucsd.edu'
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
            self.clients.resource_registry.create_attachment(instrument_agent_id, att)

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
        self.instrument_model.advance_lcs(instrument_model_id, LCE.RETIRE)
        #return self.instrument_model.delete_one(instrument_model_id)





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
        self.instrument_device.advance_lcs(instrument_device_id, LCE.RETIRE)
        #return self.instrument_device.delete_one(instrument_device_id)



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



    def _get_instrument_producer(self, instrument_device_id=""):
        producer_objs, _ = self.clients.resource_registry.find_objects(subject=instrument_device_id, predicate=PRED.hasDataProducer, object_type=RT.DataProducer, id_only=False)
        if not producer_objs:
            raise NotFound("No Producers created for this Data Process " + str(data_process_id))
        return producer_objs[0]


    ##########################################################################
    #
    # PLATFORM AGENT INSTANCE
    #
    ##########################################################################

    def create_platform_agent_instance(self, platform_agent_instance=None, platform_agent_id="", platform_device_id=""):
        """
        create a new instance
        @param platform_agent_instance the object to be created as a resource
        @retval platform_agent_instance_id the id of the new object
        @throws BadRequest if the incoming _id field is set
        @throws BadReqeust if the incoming name already exists
        """
        #validate inputs
        self.read_platform_agent(platform_agent_id)
        self.read_platform_device(platform_device_id)

        platform_agent_instance_id = self.instrument_agent_instance.create_one(platform_agent_instance)

        self.assign_platform_agent_to_platform_agent_instance(platform_agent_id, platform_agent_instance_id)

        self.assign_platform_agent_instance_to_platform_device(platform_agent_instance_id, platform_device_id)
        log.debug("create_platform_agent_instance: device %s now connected to platform agent instance %s ", str(platform_device_id),  str(platform_agent_instance_id))

        return platform_agent_instance_id

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
        self.platform_agent.advance_lcs(platform_agent_instance_id, LCE.RETIRE)
        #return self.platform_agent_instance.delete_one(platform_agent_instance_id)





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

        platform_agent_id = self.platform_agent.create_one(platform_agent)

        # Create the process definition to launch the agent
        process_definition = ProcessDefinition()
        process_definition.executable['module']='ion.agents.instrument.platform_agent'
        process_definition.executable['class'] = 'PLatformAgent'
        process_definition_id = self.clients.process_dispatcher.create_process_definition(process_definition=process_definition)
        log.debug("create_platform_agent: create_process_definition id %s"  +  str(process_definition_id))

        #associate the agent and the process def
        self.clients.resource_registry.create_association(platform_agent_id,  PRED.hasProcessDefinition, process_definition_id)

        return platform_agent_id


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
        self.platform_agent.advance_lcs(platform_agent_id, LCE.RETIRE)
        #return self.platform_agent.delete_one(platform_agent_id)


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
        self.platform_model.advance_lcs(platform_model_id, LCE.RETIRE)
        #return self.platform_model.delete_one(platform_model_id)



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
        self.platform_device.advance_lcs(platform_device_id, LCE.RETIRE)
        #return self.platform_device.delete_one(platform_device_id)





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
        self.sensor_model.advance_lcs(sensor_model_id, LCE.RETIRE)
        #return self.sensor_model.delete_one(sensor_model_id)



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
        self.sensor_device.advance_lcs(sensor_device_id, LCE.RETIRE)
        #return self.sensor_device.delete_one(sensor_device_id)



    ##########################################################################
    #
    # ASSOCIATIONS
    #
    ##########################################################################


    def assign_instrument_model_to_instrument_device(self, instrument_model_id='', instrument_device_id=''):
        instrument_model_obj  = self.instrument_model.read_one(instrument_model_id)
        instrument_device_obj = self.instrument_device.read_one(instrument_device_id)

        for k, v in instrument_device_obj.custom_attributes.iteritems():
            if not k in instrument_model_obj.custom_attributes:
                err_msg = ("InstrumentDevice '%s' contains custom attribute '%s' (value '%s'), but this attribute"
                        + " is not defined by associated InstrumentModel '%s'") % (instrument_device_id,
                                                                                   k, v,
                                                                                   instrument_model_id)
                #raise BadRequest(err_msg)
                log.warn(err_msg)

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
        self.platform_device.link_instrument_device(platform_device_id, instrument_device_id)

    def unassign_instrument_device_from_platform_device(self, instrument_device_id='', platform_device_id=''):
        self.platform_device.unlink_instrument_device(platform_device_id, instrument_device_id)

    def assign_platform_device_to_platform_device(self, child_platform_device_id='', platform_device_id=''):
        self.platform_device.link_platform_device(platform_device_id, child_platform_device_id)

    def unassign_platform_device_from_platform_device(self, child_platform_device_id='', platform_device_id=''):
        self.platform_device.unlink_platform_device(platform_device_id, child_platform_device_id)

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
        self.platform_device.link_agent_instance(platform_device_id, platform_agent_instance_id)

    def unassign_platform_agent_instance_from_platform_device(self, platform_agent_instance_id='', platform_device_id=''):
        self.platform_device.unlink_agent_instance(platform_device_id, platform_agent_instance_id)

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
        return self.platform_device.find_stemming_instrument_device(platform_device_id)

    def find_platform_device_by_instrument_device(self, instrument_device_id=''):
        return self.platform_device.find_having_instrument_device(instrument_device_id)

    def find_child_platform_device_by_platform_device(self, platform_device_id=''):
        return self.platform_device.find_stemming_platform_device(platform_device_id)

    def find_platform_device_by_child_platform_device(self, instrument_device_id=''):
        return self.platform_device.find_having_platform_device(instrument_device_id)

    def find_instrument_device_by_logical_instrument(self, logical_instrument_id=''):
        raise NotImplementedError("TODO: this function will be removed")

    def find_logical_instrument_by_instrument_device(self, instrument_device_id=''):
        raise NotImplementedError("TODO: this function will be removed")

    def find_platform_device_by_logical_platform(self, logical_platform_id=''):
        raise NotImplementedError("TODO: this function will be removed")

    def find_logical_platform_by_platform_device(self, platform_device_id=''):
        raise NotImplementedError("TODO: this function will be removed")

    def find_data_product_by_instrument_device(self, instrument_device_id=''):
        raise NotImplementedError("TODO: this function will be removed")

    def find_instrument_device_by_data_product(self, data_product_id=''):
        raise NotImplementedError("TODO: this function will be removed")


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


    ############################
    #
    #  STREAM RETRIEVAL
    #
    ############################

    def retrieve_latest_device_event(self, device_id):
        #todo: is there a constant for "events"?
        datastore = self.container.datastore_manager.get_datastore("events")

        view_name = 'event/by_type'

        key_name = device_id #todo: not sure what this needs to be for event/event_type

        opts = dict(
            start_key = [key_name, 0],
            end_key   = [key_name, {}],
            descending = True,
            limit = 1,
            include_docs = True
        )

        granules = []

        log.info('Getting data from datastore')
        for result in datastore.query_view(view_name, opts=opts):
            doc = result.get('doc')
            if doc is not None:
                ion_obj = self.granule_from_doc(doc)
                granules.append(ion_obj)
        log.info('Received %d granules.', len(granules))

        #todo: handle this better
        return granules[0]


    def retrieve_latest_data_granule(self, device_id):
        #todo: how to get dataset?
        #todo: wait for DM refactor before proceeding

        dataset_id = "fixme, how to get this"

#        # TESTING
#        stream_id  = self.PSMS.create_stream()
#        config_id  = self.get_ingestion_config()
#        dataset_id = self.create_dataset()
#        self.ingestion_management.persist_data_stream(stream_id=stream_id, ingestion_configuration_id=config_id, dataset_id=dataset_id)
#        #--------------------------------------------------------------------------------
#        # Create the datastore first,
#        #--------------------------------------------------------------------------------
#        self.get_datastore(dataset_id)
#
#        self.publish_hifi(stream_id, 0)
#        self.publish_hifi(stream_id, 1)
#
#        self.wait_until_we_have_enough_granules(dataset_id, 2) # I just need two

        return self.DRS.retrieve_last_granule(dataset_id)



    ############################
    #
    #  EXTENDED RESOURCES
    #
    ############################


    def get_instrument_device_extension(self, instrument_device_id='', ext_associations=None, ext_exclude=None):
        """Returns an InstrumentDeviceExtension object containing additional related information

        @param instrument_device_id    str
        @param ext_associations    dict
        @param ext_exclude    list
        @retval instrument_device    InstrumentDeviceExtension
        @throws BadRequest    A parameter is missing
        @throws NotFound    An object with the specified instrument_device_id does not exist
        """

        if not instrument_device_id:
            raise BadRequest("The instrument_device_id parameter is empty")

        extended_resource_handler = ExtendedResourceContainer(self)

        extended_instrument = extended_resource_handler.create_extended_resource_container(
            OT.InstrumentDeviceExtension,
            instrument_device_id,
            OT.InstrumentDeviceComputedAttributes,
            ext_associations,
            ext_exclude)

        #Loop through any attachments and remove the actual content since we don't need
        #   to send it to the front end this way
        #TODO - see if there is a better way to do this in the extended resource frame work.
        if hasattr(extended_instrument, 'attachments'):
            for att in extended_instrument.attachments:
                if hasattr(att, 'content'):
                    delattr(att, 'content')

        return extended_instrument


    # TODO: this causes a problem because an instrument agent must be running in order to look up extended attributes.
    def obtain_agent_handle(self, instrument_device_id):
        ia_client = ResourceAgentClient(instrument_device_id,  process=self)


#       #todo: any validation?
#        cmd = AgentCommand(command='get_current_state')
#        retval = self._ia_client.execute_agent(cmd)
#        state = retval.result
#        self.assertEqual(state, InstrumentAgentState.UNINITIALIZED)
#

        return ia_client

    def obtain_agent_calculation(self, instrument_device_id, result_container):
        ret = IonObject(result_container)
        a_client = None
        try:
            a_client = self.obtain_agent_handle(instrument_device_id)
            ret.status = ComputedValueAvailability.PROVIDED
        except NotFound:
            ret.status = ComputedValueAvailability.NOTAVAILABLE
            ret.reason = "Could not connect to instrument agent instance -- may not be running"
        except Exception as e:
            raise e

        return a_client, ret

    #functions for computed attributes -- currently bogus values returned

    def get_firmware_version(self, instrument_device_id):
        ia_client, ret = self.obtain_agent_calculation(instrument_device_id, OT.ComputedFloatValue)
        if ia_client:
            ret.value = 1.1 #todo: use ia_client
        return ret


    def get_location(self, instrument_device_id):
        ia_client, ret = self.obtain_agent_calculation(instrument_device_id, OT.ComputedGeospatialBoundsValue)
        if ia_client:
            ret.value = IonObject(OT.GeospatialBounds)  #todo: use ia_client
        return ret


    def get_last_data_received_time(self, instrument_device_id):
        ia_client, ret = self.obtain_agent_calculation(instrument_device_id, OT.ComputedFloatValue)
        if ia_client:
            ret.value = 42.2 #todo: use ia_client
        return ret


    def get_operational_state(self, instrument_device_id):   # from Device
        ia_client, ret = self.obtain_agent_calculation(instrument_device_id, OT.ComputedStringValue)
        if ia_client:
            ret.value = "23" #todo: use ia_client
        return ret

    def get_last_command_status(self, instrument_device_id):
        ia_client, ret = self.obtain_agent_calculation(instrument_device_id, OT.ComputedIntValue)
        if ia_client:
            ret.value = 34 #todo: use ia_client
        return ret

    def get_last_command_date(self, instrument_device_id):
        ia_client, ret = self.obtain_agent_calculation(instrument_device_id, OT.ComputedFloatValue)
        if ia_client:
            ret.value = 45.5 #todo: use ia_client
        return ret

    def get_last_command(self, instrument_device_id):
        ia_client, ret = self.obtain_agent_calculation(instrument_device_id, OT.ComputedStringValue)
        if ia_client:
            ret.value = "56" #todo: use ia_client
        return ret

    def get_last_commanded_by(self, instrument_device_id):
        ia_client, ret = self.obtain_agent_calculation(instrument_device_id, OT.ComputedStringValue)
        if ia_client:
            ret.value = "67" #todo: use ia_client
        return ret

    def get_power_status_roll_up(self, instrument_device_id): # CV: BLACK, RED, GREEN, YELLOW
        #todo: listen for events/streams from instrument agent -- there will be alarms

        ia_client, ret = self.obtain_agent_calculation(instrument_device_id, OT.ComputedIntValue)
        if ia_client:
            ret.value = 78 #todo: use ia_client
        return ret

    def get_communications_status_roll_up(self, instrument_device_id): # CV: BLACK, RED, GREEN, YELLOW
        #todo: following algorithm:
        # if telemetry agent exists:
        #     get comms schedule from telemetry agent (tbd)
        #     figure out when last transmission was expected
        #     see if any events/errors/data have come from the device at that time
        # else:
        #      ping device


        ia_client, ret = self.obtain_agent_calculation(instrument_device_id, OT.ComputedIntValue)
        if ia_client:
            ret.value = 89 #todo: use ia_client
        return ret

    def get_data_status_roll_up(self, instrument_device_id): # BLACK, RED, GREEN, YELLOW
        #todo: listen for events/streams from instrument agent -- there will be alarms

        ia_client, ret = self.obtain_agent_calculation(instrument_device_id, OT.ComputedIntValue)
        if ia_client:
            ret.value = 98 #todo: use ia_client
        return ret

    def get_location_status_roll_up(self, instrument_device_id): # CV: BLACK, RED, GREEN, YELLOW
        #todo: listen for events/streams from instrument agent -- there will be alarms

        ia_client, ret = self.obtain_agent_calculation(instrument_device_id, OT.ComputedIntValue)
        if ia_client:
            ret.value = 87 #todo: use ia_client
        return ret

    def get_recent_events(self, instrument_device_id):  #List of the 10 most recent events for this device
        ret = IonObject(OT.ComputedListValue)

        try:
            ret.status = ComputedValueAvailability.PROVIDED
            #todo: try to get the last however long of data to parse through
            ret.value = ["monday", "tuesday", "wednesday"]
        except NotFound:
            ret.status = ComputedValueAvailability.NOTAVAILABLE
            ret.reason = "Could not retrieve device stream -- may not be configured et"
        except Exception as e:
            raise e

        return ret


    def get_platform_device_extension(self, platform_device_id='', ext_associations=None, ext_exclude=None):
        """Returns an PlatformDeviceExtension object containing additional related information
        """

        if not platform_device_id:
            raise BadRequest("The platform_device_id parameter is empty")

        extended_resource_handler = ExtendedResourceContainer(self)

        extended_platform = extended_resource_handler.create_extended_resource_container(
            OT.PlatformDeviceExtension,
            platform_device_id,
            OT.PlatformDeviceComputedAttributes,
            ext_associations,
            ext_exclude)

        #Loop through any attachments and remove the actual content since we don't need to send it to the front end this way
        #TODO - see if there is a better way to do this in the extended resource frame work.
        if hasattr(extended_platform, 'attachments'):
            for att in extended_platform.attachments:
                if hasattr(att, 'content'):
                    delattr(att, 'content')

        return extended_platform


    # amount of energy being generated on the platform in Watts
    def get_power_energy_generation(self, platform_device_id):
        # amount of energy being generated on the platform in Watts
        return 1.1
    
    # amount of energy currently being consumed by the platform in Watts
    def get_energy_consumption(self, platform_device_id):
        # amount of energy currently being consumed by the platform in Watts
        return 1.1

    def get_data_transmission_rate(self, platform_device_id):
        #todo: units?
        return "data_transmission_rate"

    def get_speed_over_ground(self, platform_device_id):
        #todo: units?
        return "speed_over_ground"

    def get_aggregated_status(self, platform_device_id):
        # The status roll-up that summarizes the entire status of the device  (CV:  RED, YELLOW, GREEN, BLACK)
        #todo: class for constants?

        #todo: does framework validate id?

        #recursive function to determine the aggregate status by visiting all relevant nodes
        def get_status_helper(acc, device_id, device_type):
            if "todo: early exit criteria" == acc:
                return acc

            if RT.InstrumentDevice == device_type:
                stat_p = self.get_power_status_roll_up(device_id)
                stat_d = self.get_data_status_roll_up(device_id)
                stat_l = self.get_location_status_roll_up(device_id)
                stat_c = self.get_communications_status_roll_up(device_id)

                #todo: return acc based on instrument status?

            elif RT.PlatformDevice == device_type:
                #todo: how to get platform status?
                #stat_p = self.get_power_status_roll_up(device_id)
                #stat_d = self.get_data_status_roll_up(device_id)
                #stat_l = self.get_location_status_roll_up(device_id)
                #stat_c = self.get_communications_status_roll_up(device_id)

                #todo: return acc based on platform status?

                instrument_resources = self.platform_device.find_stemming_instrument_device(device_id)
                for instrument_resource in instrument_resources:
                    acc = get_status_helper(acc, instrument_resource._id, type(instrument_resource).__name__)

                platform_resources = self.platform_device.find_stemming_platform_device(device_id)
                for platform_resource in platform_resources:
                    acc = get_status_helper(acc, platform_resource._id, type(platform_resource).__name__)

                return acc
            else:
                raise NotImplementedError("Completely avoidable error, got bad device_type: %s" % device_type)

        retval = get_status_helper(None, platform_device_id, RT.PlatformDevice)
        retval = "RED" #todo: remove this line

        return retval




