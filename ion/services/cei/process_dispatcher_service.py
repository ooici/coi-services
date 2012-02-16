#!/usr/bin/env python

__author__ = 'Stephen P. Henrie, Michael Meisinger'
__license__ = 'Apache 2.0'

import uuid

from pyon.public import log, PRED
from pyon.core.exception import NotFound, BadRequest

from interface.services.cei.iprocess_dispatcher_service import BaseProcessDispatcherService


class ProcessDispatcherService(BaseProcessDispatcherService):

    # Implementation notes:
    #
    # Through Elaboration, the Process Dispatcher and other CEI services
    # do not run as pyon services. Instead they run as standalone processes
    # and communicate using simple AMQP messaging. However the Process
    # Dispatcher needs to be called by the Transform Management Service via
    # pyon messaging. To facilitate, this service acts as a bridge to the
    # "real" CEI process dispatcher.
    #
    # Because the real process dispatcher will only be present in a CEI
    # launch environment, this bridge service operates in two modes,
    # detected based on a config value.
    #
    # 1. When a "process_dispatcher_bridge" config section is present, this
    #    service acts as a bridge to the real PD. The real PD must be running
    #    and some additional dependencies must be available.
    #
    # 2. Otherwise, processes are started directly in the local container.
    #    This mode is meant to support the developer and integration use of
    #    r2deploy.yml and other single-container test deployments.
    #
    # Note that this is a relatively short-term situation. The PD will soon
    # natively run in the container and these tricks will be unnecessary.

    def on_init(self):

        #am I crazy or does self.CFG.get() not work?
        try:
            pd_conf = self.CFG.process_dispatcher_bridge
        except AttributeError:
            pd_conf = None

        if pd_conf:
            print pd_conf
            log.debug("Using Process Dispatcher Bridge backend -- requires running CEI services.")
            self.backend = PDBridgeBackend(pd_conf)
        else:
            log.debug("Using Process Dispatcher Local backend -- spawns processes in local container")

            self.backend = PDLocalBackend(self.container)

    def on_start(self):
        self.backend.initialize()

    def create_process_definition(self, process_definition=None):
        """Creates a Process Definition based on given object.

        @param process_definition    ProcessDefinition
        @retval process_definition_id    str
        @throws BadRequest    if object passed has _id or _rev attribute
        """
        pd_id, version = self.clients.resource_registry.create(process_definition)
        return pd_id

    def update_process_definition(self, process_definition=None):
        """Updates a Process Definition based on given object.

        @param process_definition    ProcessDefinition
        @throws BadRequest    if object does not have _id or _rev attribute
        @throws NotFound    object with specified id does not exist
        @throws Conflict    object not based on latest persisted object version
        """
        self.clients.resource_registry.update(process_definition)

    def read_process_definition(self, process_definition_id=''):
        """Returns a Process Definition as object.

        @param process_definition_id    str
        @retval process_definition    ProcessDefinition
        @throws NotFound    object with specified id does not exist
        """
        pdef = self.clients.resource_registry.read(process_definition_id)
        return pdef

    def delete_process_definition(self, process_definition_id=''):
        """Deletes/retires a Process Definition.

        @param process_definition_id    str
        @throws NotFound    object with specified id does not exist
        """
        self.clients.resource_registry.delete(process_definition_id)

    def find_process_definitions(self, filters=None):
        """Finds Process Definitions matching filter and returns a list of objects.

        @param filters    ResourceFilter
        @retval process_definition_list    []
        """
        pass

    def associate_execution_engine(self, process_definition_id='', execution_engine_definition_id=''):
        """Declare that the given process definition is compatible with the given execution engine.

        @param process_definition_id    str
        @param execution_engine_definition_id    str
        @throws NotFound    object with specified id does not exist
        """
        self.clients.resource_registry.create_association(process_definition_id,
                                                          PRED.supportsExecutionEngine,
                                                          execution_engine_definition_id)

    def dissociate_execution_engine(self, process_definition_id='', execution_engine_definition_id=''):
        """Remove the association of the process definition with an execution engine.

        @param process_definition_id    str
        @param execution_engine_definition_id    str
        @throws NotFound    object with specified id does not exist
        """
        assoc = self.clients.resource_registry.get_association(process_definition_id,
                                                          PRED.supportsExecutionEngine,
                                                          execution_engine_definition_id)
        self.clients.resource_registry.delete_association(assoc)

    def schedule_process(self, process_definition_id='', schedule=None, configuration={}):
        """Schedule a Process Definition for execution as process on an Execution Engine.

        @param process_definition_id    str
        @param schedule    ProcessSchedule
        @retval process_id    str
        @throws BadRequest    if object passed has _id or _rev attribute
        @throws NotFound    object with specified id does not exist
        """
        if not process_definition_id:
            raise NotFound('No process definition was provided')
        process_definition = self.clients.resource_registry.read(process_definition_id)

        # early validation before we pass definition through to backend
        try:
            module = process_definition.executable['module']
            cls = process_definition.executable['class']
        except KeyError,e:
            raise BadRequest("Process definition incomplete. missing: %s", e)

        if configuration is None:
            configuration = {}

        # try to get a unique but still descriptive name
        name = str(process_definition.name or "process") + uuid.uuid4().hex

        return self.backend.spawn(name, process_definition, schedule, configuration)

    def cancel_process(self, process_id=''):
        """Cancels the execution of the given process id.

        @param process_id    str
        @retval success    bool
        @throws NotFound    object with specified id does not exist
        """
        if not process_id:
            raise NotFound('No process was provided')

        return self.backend.cancel(process_id)


class PDLocalBackend(object):
    """Scheduling backend to PD that manages processes in the local container
    """

    def __init__(self, container):
        self.container = container

    def initialize(self):
        pass

    def spawn(self, name, definition, schedule, configuration):

        module = definition.executable['module']
        cls = definition.executable['class']

        # Spawn the process
        pid = self.container.spawn_process(name=name, module=module, cls=cls,
            config=configuration)
        log.debug('PD: Spawned Process (%s)', pid)

        return pid

    def cancel(self, process_id):
        self.container.proc_manager.terminate_process(process_id)
        log.debug('PD: Terminated Process (%s)', process_id)

        return True


class PDBridgeBackend(object):
    """Scheduling backend to PD that bridges to external CEI Process Dispatcher
    """

    def __init__(self, conf):
        self.dashi = None

        # grab config parameters used to connect to backend Process Dispatcher
        try:
            self.uri = conf.uri
            self.topic = conf.topic
            self.exchange = conf.exchange
        except AttributeError,e:
            log.warn("Needed Process Dispatcher config not found: %s", e)
            raise

    def initialize(self):
        self.dashi = self._init_dashi()

    def _init_dashi(self):
        # we are avoiding directly depending on dashi as this bridging approach
        # is short term and only used from CEI launches. And we have enough
        # deps. Where needed we install dashi specially via a separate
        # buildout config.

        try:
            import dashi
        except ImportError:
            log.warn("Attempted to use Process Dispatcher bridge mode but the "+
                     "dashi library dependency is not available.")
            raise
        return dashi.DashiConnection(self.topic, self.uri, self.exchange)

    def spawn(self, name, definition, schedule, configuration):

        module = definition.executable['module']
        cls = definition.executable['class']

        constraints = None
        if schedule:
            if schedule.target and schedule.target.constraints:
                constraints = schedule.target.constraints

        # form a pyon process spec
        # warning: this spec will change in the near future.

        config = configuration or {}
        app = dict(name=name, version="0,1", processapp=(name, module, cls),
            config=config)
        rel = dict(type="release", name=name, version="0.1", apps=[app])
        spec = dict(run_type="pyon_single", parameters=dict(rel=rel))

        proc = self.dashi.call(self.topic, "dispatch_process",
            upid=name, spec=spec, subscribers=None, constraints=constraints)

        log.debug("Dashi Process Dispatcher returned process: %s", proc)

        # name == upid == process_id
        return name

    def cancel(self, process_id):

        if not process_id:
            raise ValueError("invalid process id")

        proc = self.dashi.call(self.topic, "terminate_process", upid=process_id)
        log.debug("Dashi Process Dispatcher terminating process: %s", proc)
        return True
