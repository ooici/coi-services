#!/usr/bin/env python

"""Process that bootstraps an ION system"""


__author__ = 'Michael Meisinger'

from pyon.public import CFG, IonObject, log, get_sys_name, RT, LCS, PRED, iex
from pyon.ion.exchange import ION_ROOT_XS

from interface.services.ibootstrap_service import BaseBootstrapService
from ion.services.coi.policy_management_service import MANAGER_ROLE, ION_MANAGER
from ion.processes.bootstrap.load_system_policy import LoadSystemPolicy
from interface.objects import ProcessDefinition
from interface.objects import CouchStorage, HdfStorage

class BootstrapService(BaseBootstrapService):
    """
    Bootstrap service: This service will initialize the ION system environment.
    This service is triggered for each boot level.
    """

    process_type = "immediate"      # bootstrap inits/starts only, not a running process/service

    def on_init(self):
        log.info("Bootstrap service INIT: System init")
        self.system_actor_id = None

    def on_start(self):
        level = self.CFG.level
        log.info("Bootstrap service START: service start, level: %s", level)

        self.trigger_level(level, self.CFG)

    def trigger_level(self, level, config):
        #print "Bootstrap level: %s config: %s" % (str(level),str(config))


        ### COI Bootstrap levels
        if level == "datastore":
            self.post_datastore(config)
        elif level == "directory":
            self.post_directory(config)
        elif level == "resource_registry":
            self.post_resource_registry(config)
        elif level == "identity_management":
            self.post_identity_management(config)
        elif level == "policy_management":
            self.post_policy_management(config)
        elif level == "org_management":
            self.post_org_management(config)
        elif level == "exchange_management":
            self.post_exchange_management(config)
        elif level == "visualization_service":
            self.bootstrap_viz_svc(config)
        elif level == "load_system_policy":
            self.load_system_policy(config)

            self.post_startup()

            # Create ROOT user identity
            # Create default roles
            # Create default policy

        ### CEI bootstrap levels:

        elif level == "process_dispatcher":
            self.post_process_dispatcher(config)

        ### DM bootstrap levels:

        elif level == "ingestion_management":
            self.post_ingestion_management(config)

        elif level == "transform_management":
            self.post_transform_management(config)

        elif level == "data_retriever":
            self.post_data_retriever(config)



    def post_datastore(self, config):
        # Make sure to detect that system was already bootstrapped.
        # Look in datastore for secret cookie\

        cookie_name = get_sys_name() + ".ION_INIT"
        try:
            res = self.clients.datastore.read_doc(cookie_name)
            log.error("System %s already initialized: %s" % (get_sys_name(), res))
            return
        except iex.NotFound:
            pass

        # Now set the secret cookie
        import time
        cookie = dict(container=self.container.id, time=time.time())
        cid, _ = self.clients.datastore.create_doc(cookie, cookie_name)

    def post_directory(self, config):
        # Load service definitions into directory
        # Load resource types into directory
        # Load object types into directory
        pass

    def post_resource_registry(self, config):
        for res in RT.keys():
            rt = IonObject("ResourceType", name=res)
            #self.clients.datastore.create(rt)

    def post_policy_management(self, config):

        pass

    def post_identity_management(self, config):

        #Create the ION System Agent user which should be passed in subsequent bootstraping calls
        system_actor = CFG.system.system_actor
        user = IonObject(RT.ActorIdentity, name=system_actor, description="ION System Agent")
        self.clients.identity_management.create_actor_identity(user)

    def post_org_management(self, config):

        system_actor = self.clients.identity_management.find_actor_identity_by_name(name=CFG.system.system_actor)

        # Create root Org: ION
        root_orgname = CFG.system.root_org
        org = IonObject(RT.Org, name=root_orgname, description="ION Root Org")
        self.org_id = self.clients.org_management.create_org(org, headers={'ion-actor-id': system_actor._id})

        #Instantiate initial set of User Roles for this Org
        ion_manager = IonObject(RT.UserRole, name=ION_MANAGER,label='ION Manager', description='ION Manager')
        self.clients.org_management.add_user_role(self.org_id, ion_manager)
        self.clients.org_management.grant_role(self.org_id,system_actor._id,ION_MANAGER, headers={'ion-actor-id': system_actor._id} )

        #Make the ION system agent a manager for the ION Org
        self.clients.org_management.grant_role(self.org_id,system_actor._id,MANAGER_ROLE, headers={'ion-actor-id': system_actor._id} )


        #
        # Now load the base set of negotiation definitions used by the request operations (enroll/role/resource, etc)


        neg_def = IonObject(RT.NegotiationDefinition, name=RT.EnrollmentRequest,
            description='Definition of Enrollment Request Negotiation',
            pre_condition = ['is_registered(user_id)', 'is_not_enrolled(org_id,user_id)', 'enroll_req_not_exist(org_id,user_id)'],
            accept_action = 'enroll_member(org_id,user_id)'
        )

        self.clients.resource_registry.create(neg_def)

        neg_def = IonObject(RT.NegotiationDefinition, name=RT.RoleRequest,
            description='Definition of Role Request Negotiation',
            pre_condition = ['is_enrolled(org_id,user_id)'],
            accept_action = 'grant_role(org_id,user_id,role_name)'
        )

        self.clients.resource_registry.create(neg_def)

        neg_def = IonObject(RT.NegotiationDefinition, name=RT.ResourceRequest,
            description='Definition of Role Request Negotiation',
            pre_condition = ['is_enrolled(org_id,user_id)'],
            accept_action = 'acquire_resource(org_id,user_id,resource_id)'
        )

        self.clients.resource_registry.create(neg_def)




    #This operation must happen after the root ION Org has been created. This is triggered by adding an entry to the deploy file.
    def load_system_policy(self, config):

        LoadSystemPolicy.op_load_system_policies(self)


    def post_exchange_management(self, config):

        system_actor = self.clients.identity_management.find_actor_identity_by_name(name=CFG.system.system_actor)

        # find root org
        root_orgname = CFG.system.root_org      # @TODO: THIS CAN BE SPECIFIED ON A PER LAUNCH BASIS, HOW TO FIND?
        org = self.clients.org_management.find_org(name=root_orgname)

        # Create root ExchangeSpace
        xs = IonObject(RT.ExchangeSpace, name=ION_ROOT_XS, description="ION service XS")
        self.xs_id = self.clients.exchange_management.create_exchange_space(xs, org._id,headers={'ion-actor-id': system_actor._id})

        #self.clients.resource_registry.find_objects(self.org_id, "HAS-A")

        #self.clients.resource_registry.find_subjects(self.xs_id, "HAS-A")

    def bootstrap_viz_svc(self, config):

        # Create process definitions which will used to spawn off the transform processes
        matplotlib_proc_def = IonObject(RT.ProcessDefinition, name='viz_matplotlib_transform_process')
        matplotlib_proc_def.executable = {
            'module': 'ion.services.ans.visualization_service',
            'class':'VizTransformProcForMatplotlibGraphs'
        }
        matplotlib_proc_def_id, _ = self.clients.resource_registry.create(matplotlib_proc_def)

        google_dt_proc_def = IonObject(RT.ProcessDefinition, name='viz_google_dt_transform_process')
        google_dt_proc_def.executable = {
            'module': 'ion.services.ans.visualization_service',
            'class':'VizTransformProcForGoogleDT'
        }
        google_dt_proc_def_id, _ = self.clients.resource_registry.create(google_dt_proc_def)

        return

    def post_startup(self):
        log.info("Cannot sanity check bootstrap yet, need better plan to sync local state (or pull from datastore?)")

#        # Do some sanity tests across the board
#        org_ids, _ = self.clients.resource_registry.find_resources(RT.Org, None, None, True)
#        self.assert_condition(len(org_ids) == 1 and org_ids[0] == self.org_id, "Orgs not properly defined")
#
#        xs_ids, _ = self.clients.resource_registry.find_resources(RT.ExchangeSpace, None, None, True)
#        self.assert_condition(len(xs_ids) == 1 and xs_ids[0] == self.xs_id, "ExchangeSpace not properly defined")
#
#        res_ids, _ = self.clients.resource_registry.find_objects(self.org_id, PRED.hasExchangeSpace, RT.ExchangeSpace, True)
#        self.assert_condition(len(res_ids) == 1 and res_ids[0] == self.xs_id, "ExchangeSpace not associated")
#
#        res_ids, _ = self.clients.resource_registry.find_subjects(RT.Org, PRED.hasExchangeSpace, self.xs_id, True)
#        self.assert_condition(len(res_ids) == 1 and res_ids[0] == self.org_id, "Org not associated")

    def on_quit(self):
        log.info("Bootstrap service QUIT: System quit")


    def post_data_retriever(self, config):
        """
        Work is done in post_process_dispatcher... for now
        """
        pass



    def post_ingestion_management(self, config):
        """
        Defining the ingestion worker process is done in post_process_dispatcher.

        Creating transform workers happens here...
        """
        exchange_point = config.get_safe('ingestion.exchange_point','science_data')
        couch_opts = config.get_safe('ingestion.couch_storage',{})
        couch_storage = CouchStorage(**couch_opts)
        hdf_opts = config.get_safe('ingestion.hdf_storage',{})
        hdf_storage = HdfStorage(**hdf_opts)
        number_of_workers = config.get_safe('ingestion.number_of_workers',2)

        ingestion_id = self.clients.ingestion_management.create_ingestion_configuration(
            exchange_point_id=exchange_point,
            couch_storage=couch_storage,
            hdf_storage=hdf_storage,
            number_of_workers=number_of_workers
        )
        self.clients.ingestion_management.activate_ingestion_configuration(ingestion_id)

    def post_process_dispatcher(self, config):
        ingestion_module = config.get_safe('processes.ingestion.module','ion.processes.data.ingestion.ingestion_worker')
        ingestion_class  = config.get_safe('processes.ingestion.class' ,'IngestionWorker')

        replay_module    = config.get_safe('processes.replay.module', 'ion.processes.data.replay.replay_process')
        replay_class     = config.get_safe('processes.replay.class' , 'ReplayProcess')

        
        process_definition = ProcessDefinition(
            name='ingestion_worker_process',
            description='Worker transform process for ingestion of datasets')
        process_definition.executable['module']= ingestion_module
        process_definition.executable['class'] = ingestion_class
        self.clients.process_dispatcher.create_process_definition(process_definition=process_definition)


        self.process_definition = ProcessDefinition(
            name='data_replay_process',
            description='Process for the replay of datasets')
        self.process_definition.executable['module']= replay_module
        self.process_definition.executable['class'] = replay_class
        self.clients.process_dispatcher.create_process_definition(process_definition=self.process_definition)


    def post_transform_management(self,config):

        def restart_transform(transform_id):
            transform = self.clients.resource_registry.read(transform_id)
            configuration = transform.configuration
            proc_def_ids,other = self.clients.resource_registry.find_objects(subject=transform_id,predicate=PRED.hasProcessDefinition,id_only=True)

            if len(proc_def_ids) < 1:
                log.warning('Transform did not have a correct process definition.')
                return

            pid = self.clients.process_dispatcher.schedule_process(
                process_definition_id=proc_def_ids[0],
                configuration=configuration
            )

            transform.process_id = pid
            self.clients.resource_registry.update(transform)


        restart_flag = config.get_safe('service.transform_management.restart', False)
        if restart_flag:
            transform_ids, meta = self.clients.resource_registry.find_resources(restype=RT.Transform, id_only=True)
            for transform_id in transform_ids:
                restart_transform(transform_id)



