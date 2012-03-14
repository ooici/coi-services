#!/usr/bin/env python


__author__ = 'Stephen P. Henrie'
__license__ = 'Apache 2.0'

import inspect, collections, ast, simplejson, json, sys, time
from flask import Flask, request, abort
from gevent.wsgi import WSGIServer

from pyon.public import IonObject, Container, ProcessRPCClient
from pyon.core.exception import NotFound, Inconsistent, BadRequest, Unauthorized
from pyon.core.registry import get_message_class_in_parm_type, getextends, is_ion_object

from interface.services.coi.iservice_gateway_service import BaseServiceGatewayService
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceProcessClient
from interface.services.coi.iidentity_management_service import IdentityManagementServiceProcessClient
from interface.services.coi.iorg_management_service import OrgManagementServiceProcessClient
from pyon.util.log import log
from pyon.util.lru_cache import LRUCache

from pyon.agent.agent import ResourceAgentClient
from interface.services.iresource_agent import ResourceAgentProcessClient

#Initialize the flask app
app = Flask(__name__)

#Retain a module level reference to the service class for use with Process RPC calls below
service_gateway_instance = None

DEFAULT_WEB_SERVER_HOSTNAME = ""
DEFAULT_WEB_SERVER_PORT = 5000
DEFAULT_USER_CACHE_SIZE = 2000

GATEWAY_RESPONSE = 'GatewayResponse'
GATEWAY_ERROR = 'GatewayError'
GATEWAY_ERROR_EXCEPTION = 'Exception'
GATEWAY_ERROR_MESSAGE = 'Message'

DEFAULT_ACTOR_ID = 'anonymous'
DEFAULT_EXPIRY = 0

#Stuff for specifying other return types
RETURN_FORMAT_PARAM = 'return_format'
RETURN_FORMAT_RAW_JSON = 'raw_json'

#This class is used to manage the WSGI/Flask server as an ION process - and as a process endpoint for ION RPC calls
class ServiceGatewayService(BaseServiceGatewayService):

    """
	The Service Gateway Service is the service that uses a gevent web server and Flask to bridge HTTP requests to AMQP RPC ION process service calls.
	
    """

    def on_init(self):

        #defaults
        self.http_server = None
        self.server_hostname = DEFAULT_WEB_SERVER_HOSTNAME
        self.server_port = DEFAULT_WEB_SERVER_PORT
        self.web_server_enabled = True
        self.logging = None
        self.user_cache_size = DEFAULT_USER_CACHE_SIZE

        #retain a pointer to this object for use in ProcessRPC calls
        global service_gateway_instance
        service_gateway_instance = self

        try:
            self.web_server_cfg = self.CFG['container']['service_gateway']['web_server']
        except Exception, e:
            self.web_server_cfg = None

        if self.web_server_cfg is not None:
            if 'hostname' in self.web_server_cfg:
                self.server_hostname = self.web_server_cfg['hostname']
            if 'port' in self.web_server_cfg:
                self.server_port = self.web_server_cfg['port']
            if 'enabled' in self.web_server_cfg:
                self.web_server_enabled = self.web_server_cfg['enabled']
            if 'log' in self.web_server_cfg:
                self.logging = self.web_server_cfg['log']

        try:
            #Optional list of trusted originators can be specified in config.
            self.trusted_originators = self.CFG['container']['service_gateway']['trusted_originators']
            if len(self.trusted_originators) == 0:
                self.trusted_originators = None
        except Exception, e:
            self.trusted_originators = None

        if self.trusted_originators is None:
            log.info("Service Gateway will not check requests against trusted originators since none are configured.")

        try:
            #Get the user_cache_size
            self.user_cache_size = self.CFG['container']['service_gateway']['user_cache_size']
        except Exception, e:
            self.user_cache_size = DEFAULT_USER_CACHE_SIZE


        #Start the gevent web server unless disabled
        if self.web_server_enabled:
            self.start_service(self.server_hostname,self.server_port)

        #Initialize an LRU Cache to keep user roles cached for performance reasons
        #maxSize = maximum number of elements to keep in cache
        #maxAgeMs = oldest entry to keep
        self.user_data_cache = LRUCache(self.user_cache_size,0,0)

    def on_quit(self):
        self.stop_service()


    def start_service(self, hostname=DEFAULT_WEB_SERVER_HOSTNAME, port=DEFAULT_WEB_SERVER_PORT):
        """Responsible for starting the gevent based web server."""

        if self.http_server is not None:
            self.stop_service()

        self.http_server = WSGIServer((hostname, port), app, log=self.logging)
        self.http_server.start()

        return True

    def stop_service(self):
        """Responsible for stopping the gevent based web server."""
        if self.http_server is not None:
            self.http_server.stop()
        return True

    def is_trusted_address(self, requesting_address):

        if self.trusted_originators is None:
            return True

        for addr in self.trusted_originators:
            if requesting_address == addr:
                return True

        return False

@app.errorhandler(403)
def custom_403(error):
    return json_response({GATEWAY_ERROR: "The request has been denied since it did not originate from a trusted originator configured in the pyon.yaml file."})


#Checks to see if the remote_addr in the request is in the list of specified trusted addresses, if any.
@app.before_request
def is_trusted_request():

    if request.remote_addr is not None:
        log.debug("Request from: " + request.remote_addr)

    if not service_gateway_instance.is_trusted_address(request.remote_addr):
        abort(403)



# This is a service operation for handling generic service operation calls with arguments passed as query string parameters; like this:
# http://hostname:port/ion-service/resource_registry/find_resources?restype=BankAccount&id_only=False
# Shows how to handle POST requests with the parameters passed in posted JSON object, though has both since it is easier to use a browser URL GET than a POST by hand.

# An example post of json data using wget
# wget -o out.txt --post-data 'payload={"serviceRequest": { "serviceName": "resource_registry", "serviceOp": "find_resources",
# "params": { "restype": "BankAccount", "lcstate": "", "name": "", "id_only": true } } }' http://localhost:5000/ion-service/resource_registry/find_resources
#
# Probably should make this service smarter to respond to the mime type in the request data ( ie. json vs text )
#
@app.route('/ion-service/<service_name>/<operation>', methods=['GET','POST'])
def process_gateway_request(service_name, operation):


    try:

        if not service_name:
            raise BadRequest("Target service name not found in the URL")

        #Retrieve service definition
        from pyon.core.bootstrap import service_registry
        # MM: Note: service_registry can do more now
        target_service = service_registry.get_service_by_name(service_name)

        if not target_service:
            raise BadRequest("The requested service (%s) is not available" % service_name)

        if operation == '':
            raise BadRequest("Service operation not specified in the URL")


        #Find the concrete client class for making the RPC calls.
        if not target_service.client:
            raise Inconsistent("Cannot find a client class for the specified service: %s" % service_name )

        target_client = target_service.client

        #Retrieve json data from HTTP Post payload
        json_params = None
        if request.method == "POST":
            payload = request.form['payload']
            #debug only
            #payload = '{"serviceRequest": { "serviceName": "resource_registry", "serviceOp": "find_resources", "params": { "restype": "BankAccount", "lcstate": "", "name": "", "id_only": false } } }'
            json_params = json.loads(payload)

            if not json_params.has_key('serviceRequest'):
                raise Inconsistent("The JSON request is missing the 'serviceRequest' key in the request")

            if not json_params['serviceRequest'].has_key('serviceName'):
                raise Inconsistent("The JSON request is missing the 'serviceName' key in the request")

            if not json_params['serviceRequest'].has_key('serviceOp'):
                raise Inconsistent("The JSON request is missing the 'serviceOp' key in the request")

            if json_params['serviceRequest']['serviceName'] != target_service.name:
                raise Inconsistent("Target service name in the JSON request (%s) does not match service name in URL (%s)" % (str(json_params['serviceRequest']['serviceName']), target_service.name ) )

            if json_params['serviceRequest']['serviceOp'] != operation:
                raise Inconsistent("Target service operation in the JSON request (%s) does not match service name in URL (%s)" % ( str(json_params['serviceRequest']['serviceOp']), operation ) )

        param_list = create_parameter_list('serviceRequest', service_name, target_client,operation, json_params)


        #Validate requesting user and expiry and add governance headers
        ion_actor_id, expiry = get_governance_info_from_request('serviceRequest', json_params)
        ion_actor_id, expiry = validate_request(ion_actor_id, expiry)
        param_list['headers'] = build_message_headers(ion_actor_id, expiry)

        client = target_client(node=Container.instance.node, process=service_gateway_instance)
        methodToCall = getattr(client, operation)
        result = methodToCall(**param_list)


        #For service operations that add or remove user roles, remove the cached roles so that
        #the next request will get the latest set of user roles
        #TODO - this will only work while there is a single Service Gateway running - need to replace with Event
        #framework to evict from the cache when a user roles get updated.
        if operation == 'grant_role' or operation == 'revoke_role':
            #Look for a user_id in the set of parameters and remove it from the user role cache
            if param_list.has_key('user_id'):
                service_gateway_instance.user_data_cache.evict(param_list['user_id'])


        return gateway_json_response(result)


    except Exception, e:
        return build_error_response(e)


#This service method is used to communicate with a resource agent within the system from an
#external entity using HTTP requests. A resource_id of a running agent is required as is the operation
#that is being called and a JSON request block which specifies the data being sent to the agent.
#Example:
# curl -d 'payload={"agentRequest": { "agentId": "121ab46344cb3b30112", "agentOp": "execute",
# "params": { "command": ["AgentCommand",  {"command": "reset"}]  } }' http://localhost:5000/ion-agent/121ab46344cb3b30112/execute
@app.route('/ion-agent/<resource_id>/<operation>', methods=['POST'])
def process_gateway_agent_request(resource_id, operation):

    try:

        if not resource_id:
            raise BadRequest("Am agent resource_id was not found in the URL")

        if operation == '':
            raise BadRequest("An agent operation was not specified in the URL")

        #Retrieve json data from HTTP Post payload
        json_params = None
        if request.method == "POST":
            payload = request.form['payload']

            json_params = json.loads(payload)

            if not json_params.has_key('agentRequest'):
                raise Inconsistent("The JSON request is missing the 'agentRequest' key in the request")

            if not json_params['agentRequest'].has_key('agentId'):
                raise Inconsistent("The JSON request is missing the 'agentRequest' key in the request")

            if not json_params['agentRequest'].has_key('agentOp'):
                raise Inconsistent("The JSON request is missing the 'agentOp' key in the request")

            if json_params['agentRequest']['agentId'] != resource_id:
                raise Inconsistent("Target agent id in the JSON request (%s) does not match agent id in URL (%s)" % (str(json_params['agentRequest']['agentId']), resource_id) )

            if json_params['agentRequest']['agentOp'] != operation:
                raise Inconsistent("Target agent operation in the JSON request (%s) does not match agent operation in URL (%s)" % ( str(json_params['agentRequest']['agentOp']), operation ) )

        resource_agent = ResourceAgentClient(convert_unicode(resource_id), node=Container.instance.node, process=service_gateway_instance)
        if resource_agent is None:
            raise NotFound('The agent instance for id %s is not found.' % resource_id)


        param_list = create_parameter_list('agentRequest', 'resource_agent', ResourceAgentProcessClient, operation, json_params)

        #Validate requesting user and expiry and add governance headers
        ion_actor_id, expiry = get_governance_info_from_request('agentRequest', json_params)
        ion_actor_id, expiry = validate_request(ion_actor_id, expiry)
        param_list['headers'] = build_message_headers(ion_actor_id, expiry)

        methodToCall = getattr(resource_agent, operation)
        result = methodToCall(**param_list)

        return gateway_json_response(result)

    except Exception, e:
        return build_error_response(e)


#Private implementation of standard flask jsonify to specify the use of an encoder to walk ION objects
def json_response(response_data):

    return app.response_class(simplejson.dumps(response_data, default=ion_object_encoder,
        indent=None if request.is_xhr else 2), mimetype='application/json')

def gateway_json_response(response_data):


    if request.args.has_key(RETURN_FORMAT_PARAM):
        return_format = convert_unicode(request.args[RETURN_FORMAT_PARAM])
        if return_format == RETURN_FORMAT_RAW_JSON:
            return app.response_class(response_data, mimetype='application/json')

    return json_response({'data':{ GATEWAY_RESPONSE: response_data} } )

def build_error_response(e):

    exc_type, exc_obj, exc_tb = sys.exc_info()
    result = {
        GATEWAY_ERROR_EXCEPTION : exc_type.__name__,
        GATEWAY_ERROR_MESSAGE : str(e.message)
    }

    if request.args.has_key(RETURN_FORMAT_PARAM):
        return_format = convert_unicode(request.args[RETURN_FORMAT_PARAM])
        if return_format == RETURN_FORMAT_RAW_JSON:
            return app.response_class(result, mimetype='application/json')

    return json_response({'data': {GATEWAY_ERROR: result }} )

def get_governance_info_from_request(request_type = '', json_params = None):

    #Default values for governance headers.
    actor_id = DEFAULT_ACTOR_ID
    expiry = DEFAULT_EXPIRY
    if not json_params:

        if request.args.has_key('requester'):
            actor_id = convert_unicode(request.args['requester'])

        if request.args.has_key('expiry'):
            expiry = convert_unicode(request.args['expiry'])
    else:

        if json_params[request_type].has_key('requester'):
            actor_id = convert_unicode(json_params[request_type]['requester'])

        if json_params[request_type].has_key('expiry'):
            expiry = convert_unicode(json_params[request_type]['expiry'])

    return actor_id, expiry

def validate_request(ion_actor_id, expiry):

    idm_client = IdentityManagementServiceProcessClient(node=Container.instance.node, process=service_gateway_instance)

    try:
        user = idm_client.read_user_identity(user_id=ion_actor_id, headers={"ion-actor-id": service_gateway_instance.name, 'expiry': DEFAULT_EXPIRY })
    except NotFound, e:
        ion_actor_id = DEFAULT_ACTOR_ID  # If the user isn't found default to anonymous
        expiry = DEFAULT_EXPIRY  #Since this is now an anonymous request, there really is no expiry associated with it
        return ion_actor_id, expiry

    #need to convert to a float first in order to compare against current time.
    try:
        float_expiry = float(expiry)
    except Exception, e:
        raise Inconsistent("Unable to read the expiry value in the request '%s' as a floating point number" % expiry)

    #The user has been validated as being known in the system, so not check the expiry and raise exception if
    # the expiry is not set to 0 and less than the current time.
    if float_expiry > 0 and float_expiry < time.time():
        raise Unauthorized('The certificate associated with the user and expiry time in the request has expired.')

    return ion_actor_id, expiry

def build_message_headers( ion_actor_id, expiry):

    headers = dict()


    headers['ion-actor-id'] = ion_actor_id
    headers['expiry'] = expiry

    #If this is an anonymous requester then there are no roles associated with the request
    if ion_actor_id == DEFAULT_ACTOR_ID:
        headers['ion-actor-roles'] = dict()
        return headers

    try:
        #Check to see if the user's roles are cached already - keyed by user id
        #TODO - May need to synchronize this if there are "threading" issues
#        if service_gateway_instance.user_data_cache.has_key(ion_actor_id):
#            role_header = service_gateway_instance.user_data_cache.get(ion_actor_id)
#            if role_header is not None:
#                headers['ion-actor-roles'] = role_header
#                return headers


        #The user's roles were not cached so hit the datastore to find it.
        org_client = OrgManagementServiceProcessClient(node=Container.instance.node, process=service_gateway_instance)
        org_roles = org_client.find_all_roles_by_user(ion_actor_id, headers={"ion-actor-id": service_gateway_instance.name, 'expiry': DEFAULT_EXPIRY })

        role_header = get_role_message_headers(org_roles)

        #Cache the roles by user id
        service_gateway_instance.user_data_cache.put(ion_actor_id, role_header)

    except Exception, e:
        role_header = dict()  # Default to empty dict if there is a problem finding roles for the user

    headers['ion-actor-roles'] = role_header

    return headers

#Iterate the Org(s) that the user belongs to and create a header that lists only the role names per Org assigned
#to the user; i.e. {'ION': ['Member', 'Operator'], 'Org2': ['Member']}
def get_role_message_headers(org_roles):

    role_header = dict()
    for org in org_roles:
        role_header[org] = []
        for role in org_roles[org]:
            role_header[org].append(role.name)
    return role_header

#Build parameter list dynamically from
def create_parameter_list(request_type, service_name, target_client,operation, json_params):
    param_list = {}
    method_args = inspect.getargspec(getattr(target_client,operation))
    for arg in method_args[0]:
        if arg == 'self' or arg == 'headers': continue # skip self and headers from being set

        if not json_params:
            if request.args.has_key(arg):
                param_type = get_message_class_in_parm_type(service_name, operation, arg)
                if param_type == 'str':
                    param_list[arg] = convert_unicode(request.args[arg])
                else:
                    param_list[arg] = ast.literal_eval(convert_unicode(request.args[arg]))
        else:
            if json_params[request_type]['params'].has_key(arg):

                #This if handles ION objects as a 2 element list: [Object Type, { field1: val1, ...}]
                if isinstance(json_params[request_type]['params'][arg], list):

                    #TODO - Potentially remove these conversions whenever ION objects support unicode
                    # UNICODE strings are not supported with ION objects
                    ion_object_name = convert_unicode(json_params[request_type]['params'][arg][0])
                    object_params = convert_unicode(json_params[request_type]['params'][arg][1])

                    if is_ion_object(ion_object_name):
                        param_list[arg] = create_ion_object(ion_object_name, object_params)
                    else:
                        #Not an ION object so handle as a simple type then.
                        param_list[arg] = convert_unicode(json_params[request_type]['params'][arg])

                else:  # The else branch is for simple types ( non-ION objects )
                    param_list[arg] = convert_unicode(json_params[request_type]['params'][arg])

    return param_list

#Helper function for creating and initializing an ION object from a dictionary of parameters.
def create_ion_object(ion_object_name, object_params):

    new_obj = IonObject(ion_object_name)

    #Iterate over the parameters to add to object; have to do this instead
    #of passing a dict to get around restrictions in object creation on setting _id, _rev params
    for param in object_params:
        set_object_field(new_obj, param, object_params.get(param))

    new_obj._validate() # verify that all of the object fields were set with proper types
    return new_obj


#Use this function internally to recursively set sub object field values
def set_object_field(obj, field, field_val):
    if isinstance(field_val,dict):
        sub_obj = getattr(obj,field)
        for sub_field in field_val:
            set_object_field(sub_obj, sub_field, field_val.get(sub_field))
    else:
        setattr(obj, field, field_val)

#Used by json encoder
def ion_object_encoder(obj):
    return obj.__dict__

#Used to recursively convert unicode in JSON structures into proper data structures
def convert_unicode(data):
    if isinstance(data, unicode):
        return str(data)
    elif isinstance(data, collections.Mapping):
        return dict(map(convert_unicode, data.iteritems()))
    elif isinstance(data, collections.Iterable):
        return type(data)(map(convert_unicode, data))
    else:
        return data




# This service method returns the list of registered resource objects sorted alphabetically. Optional query
# string parameter will filter by extended type  i.e. type=InformationResource. All registered objects
# will be returned if not filtered
#
# Examples:
# http://hostname:port/ion-service/list_resource_types
# http://hostname:port/ion-service/list_resource_types?type=InformationResource
# http://hostname:port/ion-service/list_resource_types?type=TaskableResource
#
@app.route('/ion-service/list_resource_types', methods=['GET','POST'])
def list_resource_types():


    try:


        #Validate requesting user and expiry and add governance headers
        ion_actor_id, expiry = get_governance_info_from_request()
        ion_actor_id, expiry = validate_request(ion_actor_id, expiry)

        #Look to see if a specific resource type has been specified - if not default to all
        if request.args.has_key('type'):
            resultSet = set(getextends(request.args['type'])) if getextends(request.args['type']) is not None else set()
        else:
            type_list = getextends('Resource')
            resultSet = set(type_list)

        ret_list = []
        for res in sorted(resultSet):
            ret_list.append(res)


        return gateway_json_response(ret_list)


    except Exception, e:
        return build_error_response(e)



#Returns a json object for a specified resource type with all default values.
@app.route('/ion-service/resource_type_schema/<resource_type>')
def get_resource_schema(resource_type):


    try:

        #Validate requesting user and expiry and add governance headers
        ion_actor_id, expiry = get_governance_info_from_request()
        ion_actor_id, expiry = validate_request(ion_actor_id, expiry)

        #ION Objects are not registered as UNICODE names
        ion_object_name = convert_unicode(resource_type)
        ret_obj = IonObject(ion_object_name, {})

        # If it's an op input param or response message object.
        # Walk param list instantiating any params that were marked None as default.
        if hasattr(ret_obj, "_svc_name"):
            schema = ret_obj._schema
            for field in ret_obj._schema:
                if schema[field]["default"] is None:
                    try:
                        value = IonObject(schema[field]["type"], {})
                    except NotFound:
                        # TODO
                        # Some other non-IonObject type.  Just use None as default for now.
                        value = None
                    setattr(ret_obj, field, value)


        return gateway_json_response(ret_obj)

    except Exception, e:
        return build_error_response(e)



#More RESTfull examples...should probably not use but here for example reference

#This example calls the resource registry with an id passed in as part of the URL
#http://hostname:port/ion-service/resource/c1b6fa6aadbd4eb696a9407a39adbdc8
@app.route('/ion-service/rest/resource/<resource_id>')
def get_resource(resource_id):

        try:

            client = ResourceRegistryServiceProcessClient(node=Container.instance.node, process=service_gateway_instance)

            #Validate requesting user and expiry and add governance headers
            ion_actor_id, expiry = get_governance_info_from_request()
            ion_actor_id, expiry = validate_request(ion_actor_id, expiry)

            #Database object IDs are not unicode
            result = client.read(convert_unicode(resource_id))
            if not result:
                raise NotFound("No resource found for id: %s " % resource_id)

            return gateway_json_response(result)

        except Exception, e:
            return build_error_response(e)




#Example operation to return a list of resources of a specific type like
#http://hostname:port/ion-service/find_resources/BankAccount
@app.route('/ion-service/rest/find_resources/<resource_type>')
def list_resources_by_type(resource_type):

    try:

        client = ResourceRegistryServiceProcessClient(node=Container.instance.node, process=service_gateway_instance)

        #Validate requesting user and expiry and add governance headers
        ion_actor_id, expiry = get_governance_info_from_request()
        ion_actor_id, expiry = validate_request(ion_actor_id, expiry)

        #Resource Types are not in unicode
        res_list,_ = client.find_resources(restype=convert_unicode(resource_type) )

        return gateway_json_response(res_list)

    except Exception, e:
        return build_error_response(e)


#Gateway specific services are below

# Get image for a specific data product
#@app.route('/ion-viz-products/image/<data_product_id>/<img_name>', methods=['GET','POST'])
#def get_viz_image(data_product_id, img_name):

#    # Create client to interface with the viz service
#    vs_cli = VisualizationServiceProcessClient(node=Container.instance.node, process=service_gateway_instance)
#    return app.response_class(vs_cli.get_image(data_product_id, img_name),mimetype='image/png')



#Below are example restful calls to stuff for testing... all should be removed at some point

#http://hostname:port/ion-service/run_bank_client
@app.route('/ion-service/run_bank_client')
def create_accounts():
    from examples.bank.bank_client import run_client
    run_client(Container.instance, process=service_gateway_instance)
    return json_response("")


@app.route('/ion-service/seed_gov')
def seed_gov():
    from examples.gov_client import seed_gov
    seed_gov(Container.instance)
    return json_response("")


@app.route('/ion-service/test_policy')
def test_policy():
    from examples.gov_client import test_policy
    test_policy(Container.instance)
    return json_response("")


@app.route('/ion-service/test_requests')
def test_requests():
    from examples.gov_client import test_requests
    test_requests(Container.instance)
    return json_response("")




