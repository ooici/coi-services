#!/usr/bin/env python

__author__ = 'Stephen P. Henrie'
__license__ = 'Apache 2.0'

import inspect, ast, sys, traceback, string
import json, simplejson
from flask import Flask, request, abort
from werkzeug import secure_filename
from gevent.wsgi import WSGIServer
import os
import time

from pyon.public import IonObject, Container, OT
from pyon.core.object import IonObjectBase
from pyon.core.exception import NotFound, Inconsistent, BadRequest, Unauthorized
from pyon.core.registry import getextends, is_ion_object_dict, issubtype
from pyon.core.governance import DEFAULT_ACTOR_ID, get_role_message_headers, find_roles_by_actor
from pyon.core.governance.negotiation import Negotiation
from pyon.event.event import EventSubscriber, EventPublisher
from pyon.ion.resource import get_object_schema
from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceClient
from interface.services.coi.idirectory_service import DirectoryServiceProcessClient
from interface.services.coi.iservice_gateway_service import BaseServiceGatewayService
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceProcessClient
from interface.services.coi.iidentity_management_service import IdentityManagementServiceProcessClient
from interface.services.coi.iorg_management_service import OrgManagementServiceProcessClient
from interface.services.ans.ivisualization_service import VisualizationServiceProcessClient
from interface.services.sa.idata_product_management_service import DataProductManagementServiceClient
from pyon.util.log import log
from pyon.util.lru_cache import LRUCache
from pyon.util.containers import current_time_millis, DotDict
from pyon.util.file_sys import FileSystem, FS

from pyon.agent.agent import ResourceAgentClient
from interface.services.iresource_agent import ResourceAgentProcessClient
from interface.objects import Attachment, ProcessDefinition
from interface.objects import ProposalStatusEnum, ProposalOriginatorEnum

#Initialize the flask app
service_gateway_app = Flask(__name__)

#Retain a module level reference to the service class for use with Process RPC calls below
service_gateway_instance = None

DEFAULT_WEB_SERVER_HOSTNAME = ""
DEFAULT_WEB_SERVER_PORT = 5000
DEFAULT_USER_CACHE_SIZE = 2000

GATEWAY_RESPONSE = 'GatewayResponse'
GATEWAY_ERROR = 'GatewayError'
GATEWAY_ERROR_EXCEPTION = 'Exception'
GATEWAY_ERROR_MESSAGE = 'Message'
GATEWAY_ERROR_TRACE = 'Trace'

OMS_ACCEPTED_RESPONSE = '202 Accepted'
OMS_BAD_REQUEST_RESPONSE = '400 Bad Request'


DEFAULT_EXPIRY = '0'

# Stuff for specifying other return types
RETURN_MIMETYPE_PARAM = 'return_mimetype'

# Set standard json functions
json_dumps = json.dumps
json_loads = simplejson.loads


class ServiceGatewayService(BaseServiceGatewayService):
    """
	The Service Gateway Service is the service that uses a gevent web server and Flask
	to bridge HTTP requests to AMQP RPC ION process service calls.
    """

    def on_init(self):
        #defaults
        self.http_server = None

        #retain a pointer to this object for use in ProcessRPC calls
        global service_gateway_instance

        ######
        # to prevent cascading failure, here's an attempted hack
        if service_gateway_instance is not None and service_gateway_instance.http_server is not None:
            service_gateway_instance.http_server.stop()
        # end hack
        ######

        service_gateway_instance = self

        self.server_hostname = self.CFG.get_safe('container.service_gateway.web_server.hostname', DEFAULT_WEB_SERVER_HOSTNAME)
        self.server_port = self.CFG.get_safe('container.service_gateway.web_server.port', DEFAULT_WEB_SERVER_PORT)
        self.web_server_enabled = self.CFG.get_safe('container.service_gateway.web_server.enabled', True)
        self.web_logging = self.CFG.get_safe('container.service_gateway.web_server.log')
        self.log_errors = self.CFG.get_safe('container.service_gateway.log_errors', True)

        #Optional list of trusted originators can be specified in config.
        self.trusted_originators = self.CFG.get_safe('container.service_gateway.trusted_originators')
        if not self.trusted_originators:
            self.trusted_originators = None
            log.info("Service Gateway will not check requests against trusted originators since none are configured.")

        #Get the user_cache_size
        self.user_cache_size = self.CFG.get_safe('container.service_gateway.user_cache_size', DEFAULT_USER_CACHE_SIZE)

        #Initialize an LRU Cache to keep user roles cached for performance reasons
        #maxSize = maximum number of elements to keep in cache
        #maxAgeMs = oldest entry to keep
        self.user_role_cache = LRUCache(self.user_cache_size,0,0)

        #Start the gevent web server unless disabled
        if self.web_server_enabled:
            log.info("Starting service gateway on %s:%s", self.server_hostname, self.server_port)
            self.start_service(self.server_hostname, self.server_port)

        #Configure  subscriptions for user_cache events
        self.user_role_event_subscriber = EventSubscriber(event_type=OT.UserRoleModifiedEvent, origin_type="Org",
            callback=self.user_role_event_callback)
        self.add_endpoint(self.user_role_event_subscriber)

        self.user_role_reset_subscriber = EventSubscriber(event_type=OT.UserRoleCacheResetEvent,
            callback=self.user_role_reset_callback)
        self.add_endpoint(self.user_role_reset_subscriber)

    def on_quit(self):
        self.stop_service()

    def start_service(self, hostname=DEFAULT_WEB_SERVER_HOSTNAME, port=DEFAULT_WEB_SERVER_PORT):
        """Responsible for starting the gevent based web server."""

        if self.http_server is not None:
            self.stop_service()

        self.http_server = WSGIServer((hostname, port), service_gateway_app, log=self.web_logging)
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

    def user_role_event_callback(self, *args, **kwargs):
        """
        This method is a callback function for receiving Events when User Roles are modified.
        """
        user_role_event = args[0]
        org_id = user_role_event.origin
        actor_id = user_role_event.actor_id
        role_name = user_role_event.role_name
        log.debug("User Role modified: %s %s %s" % (org_id, actor_id, role_name))

        #Evict the user and their roles from the cache so that it gets updated with the next call.
        if service_gateway_instance.user_role_cache and service_gateway_instance.user_role_cache.has_key(actor_id):
            log.debug('Evicting user from the user_role_cache: %s' % actor_id)
            service_gateway_instance.user_role_cache.evict(actor_id)

    def user_role_reset_callback(self, *args, **kwargs):
        '''
        This method is a callback function for when an event is received to clear the user data cache
        '''
        self.user_role_cache.clear()

@service_gateway_app.errorhandler(403)
def custom_403(error):
    return json_response({GATEWAY_ERROR: "The request has been denied since it did not originate from a trusted originator."})


#Checks to see if the remote_addr in the request is in the list of specified trusted addresses, if any.
@service_gateway_app.before_request
def is_trusted_request():

    if request.remote_addr is not None:
        log.debug("%s from: %s: %s", request.method, request.remote_addr, request.url)

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
@service_gateway_app.route('/ion-service/<service_name>/<operation>', methods=['GET','POST'])
def process_gateway_request(service_name, operation):


    try:

        if not service_name:
            raise BadRequest("Target service name not found in the URL")

        #Ensure there is no unicode
        service_name = str(service_name)
        operation = str(operation)

        #Retrieve service definition
        from pyon.core.bootstrap import get_service_registry
        # MM: Note: service_registry can do more now
        target_service = get_service_registry().get_service_by_name(service_name)

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
            json_params = json_loads(str(payload))

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

        return gateway_json_response(result)


    except Exception, e:
        return build_error_response(e)


#This service method is used to communicate with a resource agent within the system from an
#external entity using HTTP requests. A resource_id of a running agent is required as is the operation
#that is being called and a JSON request block which specifies the data being sent to the agent.
#Example:
# curl -d 'payload={"agentRequest": { "agentId": "121ab46344cb3b30112", "agentOp": "execute",
# "params": { "command": ["AgentCommand",  {"command": "reset"}]  } }' http://localhost:5000/ion-agent/121ab46344cb3b30112/execute
@service_gateway_app.route('/ion-agent/<resource_id>/<operation>', methods=['POST'])
def process_gateway_agent_request(resource_id, operation):

    try:

        if not resource_id:
            raise BadRequest("Am agent resource_id was not found in the URL")

        if operation == '':
            raise BadRequest("An agent operation was not specified in the URL")


        #Ensure there is no unicode
        resource_id = str(resource_id)
        operation = str(operation)

        #Retrieve json data from HTTP Post payload
        json_params = None
        if request.method == "POST":
            payload = request.form['payload']

            json_params = json_loads(str(payload))

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

        resource_agent = ResourceAgentClient(resource_id, node=Container.instance.node, process=service_gateway_instance)

        param_list = create_parameter_list('agentRequest', 'resource_agent', ResourceAgentProcessClient, operation, json_params)

        #Validate requesting user and expiry and add governance headers
        ion_actor_id, expiry = get_governance_info_from_request('agentRequest', json_params)
        ion_actor_id, expiry = validate_request(ion_actor_id, expiry)
        param_list['headers'] = build_message_headers(ion_actor_id, expiry)

        methodToCall = getattr(resource_agent, operation)
        result = methodToCall(**param_list)

        return gateway_json_response(result)

    except Exception, e:
        if e is NotFound:
            log.warning('The agent instance for id %s is not found.' % resource_id)
        return build_error_response(e)

#This service method is used to provide the RSN OMS with a means of sending events to the CI system using HTTP requests.

#test this method with curl:
# curl -H "Content-type: application/json" --data "[{\"summary\": \"fake event triggered from CI using OMS generate_test_event\",
#     \"severity\": 3, \"class\": \"/Test\", \"platform_id\":\"test_platform_123\" , \"timestamp\":\"timestamp\" , \"message\":
#     \"fake event triggered from CI using OMS generate_test_event\", \"test_event\": True}]" http://localhost:5000/ion-service/oms_event

@service_gateway_app.route('/ion-service/oms_event', methods=['GET','POST'])
def process_oms_event():
    if not request.data:
        log.warning('process_oms_event: invalid OMS event payload: %r', request.data)
        return gateway_json_response(OMS_BAD_REQUEST_RESPONSE)

    payload = json_loads(str(request.data))
    if not isinstance(payload, list):
        log.warning('process_oms_event: invalid OMS event payload: '
                    'expecting array but got: %r', payload)
        return gateway_json_response(OMS_BAD_REQUEST_RESPONSE)

    log.debug('process_oms_event: payload=%s', payload)

    event_publisher = EventPublisher()

    for obj in payload:
        for k in ['event_id', 'platform_id', 'message']:
            if k not in obj:
                log.warning('process_oms_event: invalid OMS event: %r missing. '
                            'Received object: %s', k, obj)
                #return gateway_json_response(OMS_BAD_REQUEST_RESPONSE)

        # note the the external event_id is captured in the sub_type field:
        evt = dict(
            event_type     = 'OMSDeviceStatusEvent',
            origin_type    = 'OMS Platform',
            origin         = obj.get('platform_id', 'platform_id NOT PROVIDED'),
            sub_type       = obj.get('event_id', 'event_id NOT PROVIDED'),
            description    = obj.get('message', ''),
            status_details = obj)
        try:
            event_publisher.publish_event(**evt)
            log.debug('process_oms_event: published: %s', evt)

        except Exception as e:
            log.exception('process_oms_event: could not publish OMS event: %s', evt)

    return gateway_json_response(OMS_ACCEPTED_RESPONSE)


#Private implementation of standard flask jsonify to specify the use of an encoder to walk ION objects
def json_response(response_data):

    return service_gateway_app.response_class(json_dumps(response_data, default=ion_object_encoder,
        indent=None if request.is_xhr else 2), mimetype='application/json')

def gateway_json_response(response_data):

    if request.args.has_key(RETURN_MIMETYPE_PARAM):
        return_mimetype = str(request.args[RETURN_MIMETYPE_PARAM])
        return service_gateway_app.response_class(response_data, mimetype=return_mimetype)

    return json_response({'data':{ GATEWAY_RESPONSE: response_data} } )

def build_error_response(e):

    if hasattr(e,'get_stacks'):
        #Process potentially multiple stacks.
        full_error = ''
        for i in range(len(e.get_stacks())):
            full_error += e.get_stacks()[i][0] + "\n"
            if i == 0:
                full_error += string.join(traceback.format_exception(*sys.exc_info()), '')
            else:
                for ln in e.get_stacks()[i][1]:
                    full_error += str(ln)  + "\n"

        exec_name = e.__class__.__name__
    else:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        exec_name = exc_type.__name__
        full_error = traceback.format_exception(*sys.exc_info())

    if service_gateway_instance.log_errors:
        log.error(full_error)

    result = {
        GATEWAY_ERROR_EXCEPTION : exec_name,
        GATEWAY_ERROR_MESSAGE : str(e.message),
        GATEWAY_ERROR_TRACE : full_error
    }

    if request.args.has_key(RETURN_MIMETYPE_PARAM):
        return_mimetype = str(request.args[RETURN_MIMETYPE_PARAM])
        return service_gateway_app.response_class(result, mimetype=return_mimetype)

    return json_response({'data': {GATEWAY_ERROR: result }} )

def get_governance_info_from_request(request_type = '', json_params = None):

    #Default values for governance headers.
    actor_id = DEFAULT_ACTOR_ID
    expiry = DEFAULT_EXPIRY
    if not json_params:

        if request.args.has_key('requester'):
            actor_id = str(request.args['requester'])

        if request.args.has_key('expiry'):
            expiry = str(request.args['expiry'])
    else:

        if json_params[request_type].has_key('requester'):
            actor_id = json_params[request_type]['requester']

        if json_params[request_type].has_key('expiry'):
            expiry = json_params[request_type]['expiry']

    return actor_id, expiry

def validate_request(ion_actor_id, expiry):

    #There is no point in looking up an anonymous user - so return default values.
    if ion_actor_id == DEFAULT_ACTOR_ID:
        expiry = DEFAULT_EXPIRY  #Since this is now an anonymous request, there really is no expiry associated with it
        return ion_actor_id, expiry

    idm_client = IdentityManagementServiceProcessClient(node=Container.instance.node, process=service_gateway_instance)

    try:
        user = idm_client.read_actor_identity(actor_id=ion_actor_id, headers={"ion-actor-id": service_gateway_instance.name, 'expiry': DEFAULT_EXPIRY })
    except NotFound, e:
        ion_actor_id = DEFAULT_ACTOR_ID  # If the user isn't found default to anonymous
        expiry = DEFAULT_EXPIRY  #Since this is now an anonymous request, there really is no expiry associated with it
        return ion_actor_id, expiry

    #need to convert to a float first in order to compare against current time.
    try:
        int_expiry = int(expiry)
    except Exception, e:
        raise Inconsistent("Unable to read the expiry value in the request '%s' as an int" % expiry)

    #The user has been validated as being known in the system, so not check the expiry and raise exception if
    # the expiry is not set to 0 and less than the current time.
    if int_expiry > 0 and int_expiry < current_time_millis():
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
        if service_gateway_instance.user_role_cache.has_key(ion_actor_id):
            role_header = service_gateway_instance.user_role_cache.get(ion_actor_id)
            if role_header is not None:
                headers['ion-actor-roles'] = role_header
                return headers


        #The user's roles were not cached so hit the datastore to find it.
        org_client = OrgManagementServiceProcessClient(node=Container.instance.node, process=service_gateway_instance)
        org_roles = org_client.find_all_roles_by_user(ion_actor_id, headers={"ion-actor-id": service_gateway_instance.name, 'expiry': DEFAULT_EXPIRY })

        role_header = get_role_message_headers(org_roles)

        #Cache the roles by user id
        service_gateway_instance.user_role_cache.put(ion_actor_id, role_header)

    except Exception, e:
        role_header = dict()  # Default to empty dict if there is a problem finding roles for the user

    headers['ion-actor-roles'] = role_header

    return headers



#Build parameter list dynamically from
def create_parameter_list(request_type, service_name, target_client,operation, json_params):

    #This is a bit of a hack - should use decorators to indicate which parameter is the dict that acts like kwargs
    optional_args = request.args.to_dict(flat=True)

    param_list = {}
    method_args = inspect.getargspec(getattr(target_client,operation))
    for (arg_index, arg) in enumerate(method_args[0]):
        if arg == 'self': continue # skip self

        if not json_params:
            if request.args.has_key(arg):

                #Keep track of which query_string_parms are left after processing
                del optional_args[arg]

                #Handle strings differently because of unicode
                if isinstance(method_args[3][arg_index-1], str):
                    if isinstance(request.args[arg], unicode):
                        param_list[arg] = str(request.args[arg].encode('utf8'))
                    else:
                        param_list[arg] = str(request.args[arg])
                else:
                    param_list[arg] = ast.literal_eval(str(request.args[arg]))
        else:
            if json_params[request_type]['params'].has_key(arg):

                object_params = json_params[request_type]['params'][arg]
                if is_ion_object_dict(object_params):
                    param_list[arg] = create_ion_object(object_params)
                else:
                    #Not an ION object so handle as a simple type then.
                    if isinstance(json_params[request_type]['params'][arg], unicode):
                        param_list[arg] = str(json_params[request_type]['params'][arg].encode('utf8'))
                    else:
                        param_list[arg] = json_params[request_type]['params'][arg]

    #Send any optional_args if there are any and allowed
    if len(optional_args) > 0 and  'optional_args' in method_args[0]:
        param_list['optional_args'] = dict()
        for arg in optional_args:
            #Only support basic strings for these optional params for now
            param_list['optional_args'][arg] = str(request.args[arg])

    return param_list

#Helper function for creating and initializing an ION object from a dictionary of parameters.
def create_ion_object(object_params):
    new_obj = IonObject(object_params["type_"])

    #Iterate over the parameters to add to object; have to do this instead
    #of passing a dict to get around restrictions in object creation on setting _id, _rev params
    for param in object_params:
        set_object_field(new_obj, param, object_params.get(param))

    new_obj._validate() # verify that all of the object fields were set with proper types
    return new_obj


#Use this function internally to recursively set sub object field values
def set_object_field(obj, field, field_val):
    if isinstance(field_val, dict) and field != 'kwargs':
        sub_obj = getattr(obj, field)

        if isinstance(sub_obj, IonObjectBase):

            if field_val.has_key('type_') and field_val['type_'] != sub_obj.type_:

                if issubtype(field_val['type_'] ,sub_obj.type_):
                    sub_obj = IonObject(field_val['type_'])
                    setattr(obj, field, sub_obj)
                else:
                    raise Inconsistent("Unable to walk the field %s as an IonObject since the types don't match: %s %s" % (field, sub_obj.type_, field_val['type_']))

            for sub_field in field_val:
                set_object_field(sub_obj, sub_field, field_val.get(sub_field))

        elif isinstance(sub_obj, dict):
            setattr(obj, field, field_val)

        else:
            for sub_field in field_val:
                set_object_field(sub_obj, sub_field, field_val.get(sub_field))
    else:
        # type_ already exists in the class.
        if field != "type_":
            setattr(obj, field, field_val)

#Used by json encoder
def ion_object_encoder(obj):
    return obj.__dict__




# This service method returns the list of registered resource objects sorted alphabetically. Optional query
# string parameter will filter by extended type  i.e. type=InformationResource. All registered objects
# will be returned if not filtered
#
# Examples:
# http://hostname:port/ion-service/list_resource_types
# http://hostname:port/ion-service/list_resource_types?type=InformationResource
# http://hostname:port/ion-service/list_resource_types?type=TaskableResource
#

@service_gateway_app.route('/ion-service/org_roles/<actor_id>', methods=['GET','POST'])
def list_org_roles(actor_id):

    try:
        ret = find_roles_by_actor(str(actor_id))
        return gateway_json_response(ret)

    except Exception, e:
        return build_error_response(e)


@service_gateway_app.route('/ion-service/list_resource_types', methods=['GET','POST'])
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
@service_gateway_app.route('/ion-service/resource_type_schema/<resource_type>')
def get_resource_schema(resource_type):
    try:
        #Validate requesting user and expiry and add governance headers
        ion_actor_id, expiry = get_governance_info_from_request()
        ion_actor_id, expiry = validate_request(ion_actor_id, expiry)

        return gateway_json_response(get_object_schema(resource_type))

    except Exception, e:
        return build_error_response(e)



# Get attachment for a specific attachment id
@service_gateway_app.route('/ion-service/attachment/<attachment_id>', methods=['GET'])
def get_attachment(attachment_id):

    try:
        # Create client to interface with the viz service
        rr_client = ResourceRegistryServiceProcessClient(node=Container.instance.node, process=service_gateway_instance)
        attachment = rr_client.read_attachment(attachment_id, include_content=True)

        return service_gateway_app.response_class(attachment.content,mimetype=attachment.content_type)


    except Exception, e:
        return build_error_response(e)

@service_gateway_app.route('/ion-service/attachment', methods=['POST'])
def create_attachment():

    try:
        payload              = request.form['payload']
        json_params          = json_loads(str(payload))

        ion_actor_id, expiry = get_governance_info_from_request('serviceRequest', json_params)
        ion_actor_id, expiry = validate_request(ion_actor_id, expiry)
        headers              = build_message_headers(ion_actor_id, expiry)

        data_params          = json_params['serviceRequest']['params']
        resource_id          = str(data_params.get('resource_id', ''))
        fil                  = request.files['file']
        content              = fil.read()

        keywords             = []
        keywords_str         = data_params.get('keywords', '')
        if keywords_str.strip():
            keywords = [str(x.strip()) for x in keywords_str.split(',')]

        created_by           = data_params.get('attachment_created_by', 'unknown user')
        modified_by          = data_params.get('attachment_modified_by', 'unknown user')

        # build attachment
        attachment           = Attachment(name=str(data_params['attachment_name']),
                                          description=str(data_params['attachment_description']),
                                          attachment_type=int(data_params['attachment_type']),
                                          content_type=str(data_params['attachment_content_type']),
                                          keywords=keywords,
                                          created_by=created_by,
                                          modified_by=modified_by,
                                          content=content)

        rr_client = ResourceRegistryServiceProcessClient(node=Container.instance.node, process=service_gateway_instance)
        ret = rr_client.create_attachment(resource_id=resource_id, attachment=attachment, headers=headers)

        return gateway_json_response(ret)

    except Exception, e:
        log.exception("Error creating attachment")
        return build_error_response(e)

@service_gateway_app.route('/ion-service/attachment/<attachment_id>', methods=['DELETE'])
def delete_attachment(attachment_id):
    try:
        rr_client = ResourceRegistryServiceProcessClient(node=Container.instance.node, process=service_gateway_instance)
        ret = rr_client.delete_attachment(attachment_id)
        return gateway_json_response(ret)

    except Exception, e:
        log.exception("Error deleting attachment")
        return build_error_response(e)

# Get a visualization image for a specific data product
#TODO - will need to update this to handle parameters to pass on to the Vis service and to use proper return keys
@service_gateway_app.route('/ion-service/get_visualization_image/')
def get_visualization_image():

    # Create client to interface with the viz service
    vs_cli = VisualizationServiceProcessClient(node=Container.instance.node, process=service_gateway_instance)
    params = request.args

    data_product_id = params["data_product_id"]
    visualization_parameters = json_loads(params["visualization_parameters"])
    image_info = vs_cli.get_visualization_image(data_product_id, visualization_parameters)

    return service_gateway_app.response_class(image_info['image_obj'],mimetype=image_info['content_type'])

# Get a visualization image for the provenance of a specific data product paramter
@service_gateway_app.route('/ion-service/get_parameter_provenance_visualization_image', methods=['GET','POST'])
def get_parameter_provenance_visualization_image():

    payload = request.form['payload']
    json_params = json_loads(str(payload))
    #log.debug('get_parameter_provenance_visualization_image  json_params:  %s', json_params)

    # Create client to interface with the viz service
    data_product_cli = DataProductManagementServiceClient(node=Container.instance.node)   #, process=service_gateway_instance)

    if not json_params.has_key('serviceRequest') or not json_params['serviceRequest'].has_key('params'):
        raise  BadRequest("The JSON request is not properly formed")
    params =  json_params['serviceRequest']['params']
    if  not params.has_key('data_product_id') or not params.has_key('parameter_name'):
        raise  BadRequest("The JSON request is not properly formed")

    data_product_id = params["data_product_id"]
    parameter_name = params["parameter_name"]

    image_info = data_product_cli.get_data_product_parameter_provenance(data_product_id, parameter_name)

    return service_gateway_app.response_class(image_info,mimetype='image/png')


# Get version information about this copy of coi-services
@service_gateway_app.route('/ion-service/version')
def get_version_info():
    import pkg_resources

    pkg_list = ["coi-services",
                "pyon",
                "coverage-model",
                "ion-functions",
                "eeagent",
                "epu",
                "utilities",
                "marine-integrations"]

    version = {}

    for package in pkg_list:
        try:
            version["%s-release" % package] = pkg_resources.require(package)[0].version
            # @TODO git versions for each?
        except pkg_resources.DistributionNotFound:
            pass

    try:
        dir_client = DirectoryServiceProcessClient(process=service_gateway_instance)
        sys_attrs = dir_client.lookup("/System")
        if sys_attrs and isinstance(sys_attrs, dict):
            version.update({k: v for (k, v) in sys_attrs.iteritems() if "version" in k.lower()})

    except Exception as ex:
        log.exception("Could not determine system directory attributes")

    return gateway_json_response(version)


#More REST-ful examples...should probably not use but here for example reference

#This example calls the resource registry with an id passed in as part of the URL
#http://hostname:port/ion-service/resource/c1b6fa6aadbd4eb696a9407a39adbdc8
@service_gateway_app.route('/ion-resources/resource/<resource_id>')
def get_resource(resource_id):

    try:
        client = ResourceRegistryServiceProcessClient(node=Container.instance.node, process=service_gateway_instance)

        #Validate requesting user and expiry and add governance headers
        ion_actor_id, expiry = get_governance_info_from_request()
        ion_actor_id, expiry = validate_request(ion_actor_id, expiry)

        #Database object IDs are not unicode
        result = client.read(str(resource_id))
        if not result:
            raise NotFound("No resource found for id: %s " % resource_id)

        return gateway_json_response(result)

    except Exception, e:
        return build_error_response(e)


#Example operation to return a list of resources of a specific type like
#http://hostname:port/ion-service/find_resources/BankAccount
@service_gateway_app.route('/ion-resources/find_resources/<resource_type>')
def find_resources_by_type(resource_type):
    try:
        client = ResourceRegistryServiceProcessClient(node=Container.instance.node, process=service_gateway_instance)

        #Validate requesting user and expiry and add governance headers
        ion_actor_id, expiry = get_governance_info_from_request()
        ion_actor_id, expiry = validate_request(ion_actor_id, expiry)

        #Resource Types are not in unicode
        res_list,_ = client.find_resources(restype=str(resource_type) )

        return gateway_json_response(res_list)

    except Exception, e:
        return build_error_response(e)


#Accept/Reject negotiation
# special cased here because coi-services offers superior logic to what we can provide in the UI
@service_gateway_app.route('/ion-service/resolve-org-negotiation', methods=['POST'])
def resolve_org_negotiation():
    try:
        payload              = request.form['payload']
        json_params          = json_loads(str(payload))

        ion_actor_id, expiry = get_governance_info_from_request('serviceRequest', json_params)
        ion_actor_id, expiry = validate_request(ion_actor_id, expiry)
        headers              = build_message_headers(ion_actor_id, expiry)

        # extract negotiation-specific data (convert from unicode just in case - these are machine generated and unicode specific
        # chars are unexpected)
        verb                 = str(json_params['verb'])
        originator           = str(json_params['originator'])
        negotiation_id       = str(json_params['negotiation_id'])
        reason               = str(json_params.get('reason', ''))

        proposal_status = None
        if verb.lower() == "accept":
            proposal_status = ProposalStatusEnum.ACCEPTED
        elif verb.lower() == "reject":
            proposal_status = ProposalStatusEnum.REJECTED

        proposal_originator = None
        if originator.lower() == "consumer":
            proposal_originator = ProposalOriginatorEnum.CONSUMER
        elif originator.lower() == "provider":
            proposal_originator = ProposalOriginatorEnum.PROVIDER

        rr_client = ResourceRegistryServiceProcessClient(node=Container.instance.node, process=service_gateway_instance)
        negotiation = rr_client.read(negotiation_id, headers=headers)

        new_negotiation_sap = Negotiation.create_counter_proposal(negotiation, proposal_status, proposal_originator)

        org_client = OrgManagementServiceProcessClient(node=Container.instance.node, process=service_gateway_instance)
        resp = org_client.negotiate(new_negotiation_sap, headers=headers)

        # update reason if it exists
        if reason:
            # reload negotiation because it has changed
            negotiation = rr_client.read(negotiation_id, headers=headers)
            negotiation.reason = reason
            rr_client.update(negotiation)

        return gateway_json_response(resp)

    except Exception, e:
        return build_error_response(e)

@service_gateway_app.route('/ion-service/upload/data/<dataproduct_id>', methods=['POST'])
def upload_data(dataproduct_id):
    upload_folder = FileSystem.get_url(FS.TEMP,'uploads')
    try:

        rr_client = ResourceRegistryServiceProcessClient(node=Container.instance.node, process=service_gateway_instance)
        object_store = Container.instance.object_store

        try:
            rr_client.read(str(dataproduct_id))
        except BadRequest:
            raise BadRequest('Unknown DataProduct ID %s' % dataproduct_id)

        # required fields
        upload = request.files['file'] # <input type=file name="file">

        # determine filetype
        filetype = _check_magic(upload)
        upload.seek(0) # return to beginning for save

        if upload and filetype is not None:

            # upload file - run filename through werkzeug.secure_filename
            filename = secure_filename(upload.filename)
            path = os.path.join(upload_folder, filename)
            upload_time = time.time()
            upload.save(path)

            # register upload
            file_upload_context = {
                # TODO add dataproduct_id
                'name':'User uploaded file %s' % filename,
                'filename':filename,
                'filetype':filetype,
                'path':path,
                'upload_time':upload_time,
                'status':'File uploaded to server'
            }
            fuc_id, _ = object_store.create_doc(file_upload_context)

            # client to process dispatch
            pd_client = ProcessDispatcherServiceClient()

            # create process definition
            process_definition = ProcessDefinition(
                name='upload_data_processor',
                executable={
                    'module':'ion.processes.data.upload.upload_data_processing',
                    'class':'UploadDataProcessing'
                }
            )
            process_definition_id = pd_client.create_process_definition(process_definition)
            # create process
            process_id = pd_client.create_process(process_definition_id)
            #schedule process
            config = DotDict()
            config.process.fuc_id = fuc_id
            config.process.dp_id = dataproduct_id
            pid = pd_client.schedule_process(process_definition_id, process_id=process_id, configuration=config)
            log.info('UploadDataProcessing process created %s' % pid)
            # response - only FileUploadContext ID and determined filetype for UX display
            resp = {'fuc_id': fuc_id}
            return gateway_json_response(resp)

        raise BadRequest('Invalid Upload')

    except Exception as e:
        return build_error_response(e)

'''
upload QC (CSV format)
'''
@service_gateway_app.route('/ion-service/upload/qc', methods=['POST'])
def upload_qc():
    upload_folder = FileSystem.get_url(FS.TEMP,'uploads')
    try:

        object_store = Container.instance.object_store
        
        # required fields
        upload = request.files['file'] # <input type=file name="file">

        if upload:

            # upload file - run filename through werkzeug.secure_filename
            filename = secure_filename(upload.filename)
            path = os.path.join(upload_folder, filename)
            upload_time = time.time()
            upload.save(path)
            filetype = _check_magic(upload) or 'CSV' # Either going to be ZIP or CSV, probably

            # register upload
            file_upload_context = {
                'name':'User uploaded QC file %s' % filename,
                'filename':filename,
                'filetype':filetype, # only CSV, no detection necessary
                'path':path,
                'upload_time':upload_time,
                'status':'File uploaded to server'
            }
            fuc_id, _ = object_store.create_doc(file_upload_context)

            # client to process dispatch
            pd_client = ProcessDispatcherServiceClient()

            # create process definition
            process_definition = ProcessDefinition(
                name='upload_qc_processor',
                executable={
                    'module':'ion.processes.data.upload.upload_qc_processing',
                    'class':'UploadQcProcessing'
                }
            )
            process_definition_id = pd_client.create_process_definition(process_definition)
            # create process
            process_id = pd_client.create_process(process_definition_id)
            #schedule process
            config = DotDict()
            config.process.fuc_id = fuc_id
            pid = pd_client.schedule_process(process_definition_id, process_id=process_id, configuration=config)
            log.info('UploadQcProcessing process created %s' % pid)
            # response - only FileUploadContext ID and determined filetype for UX display
            resp = {'fuc_id': fuc_id}
            return gateway_json_response(resp)

        raise BadRequest('Invalid Upload')

    except Exception as e:
        return build_error_response(e)

@service_gateway_app.route('/ion-service/upload/calibration', methods=['POST'])
def upload_calibration():
    """
    upload calibrations (CSV or ZIP format)
    """
    upload_folder = FileSystem.get_url(FS.TEMP,'uploads')
    try:
        object_store = Container.instance.object_store
        # required fields
        upload = request.files['file'] # <input type=file name="file">
        if upload:
            # upload file - run filename through werkzeug.secure_filename
            filename = secure_filename(upload.filename)
            path = os.path.join(upload_folder, filename)
            upload_time = time.time()
            upload.save(path)
            filetype = _check_magic(upload) or 'CSV' # Either going to be ZIP or CSV, probably
            # register upload
            file_upload_context = {
                'name':'User uploaded calibration file %s' % filename,
                'filename':filename,
                'filetype':filetype, # only CSV, no detection necessary
                'path':path,
                'upload_time':upload_time,
                'status':'File uploaded to server'
            }
            fuc_id, _ = object_store.create_doc(file_upload_context)
            # client to process dispatch
            pd_client = ProcessDispatcherServiceClient()
            # create process definition
            process_definition = ProcessDefinition(
                name='upload_calibration_processor',
                executable={
                    'module':'ion.processes.data.upload.upload_calibration_processing',
                    'class':'UploadCalibrationProcessing'
                }
            )
            process_definition_id = pd_client.create_process_definition(process_definition)
            # create process
            process_id = pd_client.create_process(process_definition_id)
            #schedule process
            config = DotDict()
            config.process.fuc_id = fuc_id
            pid = pd_client.schedule_process(process_definition_id, process_id=process_id, configuration=config)
            log.info('UploadCalibrationProcessing process created %s' % pid)
            # response - only FileUploadContext ID and determined filetype for UX display
            resp = {'fuc_id': fuc_id}
            return gateway_json_response(resp)

        raise BadRequest('Invalid Upload')

    except Exception as e:
        return build_error_response(e)

@service_gateway_app.route('/ion-service/upload/<fuc_id>', methods=['GET'])
def upload_status(fuc_id):
    try:
        object_store = Container.instance.object_store
        fuc = object_store.read(str(fuc_id))
        return gateway_json_response(fuc)
    except Exception as e:
        return build_error_response(e)

def _check_magic(f):
    '''
    determines file type from leading bytes
    '''
    # CDF
    f.seek(0)
    CDF = f.read(3)
    if CDF == 'CDF':
        VERSION_BYTE = ord(f.read(1))
        if VERSION_BYTE == 1:
            return 'NetCDF Classic'
        elif VERSION_BYTE == 2:
            return 'NetCDF 64-bit'
        return None
    # HDF
    HDF_MAGIC = b"\x89HDF\r\n\x1a\n"
    f.seek(0)
    HDF = f.read(8)
    if HDF == HDF_MAGIC:
        return 'HDF'

    # PKZIP
    ZIP_MAGIC = b"\x50\x4b\x03\x04"
    f.seek(0)
    ZIP = f.read(4)
    if ZIP == ZIP_MAGIC:
        return 'ZIP'
    return None
