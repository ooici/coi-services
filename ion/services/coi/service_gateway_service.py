#!/usr/bin/env python


__author__ = 'Stephen P. Henrie'
__license__ = 'Apache 2.0'

import inspect, collections, ast, simplejson, json, sys
from flask import Flask, request, abort
from gevent.wsgi import WSGIServer
from werkzeug.exceptions import Forbidden

from pyon.public import IonObject, Container, ProcessRPCClient
from pyon.core.exception import NotFound, Inconsistent, BadRequest
from pyon.core.registry import get_message_class_in_parm_type, getextends

from interface.services.coi.iservice_gateway_service import BaseServiceGatewayService
from interface.services.coi.iresource_registry_service import IResourceRegistryService, ResourceRegistryServiceProcessClient
from interface.services.coi.iorg_management_service import OrgManagementServiceProcessClient
from pyon.util.log import log


#Initialize the flask app
app = Flask(__name__)

#Retain a module level reference to the service class for use with Process RPC calls below
service_gateway_instance = None

DEFAULT_WEB_SERVER_HOSTNAME = ""
DEFAULT_WEB_SERVER_PORT = 5000

GATEWAY_RESPONSE = 'GatewayResponse'
GATEWAY_ERROR = 'GatewayError'
GATEWAY_ERROR_EXCEPTION = 'Exception'
GATEWAY_ERROR_MESSAGE = 'Message'

DEFAULT_ACTOR_ID = 'anonymous'
DEFAULT_EXPIRY = '0'

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

        #Start the gevent web server unless disabled
        if self.web_server_enabled:
            self.start_service(self.server_hostname,self.server_port)

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
            raise BadRequest("Cannot find a client class for the specified service: %s" % service_name )

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

        param_list = create_parameter_list(service_name, target_client,operation, json_params)

        #Add governance headers
        ion_actor_id, expiry = get_governance_info_from_request(json_params)
        param_list['headers'] = build_message_headers(ion_actor_id, expiry)

        client = target_client(node=Container.instance.node, process=service_gateway_instance)
        methodToCall = getattr(client, operation)
        result = methodToCall(**param_list)

        return json_response({GATEWAY_RESPONSE: result})


    except Exception, e:
        return build_error_response(e)



#Private implementation of standard flask jsonify to specify the use of an encoder to walk ION objects
def json_response(response_data):

    return app.response_class(simplejson.dumps({'data': response_data}, default=ion_object_encoder,
        indent=None if request.is_xhr else 2), mimetype='application/json')

def build_error_response(e):

    exc_type, exc_obj, exc_tb = sys.exc_info()
    result = {
        GATEWAY_ERROR_EXCEPTION : exc_type.__name__,
        GATEWAY_ERROR_MESSAGE : str(e.message)
    }

    return json_response({ GATEWAY_ERROR :result } )

def get_governance_info_from_request(json_params):

    #Default values for governance headers.
    actor_id = DEFAULT_ACTOR_ID
    expiry = DEFAULT_EXPIRY
    if not json_params:

        if request.args.has_key('requester'):
            actor_id = convert_unicode(request.args['requester'])

        if request.args.has_key('expiry'):
            expiry = convert_unicode(request.args['expiry'])
    else:

        if json_params['serviceRequest'].has_key('requester'):
            actor_id = convert_unicode(json_params['serviceRequest']['requester'])

        if json_params['serviceRequest'].has_key('expiry'):
            expiry = convert_unicode(json_params['serviceRequest']['expiry'])

    return actor_id, expiry

def build_message_headers( ion_actor_id, expiry):

    headers = dict()

    headers['ion-actor-id'] = ion_actor_id
    headers['expiry'] = expiry

    #If this is an anonymous requester then there are no roles associated with the request
    if ion_actor_id == DEFAULT_ACTOR_ID:
        headers['ion-actor-roles'] = dict()
        return headers


    org_client = OrgManagementServiceProcessClient(node=Container.instance.node, process=service_gateway_instance)
    org_roles = org_client.find_all_roles_by_user(ion_actor_id)  #TODO - How does this request get protected?

    #Iterate the Org(s) that the user belongs to and create a header that lists only the role names per Org assigned
    #to the user; i.e. {'ION': ['Member', 'Operator'], 'Org2': ['Member']}
    role_header = dict()
    for org in org_roles:
        role_header[org] = []
        for role in org_roles[org]:
            role_header[org].append(role.name)


    headers['ion-actor-roles'] = role_header

    return headers

#Build parameter list dynamically from
def create_parameter_list(service_name, target_client,operation, json_params):
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
            if json_params['serviceRequest']['params'].has_key(arg):

                #This if handles ION objects as a 2 element list: [Object Type, { field1: val1, ...}]
                if isinstance(json_params['serviceRequest']['params'][arg], list):

                    #TODO - Potentially remove these conversions whenever ION objects support unicode
                    # UNICODE strings are not supported with ION objects
                    ion_object_name = convert_unicode(json_params['serviceRequest']['params'][arg][0])
                    object_params = convert_unicode(json_params['serviceRequest']['params'][arg][1])

                    param_list[arg] = create_ion_object(ion_object_name, object_params)

                else:  # The else branch is for simple types ( non-ION objects )
                    param_list[arg] = convert_unicode(json_params['serviceRequest']['params'][arg])

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
        #Look to see if a specific resource type has been specified - if not default to all
        if request.args.has_key('type'):
            resultSet = set(getextends(request.args['type'])) if getextends(request.args['type']) is not None else set()
        else:
            type_list = getextends('Resource')
            resultSet = set(type_list)

        ret_list = []
        for res in sorted(resultSet):
            ret_list.append(res)


        return json_response({ GATEWAY_RESPONSE :ret_list } )


    except Exception, e:
        return build_error_response(e)



#Returns a json object for a specified resource type with all default values.
@app.route('/ion-service/resource_type_schema/<resource_type>')
def get_resource_schema(resource_type):


    try:
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


        return json_response({ GATEWAY_RESPONSE :ret_obj } )

    except Exception, e:
        return build_error_response(e)



#More RESTfull examples...should probably not use but here for example reference

#This example calls the resource registry with an id passed in as part of the URL
#http://hostname:port/ion-service/resource/c1b6fa6aadbd4eb696a9407a39adbdc8
@app.route('/ion-service/rest/resource/<resource_id>')
def get_resource(resource_id):

    result = None
    client = ResourceRegistryServiceProcessClient(node=Container.instance.node, process=service_gateway_instance)
    if resource_id != '':
        try:
            #Database object IDs are not unicode
            result = client.read(convert_unicode(resource_id))
            if not result:
                raise NotFound("No resource found for id: %s " % resource_id)

            return json_response({ GATEWAY_RESPONSE :result } )

        except Exception, e:
            return build_error_response(e)




#Example operation to return a list of resources of a specific type like
#http://hostname:port/ion-service/find_resources/BankAccount
@app.route('/ion-service/rest/find_resources/<resource_type>')
def list_resources_by_type(resource_type):

    result = None

    client = ResourceRegistryServiceProcessClient(node=Container.instance.node, process=service_gateway_instance)
    try:
        #Resource Types are not in unicode
        res_list,_ = client.find_resources(restype=convert_unicode(resource_type) )

        return json_response({ GATEWAY_RESPONSE :res_list } )

    except Exception, e:
        return build_error_response(e)



#Example restful call to a client function for another service like
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





