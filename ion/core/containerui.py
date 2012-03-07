#!/usr/bin/env python

__author__ = 'Michael Meisinger'
__license__ = 'Apache 2.0'

import collections, traceback
import flask
from flask import Flask, request, abort
from gevent.wsgi import WSGIServer

from pyon.public import Container, StandaloneProcess, log
from pyon.core.exception import NotFound, Inconsistent, BadRequest
from pyon.core.registry import getextends

from pyon.util.containers import named_any


#Initialize the flask app
app = Flask(__name__)

DEFAULT_WEB_SERVER_HOSTNAME = ""
DEFAULT_WEB_SERVER_PORT = 8080

containerui_instance = None

class ContainerUI(StandaloneProcess):
    """
    """

    def on_init(self):
        #defaults
        self.http_server = None
        self.server_hostname = DEFAULT_WEB_SERVER_HOSTNAME
        self.server_port = DEFAULT_WEB_SERVER_PORT
        self.web_server_enabled = True
        self.logging = None

        #retain a pointer to this object for use in ProcessRPC calls
        global containerui_instance
        containerui_instance = self


        #Start the gevent web server unless disabled
        if self.web_server_enabled:
            self.start_service(self.server_hostname, self.server_port)

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


@app.route('/', methods=['GET','POST'])
def process_index():
    try:
        fragments = [
            "<h1>Welcome to ION TestUI</h1>",
            "<p><a href='/restypes'>List of Resource Types</a></p>",
            "<p><a href='/dir'>Browse ION directory</a></p>",
            ]
        content = "\n".join(fragments)
        return build_page(content)

    except Exception, e:
        return build_simple_page("Error: %s" % traceback.format_exc())

@app.route('/restypes', methods=['GET','POST'])
def process_list_resource_types():
    try:
        type_list = set(getextends('Resource'))
        fragments = [
            "<h1>List of Resource Types</h1>",
            "<p>",
        ]

        for restype in sorted(type_list):
            fragments.append("<a href='/list/%s'>%s</a><br>" % (restype, restype))

        fragments.append("</p>")

        content = "\n".join(fragments)
        return build_page(content)

    except Exception, e:
        return build_simple_page("Error: %s" % traceback.format_exc())

@app.route('/list/<resource_type>', methods=['GET','POST'])
def process_list_resources(resource_type):
    try:
        restype = convert_unicode(resource_type)
        res_list,_ = Container.instance.resource_registry.find_resources(restype=restype)

        fragments = [
            "<h1>List of '%s' Resources</h1>" % restype,
            "<p>",
            "<table border='1' cellspacing='0'>",
            "<tr>"
        ]

        fragments.extend(build_table_header(restype))
        fragments.append("<th>Associations</th></tr>")

        for res in res_list:
            fragments.append("<tr>")
            fragments.extend(build_table_row(res))
            fragments.append("<td>")
            fragments.extend(build_associations(res._id))
            fragments.append("</td>")
            fragments.append("</tr>")

        fragments.append("</table></p>")

        content = "\n".join(fragments)
        return build_page(content)

    except NotFound:
        return flask.redirect("/")
    except Exception, e:
        return build_simple_page("Error: %s" % traceback.format_exc())

@app.route('/view/<resource_id>', methods=['GET','POST'])
def process_view_resource(resource_id):
    try:
        resid = convert_unicode(resource_id)
        res = Container.instance.resource_registry.read(resid)
        restype = res._get_type()

        fragments = [
            "<h1>View %s '%s'</h1>" % (build_type_link(restype), res.name),
            "<h2>Fields</h2>",
            "<p>",
            "<table border='1' cellspacing='0'>",
            "<tr><th>Field</th><th>Type</th><th>Value</th></tr>"
            ]

        schema = res._schema
        fragments.append("<tr><td>%s</td><td>%s</td><td>%s</td>" % ("type", "str", restype))
        fragments.append("<tr><td>%s</td><td>%s</td><td>%s</td>" % ("_id", "str", res._id))
        fragments.append("<tr><td>%s</td><td>%s</td><td>%s</td>" % ("_rev", "str", res._rev))

        for field in sorted(schema.keys()):
            fragments.append("<tr><td>%s</td><td>%s</td><td>%s&nbsp;</td>" % (field, schema[field]["type"], getattr(res, field)))

        fragments.append("</p></table>")

        fragments.append("<h2>Associations</h2><p>")
        fragments.extend(build_associations(res._id))
        fragments.append("</p>")

        content = "\n".join(fragments)
        return build_page(content)

    except NotFound:
        return flask.redirect("/")
    except Exception, e:
        return build_simple_page("Error: %s" % traceback.format_exc())

@app.route('/dir', methods=['GET','POST'], defaults={'path':'~'})
@app.route('/dir/<path>', methods=['GET','POST'])
def process_dir_path(path):
    try:
        #path = convert_unicode(path)
        path = str(path)
        path = path.replace("~", "/")
        de_list = Container.instance.directory.find_child_entries(path)
        entry = Container.instance.directory.lookup(path)
        fragments = [
            "<h1>Directory %s</h1>" % (build_dir_path(path)),
            "<h2>Attributes</h2>",
            "<p><table border='1' cellspacing='0'><tr><th>Name</th><th>Value</th></tr>"
        ]

        for attr in sorted(entry.keys()):
            attval = entry[attr]
            fragments.append("<tr><td>%s</td><td>%s</td></tr>" % (attr, attval))
        fragments.append("</table></p>")

        fragments.append("</p><h2>Child Entries</h2><p><table border='1' cellspacing='0'><tr><th>Key</th><th>Timestamp</th><th>Attributes</th></tr>")
        for de in de_list:
            if '/' in de.parent:
                org, parent = de.parent.split("/", 1)
                parent = "/"+parent
            else:
                parent = ""
            fragments.append("<tr><td>%s</td><td>%s</td><td>%s&nbsp;</td></tr>" % (build_dir_link(parent,de.key), "&nbsp;", str(de.attributes)))

        fragments.append("</table></p>")

        content = "\n".join(fragments)
        return build_page(content)

    except NotFound:
        return flask.redirect("/")
    except Exception, e:
        return build_simple_page("Error: %s" % traceback.format_exc())

def build_dir_path(path):
    if path.startswith('/'):
        path = path[1:]
    levels = path.split("/")
    fragments = []
    parent = ""
    for level in levels:
        fragments.append(build_dir_link(parent,level))
        fragments.append("/")
        parent = "%s/%s" % (parent, level)
    return "".join(fragments)

def build_dir_link(parent, key):
    path = "%s/%s" % (parent, key)
    path = path.replace("/","~")
    return build_link(key, "/dir/%s" % path)

def build_type_link(restype):
    return build_link(restype, "/list/%s" % restype)

def build_link(text, link):
    return "<a href='%s'>%s</a>" % (link, text)

def build_associations(resid):
    fragments = []
    obj_list, assoc_list = Container.instance.resource_registry.find_objects(subject=resid, id_only=False)
    for obj,assoc in zip(obj_list,assoc_list):
        fragments.append("TO: %s <a href='/view/%s'>&apos;%s&apos;</a> (%s)<br>" % (assoc.p, assoc.o, obj.name, build_type_link(obj._get_type())))

    obj_list, assoc_list = rr_client.find_subjects(object=resid, id_only=False)
    for obj,assoc in zip(obj_list,assoc_list):
        fragments.append("FROM: %s <a href='/view/%s'>&apos;%s&apos;</a> (%s)<br>" % (assoc.p, assoc.s, obj.name, build_type_link(obj._get_type())))

    return fragments

obj_classes = {}

def _get_object_class(objtype):
    if objtype in obj_classes:
        return obj_classes[objtype]
    obj_class = named_any("interface.objects.%s" % objtype)
    obj_classes[objtype] = obj_class
    return obj_class

def build_table_header(objtype):
    schema = _get_object_class(objtype)._schema
    fragments = []
    fragments.append("<th>ID</th>")
    for field in schema:
        fragments.append("<th>%s</th>" % (field))
    return fragments

def build_table_row(obj):
    schema = obj._schema
    fragments = []
    fragments.append("<td><a href='/view/%s'>%s</a></td>" % (obj._id,obj._id))
    for field in schema:
        fragments.append("<td>%s&nbsp;</td>" % (getattr(obj, field)))
    return fragments

def build_simple_page(content):
    return build_page("<p><pre>" + content + "</pre></p>")

def build_page(content, title=""):
    fragments = [
        "<html><head></head><body>",
        content,
        "</body></html>"
    ]
    return "\n".join(fragments)

def convert_unicode(data):
    """
    Used to recursively convert unicode in JSON structures into proper data structures
    """
    if isinstance(data, unicode):
        return str(data)
    elif isinstance(data, collections.Mapping):
        return dict(map(convert_unicode, data.iteritems()))
    elif isinstance(data, collections.Iterable):
        return type(data)(map(convert_unicode, data))
    else:
        return data
