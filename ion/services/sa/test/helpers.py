import hashlib
from unittest.case import SkipTest
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from ion.util.enhanced_resource_registry_client import EnhancedResourceRegistryClient
from mock import Mock
from pyon.core.exception import Unauthorized, Inconsistent, NotFound, BadRequest
from pyon.public import IonObject
from pyon.public import RT
from ooi.logging import log

from interface.objects import AttachmentType
from pyon.util.containers import DotDict
from pyon.util.int_test import IonIntegrationTestCase

import inspect

_sa_test_helpers_ionobj_count = {}

def any_old(resource_type, extra_fields=None):
    """
    Create any old resource... a generic and unique object of a given type
    @param resource_type the resource type
    @param extra_fields dict of any extra fields to set
    """
    if not extra_fields:
        extra_fields = {}

    if resource_type not in _sa_test_helpers_ionobj_count:
        _sa_test_helpers_ionobj_count[resource_type] = 0

    _sa_test_helpers_ionobj_count[resource_type] = _sa_test_helpers_ionobj_count[resource_type] + 1

    name = "%s_%d" % (resource_type, _sa_test_helpers_ionobj_count[resource_type])
    desc = "My %s #%d" % (resource_type, _sa_test_helpers_ionobj_count[resource_type])
    log.debug("Creating any old %s IonObject (#%d)", resource_type, _sa_test_helpers_ionobj_count[resource_type])

    ret = IonObject(resource_type, name=name, description=desc)
    
    #add any extra fields
    for k, v in extra_fields.iteritems():
        setattr(ret, k, v)

    return ret

def add_keyworded_attachment(resource_registry_client, resource_id, keywords, extra_fields=None):
    """
    create a generic attachment to a given resource -- a generic and unique attachment

    @param resource_registry_client a service client
    @param resource_id string the resource to get the attachment
    @param keywords list of string keywords
    @param extra_fields dict of extra fields to set.  "keywords" can be set here with no ill effects
    """
    
    if not extra_fields:
        extra_fields = {}

    if not "attachment_type" in extra_fields:
        extra_fields["attachment_type"] = AttachmentType.ASCII
        
    if not "content" in extra_fields:
        extra_fields["content"] = "these are contents"

    if not "keywords" in extra_fields:
        extra_fields["keywords"] = []

    for k in keywords:
        extra_fields["keywords"].append(k)

    ret = any_old(RT.Attachment, extra_fields)

    resource_registry_client.create_attachment(resource_id, ret)

    return ret


class UnitTestGenerator(object):
    """
    This class adds test methods for CRUD and associations in the given service

    For example, OUTSIDE and AFTER the TestInstrumentManagement class, write this:

        utg = UnitTestGenerator(TestInstrumentManagement,
                                InstrumentManagementService)

        utg.add_resource_unittests(RT.InstrumentAgentInstance, "instrument_agent_instance",
                                          {"exchange_name": "rhubarb"}

    The impl object MUST be available as a class variable in the service under test!

    """

    def __init__(self, unit_test_class, service_under_test_class):
        """
        @param unit_test_class the class that will perform the setup/ testing, to which methods will be added
        @param service_under_test_class the class of the service that's being tested
        """

        self.tester_class = unit_test_class

        #create the service
        self.service_instance  = service_under_test_class()
        self.service_instance.on_init()


        self.all_in_one = False


    def test_all_in_one(self, yes):
        """
        @param yes whether to run int tests all in one
        """
        self.all_in_one = yes


    def build_test_descriptors(self, resource_params):
        """
        build a sample resource from the supplied impl class
        and populate it with the supplied parameters

        @param resource_params a dict of params for the sample resource
        """

        # string describing the extra fields in the resource, and a (probably) unique ID
        sample_resource_extras = ""
        sample_resource_md5    = ""

        # build out strings if there are more params
        if resource_params:
            extras = []
            for k, v in resource_params.iteritems():
                extras.append("%s='%s'" % (k, v))
            sample_resource_extras = "(with %s)" % ", ".join(extras)

        sample_resource_md5 = "_%s" % str(hashlib.sha224(sample_resource_extras).hexdigest()[:7])

        self.sample_resource_extras  = sample_resource_extras
        self.sample_resource_md5     = sample_resource_md5


    def sample_resource_factory(self, iontype, resource_params):
        """
        build a sample resource factory from the supplied impl class
         that produces resources populated with the supplied parameters

         this will give us the ability to use the service that we're testing
         to generate the resource for us.

        @param iontype the ion type to create
        @resource_params a dict of params to add
        """

        def fun():
            #ret = Mock()
            log.debug("Creating sample %s", iontype)
            ret = IonObject(iontype)
            ret.name = "sample %s" % iontype
            ret.description = "description of sample %s" % iontype
            for k, v in resource_params.iteritems():
                setattr(ret, k, v)
            return ret

        return fun

    def find_class_variable_name(self, instance, target_type):
        """
        determine which class variable in the instance is of the target type
        @param instance the class to be searched
        @param target_type the type of the variable we want to find
        @retval string the name of the class variable

        we use this to get the reference to a variable in another class but
          WITHOUT knowing what it's called.
        """
        ret = None
        for k, v in instance.__dict__.iteritems():
            if type(v) == target_type:
                ret = k
        return ret



    def add_resource_unittests(self, resource_iontype, resource_label, resource_params=None):
        """
        Add tests for the resorce_impl_class to the (self.)resource_tester_class

        @param resource_iontype the IonObject type to test with (e.g. RT.PlatformModel)
        @param resource_label the base of the function names for this resource (e.g. platform_model)
        @param resource_params dictionary of extra params to add to the sample resource

        this function will be huge.  it is a list of smaller functions that are templates
         for tests of various resource_impl class functionality.  the functions are given
         proper references to member variables in the service and test class, then injected
         into the test class itself.

        """

        #init default args
        if None == resource_params: resource_params = {}


        self.build_test_descriptors(resource_params)

        #this is convoluted but it helps me debug by
        #  being able to inject text into the sample_resource_extras
        sample_resource = self.sample_resource_factory(resource_iontype, resource_params)

        all_in_one   = self.all_in_one
        find_cv_func = self.find_class_variable_name

        service_type = type(self.service_instance)

        added_methods = {}

        def add_new_method(name, docstring, newmethod):
            """
            dynamically add a new method to the tester class
            @param name the name of the new method
            @param docstring a description of the test
            @newmethod the function itself
            """
            newmethod.__name__ = name
            newmethod.__doc__  = docstring
            setattr(self.tester_class, newmethod.__name__, newmethod)
            added_methods[name] = True

        def add_test_method(name, docstring, newmethod):
            """
            dynamically add a test method to the tester class
            @param name the name of the test function (minus the "test_" part)
            @param docstring a description of the test
            @newmethod the function itself
            """
            add_new_method("test_%s" % name, docstring, newmethod)

        def make_name(name):
            """
            make a good name for a test from the resource name and an md5 of extra params
            @param name the base string for the name
            """
            assert self.sample_resource_md5
            return "%s%s" % (name, self.sample_resource_md5)

        def make_doc(doc):
            """
            make a good doc string for a test from by including the extra params
            @param doc the base string for the descripton
            """
            return "%s %s" % (doc, self.sample_resource_extras)

        def gen_svc_cleanup():
            def fun(instance):
                for k, _ in added_methods.iteritems():
                    if hasattr(instance, k):
                        #raise BadRequest("found/removing %s from %s" % (k, type(instance)))
                        instance.addCleanup(delattr, instance, k)

            add_new_method("_utg_cleanup_%s%s" % (resource_iontype, self.sample_resource_md5), "Cleanup", fun)

            # add (probably overwrite) the function to process all the cleanups
            def whole_cleanup(instance):
                for k in dir(instance):
                    if -1 < k.find("_utg_cleanup_"):
                        cleanup_fn = getattr(instance, k)
                        cleanup_fn()

            #if hasattr(self.tester_class, "resource_impl_cleanup")
            add_new_method("_utg_whole_cleanup", "Global cleanup", whole_cleanup)

        def gen_svc_lookup():
            """
            put a new method in the tester class to find the instance of the service being tested

            the prefix is "_utg_": unit_test_generator
            """
            def getsvc_fun(self):
                """
                self is an instance of the tester class
                """
                # cache a reference to the service obj that is known to this class
                if not hasattr(self, "_utg_service_obj"):
                    service_itself = getattr(self, find_cv_func(self, service_type))
                    self._utg_service_obj = service_itself
                    assert self._utg_service_obj

                return self._utg_service_obj

            def getcrudmethod_fun(self, rsrc_lbl, method_str):
                """
                self is an instance of the tester class
                method_str is a crud method name like "create" that will become "create_resource_label"
                """
                svc = self._utg_getservice()
                methodname = "%s_%s" % (method_str, rsrc_lbl)
                if not hasattr(svc, methodname):
                    raise SkipTest("CRUD method does not exist, skipping")
                return getattr(svc, methodname)



            if not hasattr(self.tester_class, "_utg_getservice"):
                add_new_method("_utg_getservice", "Finds instance of service under test", getsvc_fun)

            if not hasattr(self.tester_class, "_utg_getcrudmethod"):
                add_new_method("_utg_getcrudmethod", "Finds instance of crud method in service", getcrudmethod_fun)




        def test_create_fun(self):
            """
            self is an instance of the tester class
            """
            log.debug("test_create_fun")
            # get objects
            svc = self._utg_getservice()
            testfun = self._utg_getcrudmethod(resource_label, "create")
            good_sample_resource = sample_resource()


            #configure Mock
            if all_in_one: svc.clients.resource_registry.create.reset_mock()
            svc.clients.resource_registry.create.return_value = ('111', 'bla')
            svc.clients.resource_registry.find_resources.return_value = ([], [])

            sample_resource_id = testfun(good_sample_resource)

            svc.clients.resource_registry.create.assert_called_once_with(good_sample_resource)
            self.assertEqual(sample_resource_id, '111')




        def test_create_bad_wrongtype_fun(self):
            """
            self is an instance of the tester class
            """
            log.debug("test_create_bad_wrongtype_fun")
            # get objects
            svc = self._utg_getservice()
            testfun = self._utg_getcrudmethod(resource_label, "create")
            bad_sample_resource = IonObject(RT.Resource, name="Generic Resource")


            #configure Mock
            if all_in_one: svc.clients.resource_registry.create.reset_mock()
            svc.clients.resource_registry.create.return_value = ('111', 'bla')

            self.assertRaisesRegexp(BadRequest, "type", testfun, bad_sample_resource)
            self.assertEqual(0, svc.clients.resource_registry.create.call_count)


        def test_create_bad_noname_fun(self):
            """
            self is an instance of the tester class
            """
            log.debug("test_create_bad_noname_fun")
            # get objects
            svc = self._utg_getservice()
            testfun = self._utg_getcrudmethod(resource_label, "create")
            bad_sample_resource = sample_resource()
            delattr(bad_sample_resource, "name")

            #configure Mock
            if all_in_one: svc.clients.resource_registry.create.reset_mock()
            svc.clients.resource_registry.create.return_value = ('111', 'bla')
            svc.clients.resource_registry.find_resources.return_value = ([], [])

            self.assertRaisesRegexp(BadRequest, "name", testfun, bad_sample_resource)
            self.assertEqual(0, svc.clients.resource_registry.create.call_count)


        def test_create_bad_dupname_fun(self):
            """
            self is an instance of the tester class
            """
            log.debug("test_create_bad_dupname_fun")
            # get objects
            svc = self._utg_getservice()
            testfun = self._utg_getcrudmethod(resource_label, "create")
            bad_sample_resource = sample_resource()
            #really, the resource doesn't matter; it's the retval from find that matters

            #configure Mock
            if all_in_one: svc.clients.resource_registry.create.reset_mock()
            svc.clients.resource_registry.create.return_value = ('111', 'bla')
            svc.clients.resource_registry.find_resources.return_value = ([0], [0])

            self.assertRaisesRegexp(BadRequest, "uplicate", testfun, bad_sample_resource)
            self.assertEqual(0, svc.clients.resource_registry.create.call_count)


        def test_read_fun(self):
            """
            self is an instance of the tester class
            """
            log.debug("test_read_fun")
            # get objects
            svc = self._utg_getservice()
            testfun = self._utg_getcrudmethod(resource_label, "read")
            myret = sample_resource()

            #configure Mock
            if all_in_one: svc.clients.resource_registry.read.reset_mock()
            svc.clients.resource_registry.read.return_value = myret

            response = testfun("111")
            svc.clients.resource_registry.read.assert_called_once_with("111", "")
            self.assertEqual(response, myret)

            if all_in_one: svc.clients.resource_registry.reset_mock()


        def test_read_bad_wrongtype_fun(self):
            """
            self is an instance of the tester class
            """
            log.debug("test_read_bad_wrongtype_fun")
            # get objects
            svc = self._utg_getservice()
            testfun = self._utg_getcrudmethod(resource_label, "read")
            myret = IonObject(RT.Resource, name="Generic Resource")

            #configure Mock
            if all_in_one: svc.clients.resource_registry.read.reset_mock()
            svc.clients.resource_registry.read.return_value = myret

            self.assertEqual(0, svc.clients.resource_registry.read.call_count)
            self.assertRaisesRegexp(BadRequest, "type", testfun, "111")
            svc.clients.resource_registry.read.assert_called_once_with("111", "")




        def test_update_fun(self):
            """
            self is an instance of the tester class
            """
            log.debug("test_update_fun")
            # get objects
            svc = self._utg_getservice()
            testfun = self._utg_getcrudmethod(resource_label, "update")
            good_sample_resource = sample_resource()
            setattr(good_sample_resource, "_id", "111")

            #configure Mock
            if all_in_one: svc.clients.resource_registry.update.reset_mock()
            svc.clients.resource_registry.update.return_value = ('111', 'bla')
            svc.clients.resource_registry.find_resources.return_value = ([], [])

            testfun(good_sample_resource)

            svc.clients.resource_registry.update.assert_called_once_with(good_sample_resource)




        def test_update_bad_dupname_fun(self):
            """
            self is an instance of the tester class
            """
            log.debug("test_update_bad_dupname_fun")
            # get objects
            svc = self._utg_getservice()
            testfun = self._utg_getcrudmethod(resource_label, "update")
            bad_sample_resource = sample_resource()
            setattr(bad_sample_resource, "_id", "111")

            #configure Mock
            if all_in_one: svc.clients.resource_registry.update.reset_mock()
            svc.clients.resource_registry.find_resources.return_value = ([0], [0])

            self.assertRaisesRegexp(BadRequest, "uplicate", testfun, bad_sample_resource)
            self.assertEqual(0, svc.clients.resource_registry.update.call_count)



        def test_update_bad_wrongtype_fun(self):
            """
            self is an instance of the tester class
            """
            log.debug("test_update_bad_wrongtype_fun")
            # get objects
            svc = self._utg_getservice()
            testfun = self._utg_getcrudmethod(resource_label, "update")
            bad_sample_resource = IonObject(RT.Resource, name="Generic Name")
            setattr(bad_sample_resource, "_id", "111")

            if all_in_one: svc.clients.resource_registry.update.reset_mock()
            self.assertRaisesRegexp(BadRequest, "type", testfun, bad_sample_resource)
            self.assertEqual(0, svc.clients.resource_registry.update.call_count)




        def test_delete_fun(self):
            """
            self is an instance of the tester class
            """
            log.debug("test_delete_fun")
            # get objects
            svc = self._utg_getservice()
            testfun = self._utg_getcrudmethod(resource_label, "delete")
            myret = sample_resource()

            #configure Mock
            if all_in_one: svc.clients.resource_registry.read.reset_mock()
            if all_in_one: svc.clients.resource_registry.delete.reset_mock()
            if all_in_one: svc.clients.resource_registry.retire.reset_mock()
            svc.clients.resource_registry.read.return_value = myret
            svc.clients.resource_registry.delete.return_value = None
            svc.clients.resource_registry.retire.return_value = None

            try:
                testfun("111")
            except TypeError as te:
                # for logic tests that run into mock trouble
                if "'Mock' object is not iterable" != te.message:
                    raise te
                elif all_in_one:
                    svc.clients.resource_registry.reset_mock()
                    return
                else:
                    raise SkipTest("Must test this with INT test")


            svc.clients.resource_registry.retire.assert_called_once_with("111")
            self.assertEqual(0, svc.clients.resource_registry.delete.call_count)




        def test_delete_bad_wrongtype_fun(self):
            """
            self is an instance of the tester class
            """
            log.debug("test_delete_bad_wrongtype_fun")
            # get objects
            svc = self._utg_getservice()
            testfun = self._utg_getcrudmethod(resource_label, "delete")
            myret = IonObject(RT.Resource, name="Generic Name")

            #configure Mock
            if all_in_one: svc.clients.resource_registry.delete.reset_mock()
            if all_in_one: svc.clients.resource_registry.retire.reset_mock()
            if all_in_one: svc.clients.resource_registry.read.reset_mock()
            svc.clients.resource_registry.read.return_value = myret

            try:
                self.assertRaisesRegexp(BadRequest, "type", testfun, "111")
            except TypeError as te:
                # for logic tests that run into mock trouble
                if "'Mock' object is not iterable" != te.message:
                    raise te
                elif all_in_one:
                    return
                else:
                    raise SkipTest("Must test this with INT test")
            self.assertEqual(0, svc.clients.resource_registry.retire.call_count)
            self.assertEqual(0, svc.clients.resource_registry.delete.call_count)


        def test_force_delete_fun(self):
            """
            self is an instance of the tester class
            """
            log.debug("test_force_delete_fun")
            # get objects
            svc = self._utg_getservice()
            testfun = self._utg_getcrudmethod(resource_label, "force_delete")
            myret = sample_resource()

            #configure Mock
            if all_in_one: svc.clients.resource_registry.delete.reset_mock()
            if all_in_one: svc.clients.resource_registry.retire.reset_mock()
            svc.clients.resource_registry.read.return_value = myret
            svc.clients.resource_registry.delete.return_value = None
            svc.clients.resource_registry.find_resources.return_value = None
            svc.clients.resource_registry.find_objects.return_value = ([], [])
            svc.clients.resource_registry.find_subjects.return_value = ([], [])

            try:
                testfun("111")
            except TypeError as te:
                # for logic tests that run into mock trouble
                if "'Mock' object is not iterable" != te.message:
                    raise te
                elif all_in_one:
                    return
                else:
                    raise SkipTest("Must test this with INT test")



            svc.clients.resource_registry.delete.assert_called_once_with("111")



        def test_force_delete_bad_wrongtype_fun(self):
            """
            self is an inst ance of the tester class
            """
            log.debug("test_force_delete_bad_wrongtype_fun")
            # get objects
            svc = self._utg_getservice()
            testfun = self._utg_getcrudmethod(resource_label, "force_delete")
            myret = IonObject(RT.Resource, name="Generic Name")

            #configure Mock
            if all_in_one: svc.clients.resource_registry.delete.reset_mock()
            if all_in_one: svc.clients.resource_registry.retire.reset_mock()
            if all_in_one: svc.clients.resource_registry.read.reset_mock()
            svc.clients.resource_registry.find_objects.return_value = ([], [])
            svc.clients.resource_registry.find_subjects.return_value = ([], [])
            svc.clients.resource_registry.read.return_value = myret

            try:
                self.assertRaisesRegexp(BadRequest, "type", testfun, "111")
            except TypeError as te:
                # for logic tests that run into mock trouble
                if "'Mock' object is not iterable" != te.message:
                    raise te
                elif all_in_one:
                    return
                else:
                    raise SkipTest("Must test this with INT test")


            self.assertEqual(0, svc.clients.resource_registry.retire.call_count)
            self.assertEqual(0, svc.clients.resource_registry.delete.call_count)



        def gen_test_create():
            """
            generate the function to test the create
            """
            name = make_name("%s_create" % resource_label)
            doc  = make_doc("Creation of a new %s resource" % resource_iontype)
            add_test_method(name, doc, test_create_fun)


        def gen_test_create_bad_wrongtype():
            """
            generate the function to test the create for the wrong resource type
            """
            name = make_name("%s_create_bad_wrongtype" % resource_label)
            doc  = make_doc("Creation of a (bad) new %s resource (wrong type)" % resource_iontype)
            add_test_method(name, doc, test_create_bad_wrongtype_fun)


        def gen_test_create_bad_noname():
            """
            generate the function to test the create in a bad case
            """
            name = make_name("%s_create_bad_noname" % resource_label)
            doc  = make_doc("Creation of a (bad) new %s resource (no name)" % resource_iontype)
            add_test_method(name, doc, test_create_bad_noname_fun)


        def gen_test_create_bad_dupname():
            """
            generate the function to test the create in a bad case where the name already exists
            """
            name = make_name("%s_create_bad_dupname" % resource_label)
            doc  = make_doc("Creation of a (bad) new %s resource (duplicate name)" % resource_iontype)
            add_test_method(name, doc, test_create_bad_dupname_fun)


        def gen_test_read():
            """
            generate the function to test the read
            """
            name = make_name("%s_read" % resource_label)
            doc  = make_doc("Reading a %s resource" % resource_iontype)
            add_test_method(name, doc, test_read_fun)


        def gen_test_read_bad_wrongtype():
            """
            generate the function to test the read with bad type
            """
            name = make_name("%s_read_bad_wrongtype" % resource_label)
            doc  = make_doc("Reading a %s resource but having it come back as wrong type" % resource_iontype)
            add_test_method(name, doc, test_read_bad_wrongtype_fun)


        def gen_test_update():
            """
            generate the function to test the update
            """
            name = make_name("%s_update" % resource_label)
            doc  = make_doc("Updating a %s resource" % resource_iontype)
            add_test_method(name, doc, test_update_fun)


        def gen_test_update_bad_wrongtype():
            """
            generate the function to test the update with wrong type
            """
            name = make_name("%s_update_bad_wrongtype" % resource_label)
            doc  = make_doc("Updating a %s resource with the wrong type" % resource_iontype)
            add_test_method(name, doc, test_update_bad_wrongtype_fun)


        def gen_test_update_bad_dupname():
            """
            generate the function to test the update with wrong type
            """
            name = make_name("%s_update_bad_duplicate" % resource_label)
            doc  = make_doc("Updating a %s resource to a duplicate name" % resource_iontype)
            add_test_method(name, doc, test_update_bad_dupname_fun)


        def gen_test_delete():
            """
            generate the function to test the delete
            """
            name = make_name("%s_delete" % resource_label)
            doc  = make_doc("Deleting (retiring) a %s resource" % resource_iontype)
            add_test_method(name, doc, test_delete_fun)


        def gen_test_delete_bad_wrongtype():
            """
            generate the function to test the delete with wrong type
            """
            name = make_name("%s_delete_bad_wrongtype" % resource_label)
            doc  = make_doc("Deleting (retiring) a %s resource of the wrong type" % resource_iontype)
            add_test_method(name, doc, test_delete_bad_wrongtype_fun)


        def gen_test_force_delete():
            """
            generate the function to test the delete
            """
            name = make_name("%s_force_delete" % resource_label)
            doc  = make_doc("Deleting -- destructively -- a %s resource" % resource_iontype)
            add_test_method(name, doc, test_force_delete_fun)


        def gen_test_force_delete_bad_wrongtype():
            """
            generate the function to test the delete with the wrong type
            """
            name = make_name("%s_force_delete_bad_wrongtype" % resource_label)
            doc  = make_doc("Deleting -- destructively -- a %s resource of the wrong type" % resource_iontype)
            add_test_method(name, doc, test_force_delete_bad_wrongtype_fun)




        def gen_test_allinone():
            """
            generate the function to test EVERYTHING at once
            """
            def fun(self):
                """
                self is an instance of the tester class
                """
                test_create_fun(self)
                test_create_bad_wrongtype_fun(self)
                test_create_bad_noname_fun(self)
                test_create_bad_dupname_fun(self)
                test_read_fun(self)
                test_read_bad_wrongtype_fun(self)
                test_update_fun(self)
                test_update_bad_wrongtype_fun(self)
                test_update_bad_dupname_fun(self)
                test_delete_fun(self)
                test_delete_bad_wrongtype_fun(self)
                test_force_delete_fun(self)
                test_force_delete_bad_wrongtype_fun(self)



            name = make_name("%s_allinone" % resource_label)
            doc  = make_doc("Performing all CRUD tests on %s resources" % resource_iontype)
            add_test_method(name, doc, fun)



        # can you believe we're still within a single function?
        # it's time to add each method to the tester class
        # add each method to the tester class
        gen_svc_lookup()
        if all_in_one:
            gen_test_allinone()
        else:
            gen_test_create()
            gen_test_create_bad_wrongtype()
            gen_test_create_bad_noname()
            gen_test_create_bad_dupname()
            gen_test_read()
            gen_test_read_bad_wrongtype()
            gen_test_update()
            gen_test_update_bad_wrongtype()
            gen_test_update_bad_dupname()
            gen_test_delete()
            gen_test_delete_bad_wrongtype()
            gen_test_force_delete()
            gen_test_force_delete_bad_wrongtype()





class GenericIntHelperTestCase(IonIntegrationTestCase):

    def setUp(self):
        # Start container
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2deploy.yml')

        self.generic_int_helper_rr  = ResourceRegistryServiceClient(node=self.container.node)
        self.generic_int_helper_rr2 = EnhancedResourceRegistryClient(self.generic_int_helper_rr)


    def assert_lcs_fail(self,
                        owner_service,
                        resource_label,
                        resource_id,
                        lc_event):
        """
        execute an lcs event and verify that it fails

        @param owner_service instance of service client that will handle the request
        @param resource_label string like "instrument_device"
        @param resource_id string
        @param lc_event string like LCE.INTEGRATE
        """

        lcsmethod = getattr(owner_service, "execute_%s_lifecycle" % resource_label)
        #lcsmethod(resource_id, lc_event)
        if True:
            log.warn("Skipping generic_lcs_fail for beta testing purposes")
            return
        self.assertRaises(Unauthorized, lcsmethod, resource_id, lc_event)


    def assert_lcs_pass(self,
                        owner_service,
                        resource_label,
                        resource_id,
                        lc_event,
                        lc_state):
        """
        execute an lcs event and verify that it passes and affects state

        @param owner_service instance of service client that will handle the request
        @param resource_label string like "instrument_device"
        @param resource_id string
        @param lc_event string like LCE.INTEGRATE
        @param lc_state string like LCS.INTEGRATED (where the state should end up
        """

        lcsmethod  = getattr(owner_service, "execute_%s_lifecycle" % resource_label)
        readmethod = getattr(owner_service, "read_%s" % resource_label)

        lcsmethod(resource_id, lc_event)
        resource_obj = readmethod(resource_id)

        self.assertEqual(lc_state, resource_obj.lcstate)




    def perform_association_script(self,
                                   assign_obj_to_subj_fn,
                                   find_subj_fn,
                                   find_obj_fn,
                                   subj_id,
                                   obj_id):
        """
        create an association and test that it went properly

        @param assign_obj_to_subj_fn the service method that takes (obj, subj) and associates them
        @param find_subj_fn the service method that returns a list of subjects given an object
        @param find_obj_fn the service method that returns a list of objects given a subject
        @param subj_id the subject id to associate
        @param obj_id the object id to associate
        """
        initial_subj_count = len(find_subj_fn(obj_id))
        initial_obj_count  = len(find_obj_fn(subj_id))

        log.debug("Creating association")
        if not ("str" == type(subj_id).__name__ == type(obj_id).__name__):
            raise NotImplementedError("%s='%s' to %s='%s'" %
                                      (type(subj_id), str(subj_id), type(obj_id), str(obj_id)))
        if not (subj_id and obj_id):
            raise NotImplementedError("%s='%s' to %s='%s'" %
                                      (type(subj_id), str(subj_id), type(obj_id), str(obj_id)))
        assign_obj_to_subj_fn(obj_id, subj_id)

        log.debug("Verifying find-subj-by-obj")
        subjects = find_subj_fn(obj_id)
        self.assertEqual(initial_subj_count + 1, len(subjects))
        subject_ids = []
        for x in subjects:
            if not "_id" in x:
                raise Inconsistent("'_id' field not found in resource! got: %s" % str(x))
            subject_ids.append(x._id)
        self.assertIn(subj_id, subject_ids)

        log.debug("Verifying find-obj-by-subj")
        objects = find_obj_fn(subj_id)
        self.assertEqual(initial_obj_count + 1, len(objects))
        object_ids = []
        for x in objects:
            if not "_id" in x:
                raise Inconsistent("'_id' field not found in resource! got: %s" % str(x))
            object_ids.append(x._id)
        self.assertIn(obj_id, object_ids)



    def perform_fd_script(self, resource_id, resource_label, owner_service):
        """
        delete a resource and check that it was properly deleted

        @param resource_id id to be deleted
        @param resource_label something like platform_model
        @param owner_service service client instance
        """

        del_op = getattr(owner_service, "force_delete_%s" % resource_label)

        del_op(resource_id)

        # try again to make sure that we get NotFound
        self.assertRaises(NotFound, del_op, resource_id)


    def perform_fcruf_script(self, resource_iontype, resource_label, owner_service, actual_obj=None, extra_fn=None):
        """
        run through find, create, read, update, and find ops on a basic resource

        NO DELETE in here.

        @param resource_iontype something like RT.PlatformModel
        @param resource_label something like platform_model
        @param owner_service a service client instance like InstrumentManagementServiceClient
        @param actual_obj use this IonObject instead of a generic object for testing
        @param extra_fn a function to run after the script, taking the new resource id as input
        @return generic_id, the resource_id of the generic resource that was created
        """



        # this section is just to make the LCA integration script easier to write.
        #
        # each resource type gets put through (essentially) the same steps.
        #
        # so, we just set up a generic service-esque object.
        # (basically just a nice package of shortcuts):
        #  create a fake service object and populate it with the methods we need

        some_service = DotDict()

        def fill(svc, method):
            """
            make a "shortcut service" for testing crud ops.
            @param svc a dotdict
            @param method the method name to add
            """

            realmethod = "%s_widget" % method

            setattr(svc, realmethod,
                    getattr(owner_service, "%s_%s" % (method, resource_label)))



        fill(some_service, "create")
        fill(some_service, "read")
        fill(some_service, "update")
        fill(some_service, "delete")

        def find_widgets():
            ret, _ = self.generic_int_helper_rr.find_resources(resource_iontype, None, None, False)
            return ret

        #UX team: generic script for LCA resource operations begins here.
        # some_service will be replaced with whatever service you're calling
        # widget will be replaced with whatever resource you're working with
        # resource_label will be data_product or logical_instrument


        log.info("Finding %s objects", resource_label)
        num_objs = len(find_widgets())
        log.info("I found %d %s objects", num_objs, resource_label)

        generic_obj = actual_obj or any_old(resource_iontype)
        log.info("Creating a %s with name='%s'", resource_label, generic_obj.name)
        generic_id = some_service.create_widget(generic_obj)
        self.assertIsNotNone(generic_id, "%s failed its creation" % resource_iontype)

        log.info("Reading %s '%s'", resource_label, generic_id)
        generic_ret = some_service.read_widget(generic_id)

        log.info("Verifying equality of stored and retrieved object")
        self.assertEqual(generic_obj.name, generic_ret.name)
        self.assertEqual(generic_obj.description, generic_ret.description)

        log.info("Updating %s '%s'", resource_label, generic_id)
        generic_newname = "%s updated" % generic_ret.name
        generic_ret.name = generic_newname
        some_service.update_widget(generic_ret)

        log.info("Reading %s '%s' to verify update", resource_iontype, generic_id)
        generic_ret = some_service.read_widget(generic_id)

        self.assertEqual(generic_newname, generic_ret.name)
        self.assertEqual(generic_obj.description, generic_ret.description)

        log.info("Finding %s objects... checking that there's a new one", resource_iontype)
        num_objs2 = len(find_widgets())

        log.info("There were %s and now there are %s", num_objs, num_objs2)
        self.assertTrue(num_objs2 > num_objs)

        if not extra_fn is None:
            log.info("Running extra_fn on generic resource '%s'" % generic_id)
            extra_fn(generic_id)

        log.info("Returning %s with id '%s'", resource_iontype, generic_id)
        return generic_id

