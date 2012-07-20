#!/usr/bin/env python

"""
@file ion/services/sa/resource_impl/resource_impl_metatest.py
@author Ian Katz

"""
import hashlib

# from mock import Mock, sentinel, patch
from pyon.core.bootstrap import IonObject
from pyon.core.exception import BadRequest #, NotFound
#from pyon.public import LCS
from unittest import SkipTest
from pyon.ion.resource import LCE

class ResourceImplMetatest(object):
    """
    This function adds test methods for CRUD and associations in the given 
    resource impl class.

    For example, OUTSIDE and AFTER the TestInstrumentManagement class, write this:

        rim = ResourceImplMetatest(TestInstrumentManagement,
                                     InstrumentManagementService,
                                     log)

        rim.add_resource_impl_unittests(InstrumentAgentInstanceImpl,
                                          {"exchange_name": "rhubarb"}

    The impl object MUST be available as a class variable in the service under test!

    """

    def __init__(self, resource_tester_class, service_under_test_class, log):
        """
        @param resource_tester_class the class that will perform the setup/ testing
        @param service_under_test_class the class of the service that's being tested
        @param log the log object 
        """

        self.tester_class = resource_tester_class
        
        #create the service
        self.service_instance  = service_under_test_class()
        self.service_instance.on_init()
        
        #save the log object (maybe don't need this)
        self.log      = log


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
            self.log.debug("Creating sample %s" % iontype)
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

    def find_impl_attribute(self, impl):
        """
        determine which class variable in the service is the impl class
        @param impl an instance of the impl
        """
        impl_attr = self.find_class_variable_name(self.service_instance, type(impl))

        assert impl_attr

        # write a message to myself that will appear in the description of a failed test
        #self.sample_resource_extras += " found at self.%s" % impl_attr

        return impl_attr

        
    def add_resource_impl_unittests(self,
                                      resource_impl_class, 
                                      resource_params=None):
        """
        Add tests for the resorce_impl_class to the (self.)resource_tester_class

        @param resource_impl_class the class of the resource impl you want tested
        @param resource_params dictionary of extra params to add to the sample resource

        this function will be huge.  it is a list of smaller functions that are templates
         for tests of various resource_impl class functionality.  the functions are given
         proper references to member variables in the service and test class, then injected
         into the test class itself.

        """

        #init default args
        if None == resource_params: resource_params = {}

        # create a impl class, no clients
        impl_instance = resource_impl_class([])

        self.build_test_descriptors(resource_params)

        impl_attr = self.find_impl_attribute(impl_instance)

        #this is convoluted but it helps me debug by
        #  being able to inject text into the sample_resource_extras
        sample_resource = self.sample_resource_factory(impl_instance.iontype, resource_params)

        all_in_one = self.all_in_one

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
            return "%s_%s%s" % (impl_instance.iontype, name, self.sample_resource_md5)

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

            add_new_method("_rim_cleanup_%s%s" % (impl_instance.iontype, self.sample_resource_md5), "Cleanup", fun)

            # add (probably overwrite) the function to process all the cleanups
            def whole_cleanup(instance):
                for k in dir(instance):
                    if -1 < k.find("_rim_cleanup_"):
                        cleanup_fn = getattr(instance, k)
                        cleanup_fn()

            #if hasattr(self.tester_class, "resource_impl_cleanup")
            add_new_method("resource_impl_cleanup", "Global cleanup", whole_cleanup)

        def gen_svc_lookup():
            """
            put a new method in the tester class to
            determine which class variable in the tester class is the service being tested

            the prefix is "_rim_": resource_impl_metatest
            """
            def fun(self):
                """
                self is an instance of the tester class
                """
                if not hasattr(self, "_rim_service_obj"):
                    service_itself = getattr(self, find_cv_func(self, service_type))
                    self._rim_service_obj = service_itself
                    assert self._rim_service_obj

                return self._rim_service_obj

            if not hasattr(self.tester_class, "_rim_getservice"):
                add_new_method("_rim_getservice", "Finds resource registry", fun)

        # def gen_ionobj_lookup():
        #     """
        #     put a new method in the tester class to
        #     get access to the test class's ionobject
        #     """
        #     def fun(self):
        #         """
        #         self is an instance of the tester class
        #         """
        #         if not hasattr(self, "_rim_mock_ionobj"):
        #             self._rim_mock_ionobj = self._create_IonObject_mock(
        #                 service_type.__name__ + '.IonObject')
        #             assert(self._rim_mock_ionobj)

        #         return self._rim_mock_ionobj

        #     if not hasattr(self.tester_class, "_rim_ionobject"):
        #         add_new_method("_rim_ionobject", "Mock Ionobject", fun)

        # def gen_sample_resource():
        #     """
        #     put a new method in the tester class to
        #     produce a sample resource
        #     """
        #     def fun(self):
        #         """
        #         self is an instance of the tester class
        #         """
        #         # sample resource to use for our tests
        #         sample_resource = Mock()
        #         sample_resource.name = "sample %s" % impl_instance.iontype
        #         sample_resource.description = "description of sample %s" % impl_instance.iontype

        #         # build out strings if there are more params
        #         if resource_params:
        #             extras = []
        #             for k, v in resource_params.iteritems():
        #                 setattr(sample_resource, k, v)

        #                 self.sample_resource         = sample_resource
        #                 self.sample_resource_extras  = sample_resource_extras
        #                 self.sample_resource_md5     = sample_resource_md5

        #         return sample_resource

        def test_create_fun(self):
            """
            self is an instance of the tester class
            """
            # get objects
            svc = self._rim_getservice()
            myimpl = getattr(svc, impl_attr)
            good_sample_resource = sample_resource()
            saved_resource = sample_resource()
            #saved_resource.lcstate = LCS.REGISTERED

            #configure Mock
            svc.clients.resource_registry.create.return_value = ('111', 'bla')
            svc.clients.resource_registry.find_resources.return_value = ([], [])
            svc.clients.resource_registry.read.return_value = saved_resource

            sample_resource_id = myimpl.create_one(good_sample_resource)

            svc.clients.resource_registry.create.assert_called_once_with(good_sample_resource)
            self.assertEqual(sample_resource_id, '111')

            if all_in_one: svc.clients.resource_registry.reset_mock()


        def test_create_bad_noname_fun(self):
            """
            self is an instance of the tester class
            """
            # get objects
            svc = self._rim_getservice()
            myimpl = getattr(svc, impl_attr)
            bad_sample_resource = sample_resource()
            delattr(bad_sample_resource, "name")

            #configure Mock
            svc.clients.resource_registry.create.return_value = ('111', 'bla')
            svc.clients.resource_registry.find_resources.return_value = ([], [])

            self.assertRaises(BadRequest, myimpl.create_one, bad_sample_resource)

            if all_in_one: svc.clients.resource_registry.find_resources.reset_mock()

        def test_create_bad_dupname_fun(self):
            """
            self is an instance of the tester class
            """
            # get objects
            svc = self._rim_getservice()
            myimpl = getattr(svc, impl_attr)
            bad_sample_resource = sample_resource()
            #really, the resource doesn't matter; it's the retval from find that matters

            #configure Mock
            svc.clients.resource_registry.create.return_value = ('111', 'bla')
            svc.clients.resource_registry.find_resources.return_value = ([0], [0])

            self.assertRaises(BadRequest, myimpl.create_one, bad_sample_resource)

            if all_in_one: svc.clients.resource_registry.find_resources.reset_mock()

        def test_read_fun(self):
            """
            self is an instance of the tester class
            """
            # get objects
            svc = self._rim_getservice()
            myimpl = getattr(svc, impl_attr)
            myret = sample_resource()

            #configure Mock
            svc.clients.resource_registry.read.return_value = myret

            response = myimpl.read_one("111")
            svc.clients.resource_registry.read.assert_called_once_with("111", "")
            self.assertEqual(response, myret)
            #self.assertDictEqual(response.__dict__,
            #                     sample_resource().__dict__)

            if all_in_one: svc.clients.resource_registry.reset_mock()

        def test_update_fun(self):
            """
            self is an instance of the tester class
            """
            # get objects
            svc = self._rim_getservice()
            myimpl = getattr(svc, impl_attr)
            good_sample_resource = sample_resource()
            setattr(good_sample_resource, "_id", "111")

            #configure Mock
            svc.clients.resource_registry.update.return_value = ('111', 'bla')
            svc.clients.resource_registry.find_resources.return_value = ([], [])

            myimpl.update_one(good_sample_resource)

            svc.clients.resource_registry.update.assert_called_once_with(good_sample_resource)

            if all_in_one: svc.clients.resource_registry.find_resources.reset_mock()

        def test_update_bad_dupname_fun(self):
            """
            self is an instance of the tester class
            """
            # get objects
            svc = self._rim_getservice()
            myimpl = getattr(svc, impl_attr)
            bad_sample_resource = sample_resource()
            setattr(bad_sample_resource, "_id", "111")

            svc.clients.resource_registry.find_resources.return_value = ([0], [0])
            self.assertRaises(BadRequest, myimpl.update_one, bad_sample_resource)

            if all_in_one: svc.clients.resource_registry.find_resources.reset_mock()

        def test_delete_fun(self):
            """
            self is an instance of the tester class
            """
            # get objects
            svc = self._rim_getservice()
            myimpl = getattr(svc, impl_attr)
            myret = sample_resource()

            #configure Mock
            svc.clients.resource_registry.read.return_value = myret
            svc.clients.resource_registry.delete.return_value = None

            try:
                myimpl.delete_one("111")
            except TypeError as te:
                # for logic tests that run into mock trouble
                if "'Mock' object is not iterable" != te.message:
                    raise te
                elif all_in_one:
                    svc.clients.resource_registry.reset_mock()
                    return
                else:
                    raise SkipTest("Must test this with INT test")
            except Exception as e:
                raise e

            svc.clients.resource_registry.read.assert_called_with("111", "")
            svc.clients.resource_registry.execute_lifecycle_transition.assert_called_once_with("111", LCE.RETIRE)

            if all_in_one: svc.clients.resource_registry.delete.reset_mock()

        def test_delete_destroy_fun(self):
            """
            self is an instance of the tester class
            """
            # get objects
            svc = self._rim_getservice()
            myimpl = getattr(svc, impl_attr)
            myret = sample_resource()

            #configure Mock
            svc.clients.resource_registry.read.return_value = myret
            svc.clients.resource_registry.delete.return_value = None

            try:
                myimpl.delete_one("111", True)
            except TypeError as te:
                # for logic tests that run into mock trouble
                if "'Mock' object is not iterable" != te.message:
                    raise te
                elif all_in_one:
                    svc.clients.resource_registry.reset_mock()
                    return
                else:
                    raise SkipTest("Must test this with INT test")
            except Exception as e:
                raise e


            svc.clients.resource_registry.read.assert_called_with("111", "")
            svc.clients.resource_registry.delete.assert_called_once_with("111")

            if all_in_one: svc.clients.resource_registry.delete.reset_mock()

        def test_find_fun(self):
            """
            self is an instance of the tester class
            """
            # get objects
            svc = self._rim_getservice()
            myimpl = getattr(svc, impl_attr)

            #configure Mock
            svc.clients.resource_registry.find_resources.return_value = ([0], [0])

            response = myimpl.find_some({})
            self.assertIsInstance(response, list)
            self.assertNotEqual(0, len(response))
            svc.clients.resource_registry.find_resources.assert_called_once_with(impl_instance.iontype,
                                                                                 None,
                                                                                 None,
                                                                                 False)
            if all_in_one: svc.clients.find_resources.resource_registry.reset_mock()


        def test_find_having_freeze(find_name):
            """
            must freeze this so the loop doesn't overwrite the parts varible
            """

            def fun(self):
                svc = self._rim_getservice()
                myimpl = getattr(svc, impl_attr)
                myfind = getattr(myimpl, find_name)

                #set up Mock
                reply = (['333'], ['444'])
                svc.clients.resource_registry.find_subjects.return_value = reply

                #call the impl
                response = myfind("111")
                self.assertEqual(response, ['333'])

                if all_in_one: svc.clients.resource_registry.find_subjects.reset_mock()

            return fun

        def test_find_stemming_freeze(find_name):

            def fun(self):
                svc = self._rim_getservice()
                myimpl = getattr(svc, impl_attr)
                myfind = getattr(myimpl, find_name)

                #set up Mock
                reply = (['333'], ['444'])
                fo = svc.clients.resource_registry.find_objects
                fo.return_value = reply

                #call the impl
                response = myfind("111")
                self.assertEqual(response, fo.call_count * ['333'])

                if all_in_one: svc.clients.resource_registry.find_objects.reset_mock()

            return fun

        def test_links_freeze(link_name):

            def fun(self):

                svc = self._rim_getservice()
                myimpl = getattr(svc, impl_attr)
                mylink = getattr(myimpl, link_name)

                #set up Mock
                find_reply = ([], []) #for exclusive associations
                svc.clients.resource_registry.find_subjects.return_value = find_reply
                svc.clients.resource_registry.find_objects.return_value = find_reply
                svc.clients.resource_registry.find_associations.return_value = find_reply

                reply = ('333', "trying %s %s" % (impl_attr, link_name))
                svc.clients.resource_registry.create_association.return_value = reply

                #call the impl
                try:
                    response = mylink("111", "222")
                    self.assertEqual(reply, response)
                except BadRequest:
                    pass # assumed to be a problem with a precondition check
                except Exception as e:
                    raise e

                if all_in_one: svc.clients.resource_registry.reset_mock()

            return fun

        def test_unlinks_freeze(link_name):

            def fun(self):

                svc = self._rim_getservice()
                myimpl = getattr(svc, impl_attr)
                myunlink = getattr(myimpl, link_name)

                svc.clients.resource_registry.find_associations.return_value = ([], [])

                #call the impl
                myunlink("111", "222")

                #there is no response, self.assertEqual("f", str(response))
                
                if all_in_one: svc.clients.resource_registry.reset_mock()

            return fun


        def gen_test_create():
            """
            generate the function to test the create
            """
            name = make_name("resource_impl_create")
            doc  = make_doc("Creation of a new %s resource" % impl_instance.iontype)
            add_test_method(name, doc, test_create_fun)


        def gen_test_create_bad_noname():
            """
            generate the function to test the create in a bad case
            """
            name = make_name("resource_impl_create_bad_noname")
            doc  = make_doc("Creation of a (bad) new %s resource (no name)" % impl_instance.iontype)
            add_test_method(name, doc, test_create_bad_noname_fun)


        def gen_test_create_bad_dupname():
            """
            generate the function to test the create in a bad case where the name already exists
            """
            name = make_name("resource_impl_create_bad_dupname")
            doc  = make_doc("Creation of a (bad) new %s resource (duplicate name)" % impl_instance.iontype)
            add_test_method(name, doc, test_create_bad_dupname_fun)


        def gen_test_read():
            """
            generate the function to test the read
            """
            name = make_name("resource_impl_read")
            doc  = make_doc("Reading a %s resource" % impl_instance.iontype)
            add_test_method(name, doc, test_read_fun)



        def gen_test_update():
            """
            generate the function to test the create
            """
            name = make_name("resource_impl_update")
            doc  = make_doc("Updating a %s resource" % impl_instance.iontype)
            add_test_method(name, doc, test_update_fun)


        def gen_test_update_bad_dupname():
            """
            generate the function to test the create
            """
            name = make_name("resource_impl_update_bad_duplicate")
            doc  = make_doc("Updating a %s resource to a duplicate name" % impl_instance.iontype)
            add_test_method(name, doc, test_update_bad_dupname_fun)


        def gen_test_delete():
            """
            generate the function to test the delete
            """

            name = make_name("resource_impl_delete")
            doc  = make_doc("Deleting (retiring) a %s resource" % impl_instance.iontype)
            add_test_method(name, doc, test_delete_fun)

        def gen_test_delete_destroy():
            """
            generate the function to test the delete
            """

            name = make_name("resource_impl_delete_destroy")
            doc  = make_doc("Deleting -- destructively -- a %s resource" % impl_instance.iontype)
            add_test_method(name, doc, test_delete_destroy_fun)


        def gen_test_find():
            """
            generate the function to test the find op
            """
            name = make_name("resource_impl_find")
            doc  = make_doc("Finding (all) %s resources" % impl_instance.iontype)
            add_test_method(name, doc, test_find_fun)


        def gen_tests_associated_finds():
            gen_tests_find_having()
            gen_tests_find_stemming()


        def gen_tests_find_having():
            """
            create a test for each of the find_having_* methods in the impl
            """
            for k in dir(impl_instance):
                parts = k.split("_", 2)
                if "find" == parts[0] and "having" == parts[1]:
                    def freeze(parts_):
                        assn_type = parts_[2]
                        name = make_name("resource_impl_find_having_%s_link" % assn_type)
                        doc  = make_doc("Checking find %s having %s" % (impl_instance.iontype, assn_type))
                        add_test_method(name, doc, test_find_having_freeze(k))

                    freeze(parts)


        def gen_tests_find_stemming():
            """
            create a test for each of the find_stemming_* methods in the impl
            """
            for k in dir(impl_instance):
                parts = k.split("_", 2)
                if "find" == parts[0] and "stemming" == parts[1]:
                    def freeze(parts_):
                        """
                        must freeze this so the loop doesn't overwrite the parts varible
                        """
                        assn_type = parts_[2]
                        name = make_name("resource_impl_find_stemming_%s_links" % assn_type)
                        doc  = make_doc("Checking find %s stemming from %s" % (assn_type, impl_instance.iontype))
                        add_test_method(name, doc, test_find_stemming_freeze(k))

                    freeze(parts)



        def gen_tests_associations():
            gen_tests_links()
            gen_tests_unlinks()

        def gen_tests_links():
            """
            create a test for each of the create_association tests in the impl
            """
            for k in dir(impl_instance):
                parts = k.split("_", 1)
                if "link" == parts[0]:
                    def freeze(parts_):
                        """
                        must freeze this so the loop doesn't overwrite the parts varible
                        """
                        assn_type = parts_[1]
                        name = make_name("resource_impl_association_%s_link" % assn_type)
                        doc  = make_doc("Checking create_association of a %s resource with its %s" % (impl_instance.iontype, assn_type))
                        add_test_method(name, doc, test_links_freeze(k))

                    freeze(parts)


        def gen_tests_unlinks():
            """
            create a test for each of the delete_association tests in the impl
            """
            for k in dir(impl_instance):
                parts = k.split("_", 1)
                if "unlink" == parts[0]:

                    def freeze(parts_):
                        """
                        must freeze this so the loop doesn't overwrite the parts varible
                        """
                        assn_type = parts_[1]
                        name = make_name("resource_impl_association_%s_unlink" % assn_type)
                        doc  = make_doc("Checking delete_association of a %s resource from its %s" % (impl_instance.iontype, assn_type))
                        add_test_method(name, doc, test_unlinks_freeze(k))

                    freeze(parts)


        def gen_test_allinone():
            """
            generate the function to test EVERYTHING at once
            """
            def fun(self):
                """
                self is an instance of the tester class
                """
                test_create_fun(self)
                test_create_bad_noname_fun(self)
                test_create_bad_dupname_fun(self)
                test_read_fun(self)
                test_update_fun(self)
                test_update_bad_dupname_fun(self)
                #test_delete_fun(self)
                #test_delete_destroy_fun(self)
                test_find_fun(self)

                for k in dir(impl_instance):
                    parts = k.split("_", 2)
                    if "find" == parts[0] and "having" == parts[1]:
                        test_find_having_freeze(k)(self)

                    if "find" == parts[0] and "stemming" == parts[1]:
                        test_find_stemming_freeze(k)(self)

                    if "link" == parts[0]:
                        test_links_freeze(k)(self)

                    if "unlink" == parts[0]:
                        test_unlinks_freeze(k)(self)

            name = make_name("resource_impl_allinone")
            doc  = make_doc("Performing all CRUD tests on %s resources" % impl_instance.iontype)
            add_test_method(name, doc, fun)



        # can you believe we're still within a single function?
        # it's time to add each method to the tester class
        # add each method to the tester class
        gen_svc_lookup()
        if self.all_in_one:
            gen_test_allinone()
        else:
            gen_test_create()
            gen_test_create_bad_noname()
            gen_test_create_bad_dupname()
            gen_test_read()
            gen_test_update()
            gen_test_update_bad_dupname()
            #gen_test_delete()
            #gen_test_delete_destroy()
            gen_test_find()
            gen_tests_associations()
            gen_tests_associated_finds()
        #gen_svc_cleanup()
