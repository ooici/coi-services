#!/usr/bin/env python

"""
@file ion/services/sa/resource_impl/resource_impl_metatest_integration.py
@author Ian Katz

"""
import hashlib

from pyon.core.bootstrap import IonObject
from pyon.core.exception import BadRequest, NotFound

from ion.services.sa.resource_impl.resource_impl_metatest import ResourceImplMetatest

from pyon.util.log import log

class ResourceImplMetatestIntegration(ResourceImplMetatest):
    """
    This function adds integration test methods for CRUD and associations in the given 
    resource impl class.

    For example, OUTSIDE and AFTER the TestInstrumentManagement class, write this:

        rimi = ResourceImplMetatestIntegration(TestInstrumentManagement,
                                               InstrumentManagementService,
                                               log)

        rimi.add_resource_impl_inttests(InstrumentAgentInstanceImpl,
                                          {"exchange_name": "rhubarb"}

    The impl object MUST be available as a class variable in the service under test!

    """

        
    def add_resource_impl_inttests(self,
                                   resource_impl_class, 
                                   resource_params):
        """
        Add tests for the resorce_impl_class to the (self.)resource_tester_class

        @param resource_impl_class the class of the resource impl you want tested
        @param resource_params dictionary of extra params to add to the sample resource

        this function will be huge.  it is a list of smaller functions that are templates
         for tests of various resource_impl class functionality.  the functions are given
         proper references to member variables in the service and test class, then injected
         into the test class itself.
        
        """
        # create a impl class, no clients
        impl_instance       = resource_impl_class([])

        self.build_test_descriptors(resource_params)

        impl_attr  = self.find_impl_attribute(impl_instance)

        #this is convoluted but it helps me debug by 
        #  being able to inject text into the sample_resource_extras
        sample_resource = self.sample_resource_factory(impl_instance, resource_params)


        service_type = type(self.service_instance)

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
            return "int_%s_%s%s" % (impl_instance.iontype, name, self.sample_resource_md5)
        
        def make_doc(doc):
            """
            make a good doc string for a test from by including the extra params
            @param doc the base string for the descripton
            """
            return "Integration: %s %s" % (doc, self.sample_resource_extras)

            
        def gen_svc_lookup():
            """
            put a new method in the tester class to
            determine which class variable in the tester class is the service being tested
            """
            def fun(self):
                """
                self is an instance of the tester class
                """
                if not hasattr(self, "_rimi_service_obj"):

                    # get service from container proc manager
                    relevant_services = [
                        item[1] for item in self.container.proc_manager.procs.items() 
                        if type(item[1]) == service_type
                        ]

                    assert (0 < len(relevant_services)), \
                        "no services of type '%s' found running in container!" % service_type
                        

                    service_itself = relevant_services[0]
                    self._rimi_service_obj = service_itself
                    assert(self._rimi_service_obj)

                return self._rimi_service_obj

            if not hasattr(self.tester_class, "_rimi_getservice"): 
                add_new_method("_rimi_getservice", "Finds the embedded service", fun)



        # TEST CASES GO BELOW HERE



        def gen_test_create():
            """
            generate the function to test the create
            """
            def fun(self):
                """
                self is an instance of the tester class
                """
                # get objects
                svc = self._rimi_getservice()
                myimpl = getattr(svc, impl_attr)                 
                good_sample_resource = sample_resource()
                

                sample_resource_id = myimpl.create_one(good_sample_resource)


                log.debug("got resource id: %s" % sample_resource_id)

            name = make_name("resource_impl_create")
            doc  = make_doc("Creation of a new %s resource" % impl_instance.iontype)
            add_test_method(name, doc, fun)



        def gen_test_create_bad_noname():
            """
            generate the function to test the create in a bad case
            """
            def fun(self):
                """
                self is an instance of the tester class
                """
                # get objects
                svc = self._rimi_getservice()
                myimpl = getattr(svc, impl_attr)                 
                bad_sample_resource = sample_resource()
                delattr(bad_sample_resource, "name")
                

                self.assertRaises(BadRequest, myimpl.create_one, bad_sample_resource)


            name = make_name("resource_impl_create_bad_noname")
            doc  = make_doc("Creation of a (bad) new %s resource (no name)" % impl_instance.iontype)
            add_test_method(name, doc, fun)


        def gen_test_create_bad_dupname():
            """
            generate the function to test the create in a bad case where the name already exists
            """
            def fun(self):
                """
                self is an instance of the tester class
                """
                # get objects
                svc = self._rimi_getservice()
                myimpl = getattr(svc, impl_attr)                 
                                
                # prep and put objects
                good_sample_resource = sample_resource()
               
                #insert 2
                myimpl.create_one(good_sample_resource)
                self.assertRaises(BadRequest, myimpl.create_one, good_sample_resource)


            name = make_name("resource_impl_create_bad_dupname")
            doc  = make_doc("Creation of a (bad) new %s resource (duplicate name)" % impl_instance.iontype)
            add_test_method(name, doc, fun)



        def gen_test_create_bad_has_id():
            """
            generate the function to test the create in a bad case
            """
            def fun(self):
                """
                self is an instance of the tester class
                """
                # get objects
                svc = self._rimi_getservice()
                myimpl = getattr(svc, impl_attr)                 
                bad_sample_resource = sample_resource()
                setattr(bad_sample_resource, "_id", "12345")
                
                self.assertRaises(BadRequest, myimpl.create_one, bad_sample_resource)


            name = make_name("resource_impl_create_bad_has_id")
            doc  = make_doc("Creation of a (bad) new %s resource (has _id)" % impl_instance.iontype)

            add_test_method(name, doc, fun)



        def gen_test_read():
            """
            generate the function to test the read
            """
            def fun(self):
                """
                self is an instance of the tester class
                """
                # get objects
                svc = self._rimi_getservice()
                myimpl = getattr(svc, impl_attr)                 
                                
                # put in an object
                sample_resource_id = myimpl.create_one(sample_resource())

                returned_resource = myimpl.read_one(sample_resource_id)

                #won't work because of changes in _rev and lcstate
                #self.assertDictEqual(returned_resource.__dict__,
                #                     sample_resource().__dict__)

                self.assertEqual(returned_resource._id,
                                 sample_resource_id)
                
            name = make_name("resource_impl_read")
            doc  = make_doc("Reading a %s resource" % impl_instance.iontype)
            add_test_method(name, doc, fun)


        def gen_test_read_notfound():
            """
            generate the function to test the read in a not-found case
            """
            def fun(self):
                """
                self is an instance of the tester class
                """
                # get objects
                svc = self._rimi_getservice()
                myimpl = getattr(svc, impl_attr)                 
                                
                self.assertRaises(NotFound, myimpl.read_one, "0000")
            
            name = make_name("resource_impl_read_notfound")
            doc  = make_doc("Reading a %s resource that doesn't exist" % impl_instance.iontype)
            add_test_method(name, doc, fun)

        def gen_test_update():
            gen_test_update_samename()
            gen_test_update_differentname()

        def gen_test_update_samename():
            """
            generate the function to test the update, but use the same name
            """
            def fun(self):
                """
                self is an instance of the tester class
                """
                # get objects
                svc = self._rimi_getservice()
                myimpl = getattr(svc, impl_attr)                 
                                
                # prep and put objects
                good_sample_resource = sample_resource()
                res_id = myimpl.create_one(good_sample_resource)

                # read and change
                good_sample_duplicate = myimpl.read_one(res_id)
                myimpl.update_one(good_sample_duplicate)

                # verify change
                good_sample_triplicate = myimpl.read_one(res_id)
                self.assertEqual(good_sample_duplicate.name, good_sample_triplicate.name)

                
            name = make_name("resource_impl_update_samename")
            doc  = make_doc("Updating a %s resource keeping name the same" % impl_instance.iontype)
            add_test_method(name, doc, fun)


        def gen_test_update_differentname():
            """
            generate the function to test the update, use a new name
            """
            def fun(self):
                """
                self is an instance of the tester class
                """
                # get objects
                svc = self._rimi_getservice()
                myimpl = getattr(svc, impl_attr)                 
                                
                # prep and put objects
                good_sample_resource = sample_resource()
                res_id = myimpl.create_one(good_sample_resource)

                # read and change
                good_sample_duplicate = myimpl.read_one(res_id)
                newname = "updated %s" % good_sample_duplicate.name
                good_sample_duplicate.name = newname
                myimpl.update_one(good_sample_duplicate)

                # verify change
                good_sample_triplicate = myimpl.read_one(res_id)
                self.assertEqual(newname, good_sample_triplicate.name)

                
            name = make_name("resource_impl_update_differentname")
            doc  = make_doc("Updating a %s resource to have a different name" % impl_instance.iontype)
            add_test_method(name, doc, fun)


        def gen_test_update_bad_noid():
            """
            generate the function to test the create
            """
            def fun(self):
                """
                self is an instance of the tester class
                """
                # get objects
                svc = self._rimi_getservice()
                myimpl = getattr(svc, impl_attr)                 
                bad_sample_resource = sample_resource()
                
                self.assertRaises(BadRequest, myimpl.update_one, bad_sample_resource)

                
            name = make_name("resource_impl_update_bad_no_id")
            doc  = make_doc("Updating a %s resource without an ID" % impl_instance.iontype)
            add_test_method(name, doc, fun)


        def gen_test_update_bad_dupname():
            """
            generate the function to test the create
            """
            def fun(self):
                """
                self is an instance of the tester class
                """
                # get objects
                svc = self._rimi_getservice()
                myimpl = getattr(svc, impl_attr)                 
                                
                # prep and put objects
                good_sample_resource = sample_resource()
                good_sample_duplicate = sample_resource()
                
                oldname = good_sample_resource.name
                good_sample_duplicate.name = "DEFINITELY NOT A DUPLICATE"

                myimpl.create_one(good_sample_resource)
                dup_id = myimpl.create_one(good_sample_duplicate)
                
                good_sample_duplicate = myimpl.read_one(dup_id)
                good_sample_duplicate.name = oldname
                
                self.assertRaises(BadRequest, myimpl.update_one, good_sample_duplicate)

                
            name = make_name("resource_impl_update_bad_duplicate")
            doc  = make_doc("Updating a %s resource to a duplicate name" % impl_instance.iontype)
            add_test_method(name, doc, fun)


        def gen_test_delete():
            """
            generate the function to test the delete
            """
            def fun(self):
                """
                self is an instance of the tester class
                """
                # get objects
                svc = self._rimi_getservice()
                myimpl = getattr(svc, impl_attr)                 

                # put in an object
                sample_resource_id = myimpl.create_one(sample_resource())

                log.debug("Attempting to delete newly created object with id=%s" % 
                          sample_resource_id)

                #delete
                myimpl.delete_one(sample_resource_id)
                
                # verify delete
                self.assertRaises(NotFound, myimpl.delete_one, sample_resource_id)

                
            name = make_name("resource_impl_delete")
            doc  = make_doc("Deleting a %s resource" % impl_instance.iontype)
            add_test_method(name, doc, fun)


        def gen_test_delete_notfound():
            """
            generate the function to test the delete in a not-found case
            """
            def fun(self):
                """
                self is an instance of the tester class
                """
                # get objects
                svc = self._rimi_getservice()
                myimpl = getattr(svc, impl_attr)                 
                                
                self.assertRaises(NotFound, myimpl.delete_one, "111")
            
            name = make_name("resource_impl_delete_notfound")
            doc  = make_doc("Deleting a %s resource that doesn't exist" % impl_instance.iontype)
            add_test_method(name, doc, fun)


        def gen_test_find():
            """
            generate the function to test the find op
            """
            def fun(self):
                """
                self is an instance of the tester class
                """
                # get objects
                svc = self._rimi_getservice()
                myimpl = getattr(svc, impl_attr)                 

                # put in 2 objects
                sr = sample_resource()
                sample_resource_id = myimpl.create_one(sr)

                sr.name = "NOT A DUPE"
                sample_resource_id2 = myimpl.create_one(sr)

                resources = myimpl.find_some({})
                self.assertIsInstance(resources, list)
                self.assertNotEqual(0, len(resources))
                self.assertNotEqual(1, len(resources))
                self.assertIn(sample_resource_id, resources)
                self.assertIn(sample_resource_id2, resources)

                
            name = make_name("resource_impl_find")
            doc  = make_doc("Finding (all) %s resources" % impl_instance.iontype)
            add_test_method(name, doc, fun)



        # can you believe we're still within a single function?

        # add the service lookup function
        gen_svc_lookup()


        # add each method to the tester class

        gen_test_create()
        gen_test_create_bad_noname()
        gen_test_create_bad_dupname()
        gen_test_create_bad_has_id()
        gen_test_read()
        gen_test_read_notfound()
        gen_test_update()
        gen_test_update_bad_noid()
        gen_test_update_bad_dupname()
        gen_test_delete()
        gen_test_delete_notfound()
        gen_test_find()

