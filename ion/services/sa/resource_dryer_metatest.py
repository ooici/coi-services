#!/usr/bin/env python

"""
@file ion/services/sa/test/resource_dryer_metatest.py
@author Ian Katz

"""
import hashlib

# from mock import Mock, sentinel, patch
from pyon.core.bootstrap import IonObject
from pyon.core.exception import BadRequest, NotFound


class ResourceDryerMetatest(object):
    """
    This function adds test methods for CRUD and associations in the given 
    resource dryer class.

    For example, OUTSIDE and AFTER the TestInstrumentManagement class, write this:

        rwm = ResourceDryerMetatest(TestInstrumentManagement,
                                     InstrumentManagementService,
                                     log)

        rwm.add_resource_dryer_unittests(InstrumentAgentInstanceDryer, 
                                          {"exchange_name": "rhubarb"}

    The dryer object MUST be available as a class variable in the service under test!
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


    def build_test_descriptors(self, resource_params):
        """
        build a sample resource from the supplied dryer class
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
            sample_resource_md5 = "_%s" % hashlib.sha224(sample_resource_extras).hexdigest()[:7]
    
        self.sample_resource_extras  = sample_resource_extras
        self.sample_resource_md5     = sample_resource_md5


    def sample_resource_factory(self, dryer, resource_params):
        """
        build a sample resource factory from the supplied dryer class
         that produces resources populated with the supplied parameters
        
        @param dryer an instance of the dryer class
        @resource_params a dict of params to add
        """
        
        def fun():
            #ret = Mock()
            ret = IonObject(dryer.iontype)
            ret.name = "sample %s" % dryer.iontype
            ret.description = "description of sample %s" % dryer.iontype
            for k, v in resource_params.iteritems():
                setattr(ret, k, v)
            return ret

        return fun

    def find_class_variable_name(self, instance, target_type):
        """
        determine which class variable in the instance is of the target type
        @param instance the class to be searched
        @param target type the type of the variable we want to find
        @retval string the name of the class variable
        """
        ret = None
        for k, v in instance.__dict__.iteritems():
            if type(v) == target_type:
                ret = k
        return ret

    def find_dryer_attribute(self, dryer):
        """
        determine which class variable in the service is the dryer class
        @param dryer an instance of the dryer
        """
        dryer_attr = self.find_class_variable_name(self.service_instance, type(dryer))

        assert(dryer_attr)

        # write a message to myself that will appear in the description of a failed test
        #self.sample_resource_extras += " found at self.%s" % dryer_attr

        return dryer_attr

        
    def add_resource_dryer_unittests(self, 
                                      resource_dryer_class, 
                                      resource_params):
        """
        Add tests for the resorce_dryer_class to the (self.)resource_tester_class

        @param resource_dryer_class the class of the resource dryer you want tested
        @param resource_params dictionary of extra params to add to the sample resource
        """
        # create a dryer class, no clients
        dryer_instance       = resource_dryer_class([])

        self.build_test_descriptors(resource_params)

        dryer_attr  = self.find_dryer_attribute(dryer_instance)

        #this is convoluted but it helps me debug by 
        #  being able to inject text into the sample_resource_extras
        sample_resource = self.sample_resource_factory(dryer_instance, resource_params)


        find_cv_func = self.find_class_variable_name

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
            return "%s_%s%s" % (dryer_instance.iontype, name, self.sample_resource_md5)
        
        def make_doc(doc):
            """
            make a good doc string for a test from by including the extra params
            @param doc the base string for the descripton
            """
            return "%s %s" % (doc, self.sample_resource_extras)

            
        def gen_svc_lookup():
            """
            put a new method in the tester class to
            determine which class variable in the tester class is the service being tested
            """
            def fun(self):
                """
                self is an instance of the tester class
                """
                if not hasattr(self, "_rwm_service_obj"):
                    service_itself = getattr(self, find_cv_func(self, service_type))
                    self._rwm_service_obj = service_itself
                    assert(self._rwm_service_obj)

                return self._rwm_service_obj

            if not hasattr(self.tester_class, "_rwm_getservice"): 
                add_new_method("_rwm_getservice", "Finds resource registry", fun)

        # def gen_ionobj_lookup():
        #     """
        #     put a new method in the tester class to
        #     get access to the test class's ionobject
        #     """
        #     def fun(self):
        #         """
        #         self is an instance of the tester class
        #         """
        #         if not hasattr(self, "_rwm_mock_ionobj"):
        #             self._rwm_mock_ionobj = self._create_IonObject_mock(
        #                 service_type.__name__ + '.IonObject')
        #             assert(self._rwm_mock_ionobj)

        #         return self._rwm_mock_ionobj

        #     if not hasattr(self.tester_class, "_rwm_ionobject"):
        #         add_new_method("_rwm_ionobject", "Mock Ionobject", fun)

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
        #         sample_resource.name = "sample %s" % dryer_instance.iontype
        #         sample_resource.description = "description of sample %s" % dryer_instance.iontype
                
        #         # build out strings if there are more params
        #         if resource_params:
        #             extras = []
        #             for k, v in resource_params.iteritems():
        #                 setattr(sample_resource, k, v)

        #                 self.sample_resource         = sample_resource
        #                 self.sample_resource_extras  = sample_resource_extras
        #                 self.sample_resource_md5     = sample_resource_md5
                        
        #         return sample_resource



        def gen_test_create():
            """
            generate the function to test the create
            """
            def fun(self):
                """
                self is an instance of the tester class
                """
                # get objects
                svc = self._rwm_getservice()
                mydryer = getattr(svc, dryer_attr)                 
                good_sample_resource = sample_resource()
                
                #configure Mock
                svc.clients.resource_registry.create.return_value = ('111', 'bla')
                svc.clients.resource_registry.find_resources.return_value = ([], [])

                response = mydryer.create_one(good_sample_resource)
                idfield = "%s_id" % dryer_instance.ionlabel
                self.assertIn(idfield, response)
                sample_resource_id = response[idfield]

                svc.clients.resource_registry.create.assert_called_once_with(good_sample_resource)
                self.assertEqual(sample_resource_id, '111')
                
            name = make_name("resource_dryer_create")
            doc  = make_doc("Creation of a new %s resource" % dryer_instance.iontype)
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
                svc = self._rwm_getservice()
                mydryer = getattr(svc, dryer_attr)                 
                bad_sample_resource = sample_resource()
                delattr(bad_sample_resource, "name")
                
                #configure Mock
                svc.clients.resource_registry.create.return_value = ('111', 'bla')
                svc.clients.resource_registry.find_resources.return_value = ([], [])

                self.assertRaises(BadRequest, mydryer.create_one, bad_sample_resource)


            name = make_name("resource_dryer_create_bad_noname")
            doc  = make_doc("Creation of a (bad) new %s resource (no name)" % dryer_instance.iontype)
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
                svc = self._rwm_getservice()
                mydryer = getattr(svc, dryer_attr)                 
                bad_sample_resource = sample_resource()
                delattr(bad_sample_resource, "name")
                
                #configure Mock
                svc.clients.resource_registry.create.return_value = ('111', 'bla')
                svc.clients.resource_registry.find_resources.return_value = ([0], [0])

                self.assertRaises(BadRequest, mydryer.create_one, bad_sample_resource)


            name = make_name("resource_dryer_create_bad_dupname")
            doc  = make_doc("Creation of a (bad) new %s resource (duplicate name)" % dryer_instance.iontype)
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
                svc = self._rwm_getservice()
                mydryer = getattr(svc, dryer_attr)                 
                bad_sample_resource = sample_resource()
                setattr(bad_sample_resource, "_id", "12345")
                
                #configure Mock
                svc.clients.resource_registry.create.return_value = ('111', 'bla')
                svc.clients.resource_registry.find_resources.return_value = ([], [])

                self.assertRaises(BadRequest, mydryer.create_one, bad_sample_resource)


            name = make_name("resource_dryer_create_bad_has_id")
            doc  = make_doc("Creation of a (bad) new %s resource (has _id)" % dryer_instance.iontype)

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
                svc = self._rwm_getservice()
                mydryer = getattr(svc, dryer_attr)                 
                myret = sample_resource()
                                
                #configure Mock
                svc.clients.resource_registry.read.return_value = myret

                response = mydryer.read_one("111")
                svc.clients.resource_registry.read.assert_called_once_with("111", "")
                self.assertIn(dryer_instance.ionlabel, response)
                self.assertEqual(response[dryer_instance.ionlabel], myret)
                #self.assertDictEqual(response[dryer_instance.ionlabel].__dict__,
                #                     sample_resource().__dict__)

                
            name = make_name("resource_dryer_read")
            doc  = make_doc("Reading a %s resource" % dryer_instance.iontype)
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
                svc = self._rwm_getservice()
                mydryer = getattr(svc, dryer_attr)                 
                                
                #configure Mock
                svc.clients.resource_registry.read.return_value = None

                self.assertRaises(NotFound, mydryer.read_one, "111")
                svc.clients.resource_registry.read.assert_called_once_with("111", "")
            
            name = make_name("resource_dryer_read_notfound")
            doc  = make_doc("Reading a %s resource that doesn't exist" % dryer_instance.iontype)
            add_test_method(name, doc, fun)



        def gen_test_update():
            """
            generate the function to test the create
            """
            def fun(self):
                """
                self is an instance of the tester class
                """
                # get objects
                svc = self._rwm_getservice()
                mydryer = getattr(svc, dryer_attr)                 
                good_sample_resource = sample_resource()
                setattr(good_sample_resource, "_id", "111")

                #configure Mock
                svc.clients.resource_registry.update.return_value = ('111', 'bla')                
                svc.clients.resource_registry.find_resources.return_value = ([], [])

                response = mydryer.update_one(good_sample_resource)
                self.assertIn("success", response)
                self.assertTrue(response["success"])

                svc.clients.resource_registry.update.assert_called_once_with(good_sample_resource)

                
            name = make_name("resource_dryer_update")
            doc  = make_doc("Updating a %s resource" % dryer_instance.iontype)
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
                svc = self._rwm_getservice()
                mydryer = getattr(svc, dryer_attr)                 
                bad_sample_resource = sample_resource()
                
                self.assertRaises(BadRequest, mydryer.update_one, bad_sample_resource)

                
            name = make_name("resource_dryer_update_bad_no_id")
            doc  = make_doc("Updating a %s resource without an ID" % dryer_instance.iontype)
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
                svc = self._rwm_getservice()
                mydryer = getattr(svc, dryer_attr)                 
                bad_sample_resource = sample_resource()
                setattr(bad_sample_resource, "_id", "111")
                
                svc.clients.resource_registry.find_resources.return_value = ([0], [0])
                self.assertRaises(BadRequest, mydryer.update_one, bad_sample_resource)

                
            name = make_name("resource_dryer_update_bad_duplicate")
            doc  = make_doc("Updating a %s resource to a dupcliate name" % dryer_instance.iontype)
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
                svc = self._rwm_getservice()
                mydryer = getattr(svc, dryer_attr)                 
                myret = sample_resource()
                                
                #configure Mock
                svc.clients.resource_registry.read.return_value = myret
                svc.clients.resource_registry.delete.return_value = None

                response = mydryer.delete_one("111")
                self.assertIn("success", response)
                self.assertTrue(response["success"])
                svc.clients.resource_registry.read.assert_called_once_with("111", "")
                svc.clients.resource_registry.delete.assert_called_once_with(myret)

                
            name = make_name("resource_dryer_delete")
            doc  = make_doc("Deleting a %s resource" % dryer_instance.iontype)
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
                svc = self._rwm_getservice()
                mydryer = getattr(svc, dryer_attr)                 
                                
                #configure Mock
                svc.clients.resource_registry.read.return_value = None

                self.assertRaises(NotFound, mydryer.delete_one, "111")
            
            name = make_name("resource_dryer_delete_notfound")
            doc  = make_doc("Deleting a %s resource that doesn't exist" % dryer_instance.iontype)
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
                svc = self._rwm_getservice()
                mydryer = getattr(svc, dryer_attr)                 
                                
                #configure Mock
                svc.clients.resource_registry.find_resources.return_value = ([0], [0])

                response = mydryer.find_some({})
                out_list = "%s_list" % dryer_instance.ionlabel
                self.assertIn(out_list, response)
                self.assertIsInstance(response[out_list], list)
                self.assertNotEqual(0, len(response[out_list]))
                svc.clients.resource_registry.find_resources.assert_called_once_with(dryer_instance.iontype,
                                                                                     None,
                                                                                     None,
                                                                                     True)

                
            name = make_name("resource_dryer_find")
            doc  = make_doc("Finding (all) %s resources" % dryer_instance.iontype)
            add_test_method(name, doc, fun)


        def gen_tests_associated_finds():
            gen_tests_find_having()
            gen_tests_find_stemming()


        def gen_tests_find_having():
            """
            create a test for each of the find_having_* methods in the dryer
            """
            for k in dir(dryer_instance):
                parts = k.split("_", 2)
                if "find" == parts[0] and "having" == parts[1]:

                    def freeze(parts):
                        """
                        must freeze this so the loop doesn't overwrite the parts varible
                        """
                        assn_type = parts[2]
                        find_name = "_".join(parts)

                        def fun(self):
                            svc = self._rwm_getservice()
                            mydryer = getattr(svc, dryer_attr)
                            myfind = getattr(mydryer, find_name)

                            #set up Mock
                            reply = (['333'], ['444'])
                            svc.clients.resource_registry.find_subjects.return_value = reply

                            #call the dryer
                            response = myfind("111")
                            self.assertEqual(reply, response)
                        
                        name = make_name("resource_dryer_find_having_%s_link" % assn_type)
                        doc  = make_doc("Checking find %s having %s" % (dryer_instance.iontype, assn_type))
                        add_test_method(name, doc, fun)

                    freeze(parts)


        def gen_tests_find_stemming():
            """
            create a test for each of the find_stemming_* methods in the dryer
            """
            for k in dir(dryer_instance):
                parts = k.split("_", 2)
                if "find" == parts[0] and "stemming" == parts[1]:

                    def freeze(parts):
                        """
                        must freeze this so the loop doesn't overwrite the parts varible
                        """
                        assn_type = parts[2]
                        find_name = "_".join(parts)

                        def fun(self):
                            svc = self._rwm_getservice()
                            mydryer = getattr(svc, dryer_attr)
                            myfind = getattr(mydryer, find_name)

                            #set up Mock
                            reply = (['333'], ['444'])
                            svc.clients.resource_registry.find_objects.return_value = reply

                            #call the dryer
                            response = myfind("111")
                            self.assertEqual(reply, response)
                        
                        name = make_name("resource_dryer_find_stemming_%s_links" % assn_type)
                        doc  = make_doc("Checking find %s stemming from %s" % (assn_type, dryer_instance.iontype))
                        add_test_method(name, doc, fun)

                    freeze(parts)

        
            
        def gen_tests_associations():
            gen_tests_links()
            gen_tests_unlinks()

        def gen_tests_links():
            """
            create a test for each of the create_association tests in the dryer
            """
            for k in dir(dryer_instance):
                parts = k.split("_", 1)
                if "link" == parts[0]:

                    def freeze(parts):
                        """
                        must freeze this so the loop doesn't overwrite the parts varible
                        """
                        assn_type = parts[1]
                        link_name = "_".join(parts)

                        def fun(self):
                            svc = self._rwm_getservice()
                            mydryer = getattr(svc, dryer_attr)
                            mylink = getattr(mydryer, link_name)

                            #set up Mock
                            reply = ('333', 'bla')
                            svc.clients.resource_registry.create_association.return_value = reply

                            #call the dryer
                            response = mylink("111", "222")
                            self.assertEqual(reply, response)

                        name = make_name("resource_dryer_association_%s_link" % assn_type)
                        doc  = make_doc("Checking create_association of a %s resource with its %s" % (dryer_instance.iontype, assn_type))
                        add_test_method(name, doc, fun)

                    freeze(parts)


        def gen_tests_unlinks():
            """
            create a test for each of the delete_association tests in the dryer
            """
            for k in dir(dryer_instance):
                parts = k.split("_", 1)
                if "unlink" == parts[0]:

                    def freeze(parts):
                        """
                        must freeze this so the loop doesn't overwrite the parts varible
                        """
                        assn_type = parts[1]
                        link_name = "_".join(parts)

                        def fun(self):
                            svc = self._rwm_getservice()
                            mydryer = getattr(svc, dryer_attr)
                            myunlink = getattr(mydryer, link_name)
                            
                            svc.clients.resource_registry.create_association.return_value = None

                            #call the dryer
                            response = myunlink("111", "222")
                            
                            #there is no response, self.assertEqual("f", str(response))

                        name = make_name("resource_dryer_association_%s_unlink" % assn_type)
                        doc  = make_doc("Checking delete_association of a %s resource from its %s" % (dryer_instance.iontype, assn_type))
                        add_test_method(name, doc, fun)

                    freeze(parts)




        # can you believe we're still within a single function?
        # it's time to add each method to the tester class
        gen_svc_lookup()
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
        gen_tests_associations()
        gen_tests_associated_finds()

