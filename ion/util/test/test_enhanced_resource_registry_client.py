#!/usr/bin/env python

"""
@file ion/util/test/test_enhanced_resource_registry_client.py
@author Ian Katz
@test ion.util.enhanced_resource_registry_client Unit test suite
"""
from unittest.case import SkipTest
from ion.services.sa.test.helpers import any_old

from mock import Mock #, sentinel, patch
from ion.util.enhanced_resource_registry_client import EnhancedResourceRegistryClient
from nose.plugins.attrib import attr

from pyon.core.exception import BadRequest, Inconsistent, NotFound
from pyon.ion.resource import RT, PRED, LCE
from pyon.util.unit_test import PyonTestCase


@attr('UNIT', group='sa')
class TestEnhancedResourceRegistryClient(PyonTestCase):

    def setUp(self):
        self.rr = Mock()
        self.RR2 = EnhancedResourceRegistryClient(self.rr)
        
    def sample_resource(self):
        return any_old(RT.InstrumentDevice)
        
    def test_init(self):
        pass


    def test_create(self):
        """
        test resource creation in normal case
        """
        # get objects
        good_sample_resource = self.sample_resource()

        #configure Mock
        self.rr.create.return_value = ('111', 'bla')
        self.rr.find_resources.return_value = ([], [])

        sample_resource_id = self.RR2.create(good_sample_resource, RT.InstrumentDevice)

        self.rr.create.assert_called_once_with(good_sample_resource)
        self.assertEqual(sample_resource_id, '111')



    def test_create_bad_wrongtype(self):
        """
        test resource creation failure for wrong type
        """
        # get objects

        bad_sample_resource = any_old(RT.PlatformDevice)
        delattr(bad_sample_resource, "name")

        #configure Mock
        self.rr.create.return_value = ('111', 'bla')
        self.rr.find_resources.return_value = ([], [])

        self.assertRaises(BadRequest, self.RR2.create, bad_sample_resource, RT.InstrumentDevice)


    def test_create_bad_noname(self):
        """
        test resource creation failure for no name
        """
        # get objects

        bad_sample_resource = self.sample_resource()
        delattr(bad_sample_resource, "name")

        #configure Mock
        self.rr.create.return_value = ('111', 'bla')
        self.rr.find_resources.return_value = ([], [])

        self.assertRaises(BadRequest, self.RR2.create, bad_sample_resource, RT.InstrumentDevice)


    def test_create_bad_dupname(self):
        """
        test resource creation failure for duplicate name
        """
        # get objects

        bad_sample_resource = self.sample_resource()
        #really, the resource doesn't matter; it's the retval from find that matters

        #configure Mock
        self.rr.create.return_value = ('111', 'bla')
        self.rr.find_resources.return_value = ([0], [0])

        self.assertRaises(BadRequest, self.RR2.create, bad_sample_resource, RT.InstrumentDevice)



    def test_read(self):
        """
        test resource read (passthru)
        """
        # get objects
        myret = self.sample_resource()

        #configure Mock
        self.rr.read.return_value = myret

        response = self.RR2.read("111", RT.InstrumentDevice)
        self.rr.read.assert_called_once_with("111")
        self.assertEqual(response, myret)
        #self.assertDictEqual(response.__dict__,
        #                     self.sample_resource().__dict__)


    def test_read_bad_wrongtype(self):
        """
        test resource read (passthru)
        """
        # get objects
        myret = self.sample_resource()

        #configure Mock
        self.rr.read.return_value = myret

        self.assertRaises(BadRequest, self.RR2.read, "111", RT.PlatformDevice)
        self.rr.read.assert_called_once_with("111")


    def test_update(self):
        """
        test resource update in normal case
        """
        # get objects

        good_sample_resource = self.sample_resource()
        setattr(good_sample_resource, "_id", "111")

        #configure Mock
        self.rr.update.return_value = ('111', 'bla')
        self.rr.find_resources.return_value = ([], [])

        self.RR2.update(good_sample_resource, RT.InstrumentDevice)

        self.rr.update.assert_called_once_with(good_sample_resource)


    def test_update_bad_wrongtype(self):
        """
        test update failure due to duplicate name
        """
        # get objects

        bad_sample_resource = self.sample_resource()

        self.assertRaises(BadRequest, self.RR2.update, bad_sample_resource, RT.PlatformDevice)


    def test_update_bad_dupname(self):
        """
        test update failure due to duplicate name
        """
        # get objects

        bad_sample_resource = self.sample_resource()
        setattr(bad_sample_resource, "_id", "111")

        self.rr.find_resources.return_value = ([0], [0])
        self.assertRaises(BadRequest, self.RR2.update, bad_sample_resource, RT.InstrumentDevice)


    def test_update_bad_noid(self):
        """
        test update failure due to duplicate name
        """
        # get objects

        bad_sample_resource = self.sample_resource()


        self.rr.find_resources.return_value = ([0], [0])
        self.assertRaises(BadRequest, self.RR2.update, bad_sample_resource, RT.InstrumentDevice)


    def test_retire(self):
        """
        test retire
        """
        # get objects

        myret = self.sample_resource()

        #configure Mock
        self.rr.read.return_value = myret
        self.rr.delete.return_value = None
        self.rr.retire.return_value = None

        try:
            self.RR2.retire("111", RT.InstrumentDevice)
        except TypeError as te:
            # for logic tests that run into mock trouble
            if "'Mock' object is not iterable" != te.message:
                raise te
            else:
                raise SkipTest("Must test this with INT test")
        except Exception as e:
            raise e

        #self.rr.read.assert_called_with("111", "")
        self.rr.retire.assert_called_once_with("111")


    def test_retire_bad_wrongtype(self):
        """
        test resource read (passthru)
        """
        # get objects
        myret = self.sample_resource()

        #configure Mock
        self.rr.read.return_value = myret

        self.assertRaises(BadRequest, self.RR2.retire, "111", RT.PlatformDevice)
        self.rr.read.assert_called_once_with("111")


    def test_pluck_delete(self):
        """
        test delete
        """
        # get objects

        myret = self.sample_resource()

        #configure Mock
        self.rr.read.return_value = myret
        self.rr.delete.return_value = None
        self.rr.find_resources.return_value = None
        self.rr.find_objects.return_value = (["2"], ["2"])
        self.rr.find_subjects.return_value = (["3"], ["3"])

        self.RR2.pluck_delete("111", RT.InstrumentDevice)

        self.rr.delete.assert_called_once_with("111")


    def test_advance_lcs(self):
        """
        call RR when the transition ISN'T retire
        """
        self.RR2.advance_lcs("111", LCE.PLAN)
        self.rr.execute_lifecycle_transition.assert_called_once_with(resource_id="111", transition_event=LCE.PLAN)

        self.RR2.advance_lcs("222", LCE.RETIRE)
        self.rr.retire.assert_called_once_with("222")


    def test_delete_association(self):
        self.rr.get_association.return_value = "111"
        self.RR2.delete_association("a", "b", "c")
        self.rr.delete_association.assert_called_once_with("111")


    def test_delete_all_object_associations(self):
        self.rr.find_associations.return_value = ["111"]
        self.RR2.delete_object_associations("x")
        self.rr.delete_association.assert_called_once_with("111")


    def test_delete_all_subject_associations(self):
        self.rr.find_associations.return_value = ["111"]
        self.RR2.delete_subject_associations("x")
        self.rr.delete_association.assert_called_once_with("111")

    def test_pluck(self):
        self.rr.find_subjects.return_value = (["111"], ["aaa"])
        self.rr.find_objects.return_value = (["222"], ["bbb"])
        self.RR2.pluck("x")
        #self.rr.delete_association.assert_called_with("bbb")
        self.rr.delete_association.assert_called_with("aaa")
        self.assertEqual(self.rr.delete_association.call_count, 2)


    def test_find_objects_using_id(self):
        self.tbase_find_objects("x_id")


    def test_find_objects_using_ionobj(self):
        obj = any_old(RT.InstrumentDevice)
        setattr(obj, "_id", "foo_id")
        self.tbase_find_objects(obj)


    def test_find_objects_using_junk(self):
        self.tbase_find_objects(1)


    def tbase_find_objects(self, sample_obj):
        """
        test all 8 flavors of find objects: return IonObjects/ids, return single/multiple, use predicate/no-predicate
        """

        def rst():
            self.rr.find_objects.reset_mock()
            self.rr.find_objects.return_value = ([], [])
            self.assertEqual(0, self.rr.find_subjects.call_count)

        def rst1():
            self.rr.find_objects.reset_mock()
            self.rr.find_objects.return_value = (["x"], ["x"])
            self.assertEqual(0, self.rr.find_subjects.call_count)

        def rst2():
            self.rr.find_objects.reset_mock()
            self.rr.find_objects.return_value = (["x", "y"], ["z", "k"])
            self.assertEqual(0, self.rr.find_subjects.call_count)

        x = sample_obj
        xx = x
        if hasattr(x, "_id"):
            xx = x._id

        # find none
        rst()
        self.RR2.find_instrument_models_of_instrument_device(x)
        self.rr.find_objects.assert_called_once_with(subject=xx, predicate=PRED.hasModel, object_type=RT.InstrumentModel, id_only=False)

        rst()
        self.assertRaises(NotFound, self.RR2.find_instrument_model_of_instrument_device, x)
        self.rr.find_objects.assert_called_once_with(subject=xx, predicate=PRED.hasModel, object_type=RT.InstrumentModel, id_only=False)

        rst()
        self.RR2.find_instrument_model_ids_of_instrument_device(x)
        self.rr.find_objects.assert_called_once_with(subject=xx, predicate=PRED.hasModel, object_type=RT.InstrumentModel, id_only=True)

        rst()
        self.assertRaises(NotFound, self.RR2.find_instrument_model_id_of_instrument_device, x)
        self.rr.find_objects.assert_called_once_with(subject=xx, predicate=PRED.hasModel, object_type=RT.InstrumentModel, id_only=True)

        # find one
        rst1()
        self.RR2.find_instrument_models_of_instrument_device(x)
        self.rr.find_objects.assert_called_once_with(subject=xx, predicate=PRED.hasModel, object_type=RT.InstrumentModel, id_only=False)

        rst1()
        self.RR2.find_instrument_model_of_instrument_device(x)
        self.rr.find_objects.assert_called_once_with(subject=xx, predicate=PRED.hasModel, object_type=RT.InstrumentModel, id_only=False)

        rst1()
        self.RR2.find_instrument_model_ids_of_instrument_device(x)
        self.rr.find_objects.assert_called_once_with(subject=xx, predicate=PRED.hasModel, object_type=RT.InstrumentModel, id_only=True)

        rst1()
        self.RR2.find_instrument_model_id_of_instrument_device(x)
        self.rr.find_objects.assert_called_once_with(subject=xx, predicate=PRED.hasModel, object_type=RT.InstrumentModel, id_only=True)


        # find multiples
        rst2()
        self.RR2.find_instrument_models_of_instrument_device(x)
        self.rr.find_objects.assert_called_once_with(subject=xx, predicate=PRED.hasModel, object_type=RT.InstrumentModel, id_only=False)

        rst2()
        self.assertRaises(Inconsistent, self.RR2.find_instrument_model_of_instrument_device, x)
        self.rr.find_objects.assert_called_once_with(subject=xx, predicate=PRED.hasModel, object_type=RT.InstrumentModel, id_only=False)

        rst2()
        self.RR2.find_instrument_model_ids_of_instrument_device(x)
        self.rr.find_objects.assert_called_once_with(subject=xx, predicate=PRED.hasModel, object_type=RT.InstrumentModel, id_only=True)

        rst2()
        self.assertRaises(Inconsistent, self.RR2.find_instrument_model_id_of_instrument_device, x)
        self.rr.find_objects.assert_called_once_with(subject=xx, predicate=PRED.hasModel, object_type=RT.InstrumentModel, id_only=True)

        # find using
        rst2()
        self.RR2.find_instrument_models_of_instrument_device_using_has_model(x)
        self.rr.find_objects.assert_called_once_with(subject=xx, predicate=PRED.hasModel, object_type=RT.InstrumentModel, id_only=False)

        rst2()
        self.assertRaises(Inconsistent, self.RR2.find_instrument_model_of_instrument_device_using_has_model, x)
        self.rr.find_objects.assert_called_once_with(subject=xx, predicate=PRED.hasModel, object_type=RT.InstrumentModel, id_only=False)

        rst2()
        self.RR2.find_instrument_model_ids_of_instrument_device_using_has_model(x)
        self.rr.find_objects.assert_called_once_with(subject=xx, predicate=PRED.hasModel, object_type=RT.InstrumentModel, id_only=True)

        rst2()
        self.assertRaises(Inconsistent, self.RR2.find_instrument_model_id_of_instrument_device_using_has_model, x)
        self.rr.find_objects.assert_called_once_with(subject=xx, predicate=PRED.hasModel, object_type=RT.InstrumentModel, id_only=True)


    def test_find_subjects_using_id(self):
        self.tbase_find_subjects("x_id")


    def test_find_subjects_using_ionobj(self):
        obj = any_old(RT.InstrumentDevice)
        setattr(obj, "_id", "foo_id")
        self.tbase_find_subjects(obj)


    def test_find_subjects_using_junk(self):
        self.tbase_find_subjects(1)


    def tbase_find_subjects(self, sample_obj):
        """
        test all 8 flavors of find subjects: return IonObjects/ids, return single/multiple, use predicate/no-predicate
        """
        def rst():
            self.rr.find_subjects.reset_mock()
            self.rr.find_subjects.return_value = ([], [])
            self.assertEqual(0, self.rr.find_objects.call_count)

        def rst1():
            self.rr.find_subjects.reset_mock()
            self.rr.find_subjects.return_value = (["x"], ["x"])
            self.assertEqual(0, self.rr.find_objects.call_count)

        def rst2():
            self.rr.find_subjects.reset_mock()
            self.rr.find_subjects.return_value = (["x", "y"], ["z", "k"])
            self.assertEqual(0, self.rr.find_objects.call_count)

        x = sample_obj
        xx = x
        if hasattr(x, "_id"):
            xx = x._id

        # find none
        rst()
        self.RR2.find_instrument_devices_by_instrument_model(x)
        self.rr.find_subjects.assert_called_once_with(object=xx, predicate=PRED.hasModel, subject_type=RT.InstrumentDevice, id_only=False)

        rst()
        self.assertRaises(NotFound, self.RR2.find_instrument_device_by_instrument_model, x)
        self.rr.find_subjects.assert_called_once_with(object=xx, predicate=PRED.hasModel, subject_type=RT.InstrumentDevice, id_only=False)

        rst()
        self.RR2.find_instrument_device_ids_by_instrument_model(x)
        self.rr.find_subjects.assert_called_once_with(object=xx, predicate=PRED.hasModel, subject_type=RT.InstrumentDevice, id_only=True)

        rst()
        self.assertRaises(NotFound, self.RR2.find_instrument_device_id_by_instrument_model, x)
        self.rr.find_subjects.assert_called_once_with(object=xx, predicate=PRED.hasModel, subject_type=RT.InstrumentDevice, id_only=True)


        # find 1
        rst1()
        self.RR2.find_instrument_devices_by_instrument_model(x)
        self.rr.find_subjects.assert_called_once_with(object=xx, predicate=PRED.hasModel, subject_type=RT.InstrumentDevice, id_only=False)

        rst1()
        self.RR2.find_instrument_device_by_instrument_model(x)
        self.rr.find_subjects.assert_called_once_with(object=xx, predicate=PRED.hasModel, subject_type=RT.InstrumentDevice, id_only=False)

        rst1()
        self.RR2.find_instrument_device_ids_by_instrument_model(x)
        self.rr.find_subjects.assert_called_once_with(object=xx, predicate=PRED.hasModel, subject_type=RT.InstrumentDevice, id_only=True)

        rst1()
        self.RR2.find_instrument_device_id_by_instrument_model(x)
        self.rr.find_subjects.assert_called_once_with(object=xx, predicate=PRED.hasModel, subject_type=RT.InstrumentDevice, id_only=True)


        # find multiple
        rst2()
        self.RR2.find_instrument_devices_by_instrument_model(x)
        self.rr.find_subjects.assert_called_once_with(object=xx, predicate=PRED.hasModel, subject_type=RT.InstrumentDevice, id_only=False)

        rst2()
        self.assertRaises(Inconsistent, self.RR2.find_instrument_device_by_instrument_model, x)
        self.rr.find_subjects.assert_called_once_with(object=xx, predicate=PRED.hasModel, subject_type=RT.InstrumentDevice, id_only=False)

        rst2()
        self.RR2.find_instrument_device_ids_by_instrument_model(x)
        self.rr.find_subjects.assert_called_once_with(object=xx, predicate=PRED.hasModel, subject_type=RT.InstrumentDevice, id_only=True)

        rst2()
        self.assertRaises(Inconsistent, self.RR2.find_instrument_device_id_by_instrument_model, x)
        self.rr.find_subjects.assert_called_once_with(object=xx, predicate=PRED.hasModel, subject_type=RT.InstrumentDevice, id_only=True)


        # find using
        rst2()
        self.RR2.find_instrument_devices_by_instrument_model_using_has_model(x)
        self.rr.find_subjects.assert_called_once_with(object=xx, predicate=PRED.hasModel, subject_type=RT.InstrumentDevice, id_only=False)

        rst2()
        self.assertRaises(Inconsistent, self.RR2.find_instrument_device_by_instrument_model_using_has_model, x)
        self.rr.find_subjects.assert_called_once_with(object=xx, predicate=PRED.hasModel, subject_type=RT.InstrumentDevice, id_only=False)

        rst2()
        self.RR2.find_instrument_device_ids_by_instrument_model_using_has_model(x)
        self.rr.find_subjects.assert_called_once_with(object=xx, predicate=PRED.hasModel, subject_type=RT.InstrumentDevice, id_only=True)

        rst2()
        self.assertRaises(Inconsistent, self.RR2.find_instrument_device_id_by_instrument_model_using_has_model, x)
        self.rr.find_subjects.assert_called_once_with(object=xx, predicate=PRED.hasModel, subject_type=RT.InstrumentDevice, id_only=True)


    def test_assign_unassign(self):
        """
        test all flavors of assign and unassign: with/without predicates
        """
        x = "x_id"
        y = "y_id"
        self.RR2.assign_instrument_model_to_instrument_device(y, x)
        self.rr.create_association.assert_called_once_with(x, PRED.hasModel, y)

        self.rr.get_association.return_value = "zzz"
        self.RR2.unassign_instrument_model_from_instrument_device(y, x)
        self.rr.delete_association.assert_called_once_with("zzz")

        self.assertRaises(BadRequest, getattr, self.RR2, "assign_data_product_to_data_process")
        self.assertRaises(BadRequest, getattr, self.RR2, "unassign_data_product_from_data_process")

        self.rr.create_association.reset_mock()
        self.RR2.assign_data_product_to_data_process_with_has_output_product(y, x)
        self.rr.create_association.assert_called_once_with(x, PRED.hasOutputProduct, y)

        self.rr.delete_association.reset_mock()
        self.rr.get_association.reset_mock()
        self.rr.get_association.return_value = "aaa"
        self.RR2.unassign_data_product_from_data_process_with_has_output_product(y, x)
        self.rr.delete_association.assert_called_once_with("aaa")

    def test_assign_single_object(self):
        x = "x_id"
        y = "y_id"

        def rst():
            self.rr.find_objects.reset_mock()
            self.rr.get_association.reset_mock()

        rst()
        self.rr.find_objects.return_value = ([], [])
        self.RR2.assign_one_instrument_model_to_instrument_device(y, x)
        self.rr.create_association.assert_called_once_with(x, PRED.hasModel, y)

        rst()
        self.rr.find_objects.return_value = (["a", "b"], ["c", "d"])
        self.assertRaises(Inconsistent, self.RR2.assign_one_instrument_model_to_instrument_device, y, x)

        rst()
        self.rr.find_objects.return_value = (["a"], ["b"])
        self.rr.get_association.return_value = "yay"
        self.RR2.assign_one_instrument_model_to_instrument_device(y, x)

        rst()
        self.rr.find_objects.return_value = (["a"], ["b"])
        self.rr.get_association.side_effect = NotFound("")
        self.assertRaises(BadRequest, self.RR2.assign_one_instrument_model_to_instrument_device, y, x)

    def test_assign_single_subject(self):
        x = "x_id"
        y = "y_id"

        def rst():
            self.rr.find_subjects.reset_mock()
            self.rr.get_association.reset_mock()


        rst()
        self.rr.find_subjects.return_value = ([], [])
        self.RR2.assign_instrument_device_to_one_instrument_site(y, x)
        self.rr.create_association.assert_called_once_with(x, PRED.hasDevice, y)

        rst()
        self.rr.find_subjects.return_value = (["a", "b"], ["c", "d"])
        self.assertRaises(Inconsistent, self.RR2.assign_instrument_device_to_one_instrument_site, y, x)

        rst()
        self.rr.find_subjects.return_value = (["a"], ["b"])
        self.rr.get_association.return_value = "yay"
        self.RR2.assign_instrument_device_to_one_instrument_site(y, x)

        rst()
        self.rr.find_subjects.return_value = (["a"], ["b"])
        self.rr.get_association.side_effect = NotFound("")
        self.assertRaises(BadRequest, self.RR2.assign_instrument_device_to_one_instrument_site, y, x)



    def test_bad_dynamics(self):
        x = "x_id"
        self.RR2.assign_foo_to_bar(x)
        self.rr.assign_foo_to_bar.assert_called_once_with(x)

        self.assertRaises(BadRequest, getattr, self.RR2, "find_instrument_model_of_instrument_device_using_has_site")
        self.assertRaises(BadRequest, getattr, self.RR2, "find_instrument_model_of_instrument_device_using_has_banana")
        self.assertRaises(BadRequest, getattr, self.RR2, "find_data_product_of_data_process")

        self.RR2.find_sensor_model_by_data_product(x)
        self.rr.find_sensor_model_by_data_product.assert_called_once_with(x)



