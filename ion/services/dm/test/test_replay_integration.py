"""
@author Swarbhanu Chatterjee
@file ion/services/dm/test/test_replay_integration.py
@description Provides a full fledged integration from ingestion to replay using scidata
"""
import hashlib
from prototype.hdf.hdf_array_iterator import acquire_data
from pyon.public import CFG
from interface.objects import CouchStorage, HdfStorage, StreamGranuleContainer
from interface.services.dm.idata_retriever_service import DataRetrieverServiceClient
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient
from interface.services.dm.iingestion_management_service import IngestionManagementServiceClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from prototype.sci_data.constructor_apis import PointSupplementConstructor
from prototype.sci_data.stream_defs import SBE37_CDM_stream_definition
from pyon.core import bootstrap
from pyon.net.endpoint import Publisher, Subscriber
from pyon.util.file_sys import FS, FileSystem
from pyon.util.int_test import IonIntegrationTestCase
from nose.plugins.attrib import attr
from pyon.public import log

import random

import gevent

#------------------------------------------------------------------------------------------------------
# Create an async result
#------------------------------------------------------------------------------------------------------


def _create_packet(definition):
    """
    Create a ctd_packet for scientific data
    """

    point_def = definition
    psc = PointSupplementConstructor(point_definition=point_def)

    point_id = psc.add_point(time=1, location=(0,0,0))

    psc.add_scalar_point_coverage(point_id=point_id, coverage_id='temperature', value=random.normalvariate(mu=8.888, sigma=2.0))
    psc.add_scalar_point_coverage(point_id=point_id, coverage_id='pressure',value=float(10.0) )
    psc.add_scalar_point_coverage(point_id=point_id, coverage_id='conductivity', value=float(0.0))

    ctd_packet = psc.close_stream_granule()
    return ctd_packet








@attr('INT',group='dm')
class ReplayIntegrationTest(IonIntegrationTestCase):
    def setUp(self):
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2dm.yml')
        xs_dot_xp = CFG.core_xps.science_data
        try:
            self.XS, xp_base = xs_dot_xp.split('.')
            self.XP = '.'.join([bootstrap.get_sys_name(), xp_base])
        except ValueError:
            raise StandardError('Invalid CFG for core_xps.science_data: "%s"; must have "xs.xp" structure' % xs_dot_xp)



    def test_replay_integration(self):
        '''
        test_replay_integration
        '''
        import numpy as np
        # Keep the import it's used in the vector comparison below even though pycharm says its unused.

        cc = self.container
        XP = self.XP
        assertions = self.assertTrue

        ### Every thing below here can be run as a script:
        log.debug('Got it')

        pubsub_management_service = PubsubManagementServiceClient(node=cc.node)
        ingestion_management_service = IngestionManagementServiceClient(node=cc.node)
        dataset_management_service = DatasetManagementServiceClient(node=cc.node)
        data_retriever_service = DataRetrieverServiceClient(node=cc.node)

        datastore_name = 'dm_test_replay_integration'

        producer = Publisher(name=(XP,'stream producer'))

        ingestion_configuration_id = ingestion_management_service.create_ingestion_configuration(
            exchange_point_id=XP,
            couch_storage=CouchStorage(datastore_name=datastore_name,datastore_profile='SCIDATA'),
            hdf_storage=HdfStorage(),
            number_of_workers=1
        )

        ingestion_management_service.activate_ingestion_configuration(
            ingestion_configuration_id=ingestion_configuration_id
        )

        definition = SBE37_CDM_stream_definition()
        data_stream_id = definition.data_stream_id
        encoding_id = definition.identifiables[data_stream_id].encoding_id
        element_count_id = definition.identifiables[data_stream_id].element_count_id

        stream_def_id = pubsub_management_service.create_stream_definition(
            container=definition
        )
        stream_id = pubsub_management_service.create_stream(
            stream_definition_id=stream_def_id
        )

        dataset_id = dataset_management_service.create_dataset(
            stream_id=stream_id,
            datastore_name=datastore_name,
            view_name='datasets/dataset_by_id'
        )
        ingestion_management_service.create_dataset_configuration(
            dataset_id=dataset_id,
            archive_data=True,
            archive_metadata=True,
            ingestion_configuration_id = ingestion_configuration_id
        )
        definition.stream_resource_id = stream_id

        packet = _create_packet(definition)
        input_file = FileSystem.mktemp()
        input_file.write(packet.identifiables[data_stream_id].values)
        input_file_path = input_file.name
        input_file.close()

        fields=[
            'conductivity',
            'height',
            'latitude',
            'longitude',
            'pressure',
            'temperature',
            'time'
        ]

        input_vectors = acquire_data([input_file_path],fields , 2).next()

        producer.publish(msg=packet, to_name=(XP,'%s.data' % stream_id))

        replay_id, replay_stream_id = data_retriever_service.define_replay(dataset_id)
        ar = gevent.event.AsyncResult()
        def sub_listen(msg, headers):

            assertions(isinstance(msg,StreamGranuleContainer),'replayed message is not a granule.')
            hdf_string = msg.identifiables[data_stream_id].values
            sha1 = hashlib.sha1(hdf_string).hexdigest().upper()
            assertions(sha1 == msg.identifiables[encoding_id].sha1,'Checksum failed.')
            assertions(msg.identifiables[element_count_id].value==1, 'record replay count is incorrect %d.' % msg.identifiables[element_count_id].value)
            output_file = FileSystem.mktemp()
            output_file.write(msg.identifiables[data_stream_id].values)
            output_file_path = output_file.name
            output_file.close()
            output_vectors = acquire_data([output_file_path],fields,2).next()
            for field in fields:
                comparison = (input_vectors[field]['values']==output_vectors[field]['values'])
                assertions(comparison.all(), 'vector mismatch: %s vs %s' %
                                             (input_vectors[field]['values'],output_vectors[field]['values']))
            FileSystem.unlink(output_file_path)
            ar.set(True)

        subscriber = Subscriber(name=(XP,'replay listener'),callback=sub_listen)

        g = gevent.Greenlet(subscriber.listen, binding='%s.data' % replay_stream_id)
        g.start()

        data_retriever_service.start_replay(replay_id)

        ar.get(timeout=4)

        FileSystem.unlink(input_file_path)


