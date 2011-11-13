#!/usr/bin/env python

__author__ = 'Maurice Manning'
__license__ = 'Apache 2.0'


from interface.services.dm.itransformation_service import BaseTransformationService

class TransformationService(BaseTransformationService):

    def bind_transform(self, process=None):
        # Return Value
        # ------------
        # {status: true}
        #
        pass

    def define_transform(self, transform={}):
        # Return Value
        # ------------
        # {status: true}
        #
        pass

    def schedule_transform(self):
        # Return Value
        # ------------
        # null
        # ...
        #
        pass
  