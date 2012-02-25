'''
@author David Stuebe
@file ion/services/dm/transformation/example/ion_seawater.py
@description Transforms using the csiro/gibbs seawater toolbox
'''

from pyon.ion.transform import TransformFunction
from pyon.service.service import BaseService
from pyon.core.exception import BadRequest
from pyon.public import IonObject, RT, log


from 



class ReverseTransform(TransformFunction):
    ''' This transform is an example of a transform that can be used as a TransformFunction
    it is interchangeable as either a TransformDataProcess or a TransformFunction

    TransformFunctions can be run by calling transform_management_service.execute_transform
    or they can be created normally through create_transform

    Typically these are short run, small scale transforms, they are blocking and will block
    the management service until the result is computed. The result of the transform is returned
    from execute_transform.
    '''
    def execute(self, input):
        retval = input
        retval.reverse()

        return retval