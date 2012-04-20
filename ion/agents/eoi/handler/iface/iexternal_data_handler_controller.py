#!/usr/bin/env python

__author__ = 'cmueller'

from zope.interface import Interface

class IExternalDataHandlerController(Interface):
    """Interface for all interaction with the ExternalDataHandler framework"""

    def __init__(data_provider=None, data_source=None, ext_dset=None, ds_desc=None, update_desc=None, *args, **kwargs):
        """
        Initializes this instance of ExternalDataHandler

        Subclasses of this interface must implement this method
        @param data_provider An instance of the ExternalDataProvider Resource
        @param data_source An instance of the DataSource Resource
        @param ext_dset An instance of the ExternalDataset Resource
        @param ds_desc An instance of the *DatasetDescription IonObject - the concrete implementation of this object is specific to the concrete *DataHandler
        @param update_desc An instance of the UpdateDescription IonObject
        """

    def get_status(**kwargs):
        """
        Returns the status of the External Observatory.
        @retval Should be a member of the OBSERVATORY_* attribute set defined in BaseExternalDataHandler
        """

        ## This is not in the correct place - there is no reason for the DataHandler to have any knowledge of the catalog for a given data provider/source
    #    def get_catalog(**kwargs):
    #        """
    #        Returns a catalog of assets from the External Observatory
    #        """
    #        pass

    def has_data_changed(last_fingerprint='', **kwargs):
        """
        This method should use the get_fingerprint and compare methods to determine if the current data source has changed

        @retval A boolean indicating if the data has changed
        """

    def has_new_data(**kwargs):
        """
        This method determines if there is new data at the surce

        @retval A boolean indicating if there is new data available
        """

    def acquire_data(var_name=None, slice_=(), **kwargs):
        """
        Acquires data based on the information in the request parameter.  The exact form of the request object is left up to the concrete implementation of the specific DataHandler
        @param var_name (optional) specifies the name of the variable for which to retrieve data; if empty, all variables returned
        @param slice_ (optional) a tuple of slice objects that indicate how to subset the ordered dimensions; if empty, no subsetting performed

        @retval a generator object that returns a tuple of objects on each iteration (objects TBD)
        """

    def acquire_new_data(**kwargs):
        """
        Acquires all new data from the source.

        @retval a generator object that returns a tuple of objects on each iteration (objects TBD)
        """
    def acquire_data_by_request(request=None, **kwargs):
        """
        Acquires all data based on the information in the request param.
        The request param should contain an object specific to the ExternalDataHandler implementation

        @param request an object containing request information specific to the ExternalDataHandler implementation

        @retval a generator object that returns a tuple of objects on each iteration (objects TBD)
        """

    def get_attributes(scope=None):
        """
        Gets the attributes based on the indicated scope

        @param scope An indicator of scope used by the concrete implementation to determine if "global" or "variable" attributes are to be returned

        @retval a dictionary containing the name/value pairs for all attributes in the given scope.
        """

    def get_fingerprint(recalculate=False, data_sampling=None, **kwargs):
        """
        Generates a "_fingerprint" of the dataset referenced by this ExternalDataHandler.
        It's precise form is determined by the concrete *ExternalDataHandler class.

        The _fingerprint should describe the data set in enough detail that changes
        can be detected by comparison of fingerprints.

        @param recalculate If True, the method should force recalculation of the _fingerprint
        @param data_sampling Indicates the type of data sampling that should be used when generating the _fingerprint - a member of the BaseExternalDataHandler.DATA_SAMPLING* attribute set should be used.  If BaseExternalDataHandler.DATA_SAMPLING_SHOTGUN is provided, a kwarg ==> BaseExternalDataHandler.DATA_SAMPLING_SHOTGUN_COUNT: (int) should also be provided

        @retval The _fingerprint for the dataset referenced by this ExternalDataHandler
        """

    def compare(dataset_fingerprint=""):
        """
        Compares the _fingerprint of this instance of ExternalDataHandler with the passed instance.
        At a minimum, the compare method should return an indicator that can be used to determine if there
        is new data available from the ExternalDataProducer.


        """
    def close():
        """
        Closes any open network connections and releases any resources used by this DataHandler.
        This method is called when the ExternalAgent process is terminated and should completely clean-up anything in use by this DataHandler
        """

    def scan():
        """
        Scans the dataset to get the list of parameters when registering a new datasource.
        """

    def get_variable_data(key=''):
        """
        Returns an array of the data referenced by a particular key
        """