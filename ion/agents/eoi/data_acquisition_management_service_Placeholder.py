
__author__ = 'timgiguere'
__author__ = 'cmueller'

from interface.objects import ExternalDataset, ExternalDataProvider, DataSource, Institution, ContactInformation, DatasetDescription, UpdateDescription
from ion.agents.eoi.handler.dap_external_data_handler\
import DapExternalDataHandler
import os

HFR = "hfr"
HFR_LOCAL = "hfr_local"
KOKAGG = "kokagg"
AST2 = "ast2"
SSTA = "ssta"
GHPM = "ghpm"
COMP1 = "comp1"
COMP2 = "comp2"
COADS = "coads"
NCOM = "ncom"
NAVO_GLIDER_LOCAL = "navo_glider_local"
USGS = "usgs"

class DataAcquisitionManagementServicePlaceholder:

    def get_data_handler(self, ds_id=''):
        external_data_provider = self.read_external_data_provider(ds_id)
        data_source = self.read_data_source(ds_id)
        external_data_set = self.read_external_dataset(ds_id)

        protocol_type = data_source.protocol_type
        if protocol_type == "DAP":
            dsh = DapExternalDataHandler(external_data_provider,
                                         data_source,
                                         external_data_set)
        else:
            raise Exception("Unknown Protocol Type: %s" % protocol_type)

        return dsh

    def create_external_data_provider(self, external_data_provider=None):
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def read_external_data_provider(self, ds_id=''):
        dprov = ExternalDataProvider(institution=Institution(), contact=ContactInformation())
        if ds_id == HFR or ds_id == HFR_LOCAL:
#            dprov.institution.name = "HFRNET UCSD"
            pass
        elif ds_id == KOKAGG:
#            dprov.institution.name = "University of Hawaii"
            pass
        elif ds_id == AST2:
#            dprov.institution.name = "OOI CGSN"
            dprov.contact.name = "Robert Weller"
            dprov.contact.email = "rweller@whoi.edu"
        elif ds_id == SSTA:
#            dprov.institution.name = "Remote Sensing Systems"
            dprov.contact.email = "support@gmss.com"
        else:
            return None

        return dprov

    def delete_external_data_provider(self, ds_id=''):
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def update_external_data_provider(self, external_data_provider={}):
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def create_data_source(self, data_source=None):
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def read_data_source(self, ds_id=''):
        dsrc = DataSource(protocol_type="DAP", institution=Institution(), contact=ContactInformation())
        if ds_id == HFR:
            dsrc.connection_params["base_data_url"] = "http://hfrnet.ucsd.edu:8080/thredds/dodsC/"
        elif ds_id == HFR_LOCAL:
            pass
        elif ds_id == KOKAGG:
            dsrc.connection_params["base_data_url"] = "http://oos.soest.hawaii.edu/thredds/dodsC/"
        elif ds_id == AST2:
            dsrc.connection_params["base_data_url"] = "http://ooi.whoi.edu/thredds/dodsC/"
            dsrc.contact.name="Rich Signell"
            dsrc.contact.email = "rsignell@usgs.gov"
        elif ds_id == SSTA:
            dsrc.connection_params["base_data_url"] = "http://thredds1.pfeg.noaa.gov/thredds/dodsC/"
        elif ds_id == GHPM:
            pass
        elif ds_id == COMP1:
            pass
        elif ds_id == COMP2:
            pass
        elif ds_id == COADS:
            pass
        elif ds_id == NCOM:
            pass
        elif ds_id == NAVO_GLIDER_LOCAL:
            pass
        elif ds_id == USGS:
            pass
        else:
            return None

        return dsrc

    def delete_data_source(self, ds_id=''):
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def update_data_source(self, data_source={}):
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def create_external_dataset(self, external_data_set=None):
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def read_external_dataset(self, ds_id=''):
        CWD = os.getcwd()
        ext_ds = ExternalDataset(name="test", dataset_description=DatasetDescription(), update_description=UpdateDescription(), contact=ContactInformation())
        if ds_id == HFR:
            ext_ds.dataset_description.parameters
            ext_ds.dataset_description.parameters["dataset_path"] = "HFRNet/USEGC/6km/hourly/RTV"
            ext_ds.dataset_description.parameters["temporal_dimension"] = "time"
            ext_ds.dataset_description.parameters["zonal_dimension"] = "lon"
            ext_ds.dataset_description.parameters["meridional_dimension"] = "lat"
        elif ds_id == HFR_LOCAL:
            ext_ds.dataset_description.parameters["dataset_path"] = CWD + "/test_data/hfr.nc"
            ext_ds.dataset_description.parameters["temporal_dimension"] = "time"
            ext_ds.dataset_description.parameters["zonal_dimension"] = "lon"
            ext_ds.dataset_description.parameters["meridional_dimension"] = "lat"
        elif ds_id == KOKAGG:
            ext_ds.dataset_description.parameters["dataset_path"] = "hioos/hfr/kokagg"
            ext_ds.dataset_description.parameters["temporal_dimension"] = "time"
            ext_ds.dataset_description.parameters["zonal_dimension"] = "lon"
            ext_ds.dataset_description.parameters["meridional_dimension"] = "lat"
            ext_ds.contact.name = "Pierre Flament"
            ext_ds.contact.email = "pflament@hawaii.edu"
        elif ds_id == AST2:
            ext_ds.dataset_description.parameters["dataset_path"] = "ooi/AS02CPSM_R_M.nc"
            ext_ds.dataset_description.parameters["temporal_dimension"] = "time"
            ext_ds.dataset_description.parameters["zonal_dimension"] = "lon"
            ext_ds.dataset_description.parameters["meridional_dimension"] = "lat"
        elif ds_id == SSTA:
            ext_ds.dataset_description.parameters["dataset_path"] = "satellite/GR/ssta/1day"
            ext_ds.dataset_description.parameters["temporal_dimension"] = "time"
            ext_ds.dataset_description.parameters["zonal_dimension"] = "lon"
            ext_ds.dataset_description.parameters["meridional_dimension"] = "lat"
        elif ds_id == GHPM:
            ext_ds.dataset_description.parameters["dataset_path"] = CWD + "/test_data/ast2_ghpm_spp_ctd_1.nc"
            ext_ds.dataset_description.parameters["temporal_dimension"] = "time"
            ext_ds.dataset_description.parameters["zonal_dimension"] = "lon"
            ext_ds.dataset_description.parameters["meridional_dimension"] = "lat"
        elif ds_id == COMP1:
            ext_ds.dataset_description.parameters["dataset_path"] = CWD + "/test_data/ast2_ghpm_spp_ctd_1.nc"
            ext_ds.dataset_description.parameters["temporal_dimension"] = "time"
            ext_ds.dataset_description.parameters["zonal_dimension"] = "lon"
            ext_ds.dataset_description.parameters["meridional_dimension"] = "lat"
        elif ds_id == COMP2:
            ext_ds.dataset_description.parameters["dataset_path"] = CWD + "/test_data/ast2_ghpm_spp_ctd_2.nc"
            ext_ds.dataset_description.parameters["temporal_dimension"] = "time"
            ext_ds.dataset_description.parameters["zonal_dimension"] = "lon"
            ext_ds.dataset_description.parameters["meridional_dimension"] = "lat"
        elif ds_id == COADS:
            ext_ds.dataset_description.parameters["dataset_path"] = CWD + "/test_data/coads.nc"
            ext_ds.dataset_description.parameters["temporal_dimension"] = "time"
            ext_ds.dataset_description.parameters["zonal_dimension"] = "lon"
            ext_ds.dataset_description.parameters["meridional_dimension"] = "lat"
        elif ds_id == NCOM:
            ext_ds.dataset_description.parameters["dataset_path"] = CWD + "/test_data/ncom.nc"
            ext_ds.dataset_description.parameters["temporal_dimension"] = "time"
            ext_ds.dataset_description.parameters["zonal_dimension"] = "lon"
            ext_ds.dataset_description.parameters["meridional_dimension"] = "lat"
        elif ds_id == NAVO_GLIDER_LOCAL:
            ext_ds.dataset_description.parameters["dataset_path"] = CWD + "/test_data/p5150001.nc"
            ext_ds.dataset_description.parameters["temporal_dimension"] = "time"
            ext_ds.dataset_description.parameters["zonal_dimension"] = "lon"
            ext_ds.dataset_description.parameters["meridional_dimension"] = "lat"
        elif ds_id == USGS:
            ext_ds.dataset_description.parameters["dataset_path"] = CWD + "/test_data/usgs.nc"
            ext_ds.dataset_description.parameters["temporal_dimension"] = "time"
            ext_ds.dataset_description.parameters["zonal_dimension"] = "lon"
            ext_ds.dataset_description.parameters["meridional_dimension"] = "lat"
        else:
            return None

        return ext_ds

    def delete_external_dataset(self, ds_id=''):
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def update_external_dataset(self, external_data_set={}):
        # Return Value
        # ------------
        # {success: true}
        #
        pass
