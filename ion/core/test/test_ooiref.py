#!/usr/bin/env python

__author__ = 'Michael Meisinger'

from nose.plugins.attrib import attr

from pyon.util.unit_test import IonUnitTestCase
from pyon.core.exception import BadRequest, Conflict, Inconsistent, NotFound
from pyon.public import PRED, RT, IonObject, OT

from ion.core.ooiref import OOIReferenceDesignator

@attr('UNIT', group='coi')
class TestOOIReferenceDesignator(IonUnitTestCase):

    rd_list = [
        ('', dict(valid=False)),
        ('WR*NG', dict(valid=False)),
        ('WRONGWRONG', dict(valid=False)),
        ('WRONGWRONGWRONGWRONGWRONGWR', dict(valid=False)),
        ('WRONGWRO-NGWRO-01-RONGWRONG', dict(valid=False)),
        ('CE01ISSM-MF004-01-DOSTAD999', dict(valid=True, rdtype="asset", rdsub="instrument", mio="EA")),
        ('CP01ISSM-MF004-01-DOSTAD999', dict(valid=True, rdtype="asset", rdsub="instrument", mio="CG")),
        ('GI01ISSM-MF004-01-DOSTAD999', dict(valid=True, rdtype="asset", rdsub="instrument", mio="CG")),
        ('RS01ISSM-MF004-01-DOSTAD999', dict(valid=True, rdtype="asset", rdsub="instrument", mio="RSN")),
        ('CI01ISSM-MF004-01-DOSTAD999', dict(valid=True, rdtype="asset", rdsub="instrument", mio="CI")),
        ('XX01ISSM-MF004-01-DOSTAD999', dict(valid=True, rdtype="asset", rdsub="instrument", mio="OOI")),
        ('CTDBP', dict(valid=True, rdtype="inst_class")),
        ('PRESWAT', dict(valid=True, rdtype="dataproduct")),
    ]

    def test_reference_designator(self):
        for rd, attrs in self.rd_list:
            ooi_rd = OOIReferenceDesignator(rd)
            is_valid = attrs.get("valid", True)

            str(ooi_rd)
            repr(ooi_rd)

            self.assertEquals(ooi_rd.error, not is_valid)
            if not is_valid:
                continue

            rd_type = attrs["rdtype"]

            self.assertEquals(ooi_rd.rd_type, rd_type)

            if rd_type == "asset":
                rd_subtype = attrs["rdsub"]
                self.assertEquals(ooi_rd.rd_subtype, rd_subtype)
                if rd_subtype == "instrument":
                    self.assertEquals(ooi_rd.inst_rd, rd[:27])
                    self.assertEquals(ooi_rd.series_rd, rd[18:24])
                    self.assertEquals(ooi_rd.subseries_rd, rd[18:24] + "01")

                if rd_subtype in ("instrument", "port"):
                    self.assertEquals(ooi_rd.port_rd, rd[:17])

                if rd_subtype in ("instrument", "port", "node"):
                    self.assertEquals(ooi_rd.node_rd, rd[:14])

                if rd_subtype in ("instrument", "port", "node", "subsite"):
                    self.assertEquals(ooi_rd.subsite_rd, rd[:8])

                if rd_subtype in ("instrument", "port", "node", "subsite", "site"):
                    self.assertEquals(ooi_rd.site_rd, rd[:4])
                    self.assertEquals(ooi_rd.array, rd[:2])

                mio = attrs["mio"]
                self.assertEquals(ooi_rd.marine_io, mio)

            elif rd_type == "inst_class":
                self.assertEquals(ooi_rd.inst_class, rd[:5])

            elif rd_type == "dataproduct":
                self.assertEquals(ooi_rd.dataproduct, rd[:7])

