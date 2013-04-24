#!/usr/bin/env python

"""Module providing OOI reference support, such as reference designator parsing."""

__author__ = 'Michael Meisinger'

import re


class OOIReferenceDesignator(object):
    """
    Parses an official OOI reference designator and provides accessors to constituent parts.
    """
    def __init__(self, rdstr):
        self.rd = rdstr
        self.error = False
        self.rd_type = None
        self.rd_subtype = None

        # Type asset
        # Example: CE01ISSM-MF004-01-DOSTAD999
        self.marine_io = None
        self.array, self.site, self.subsite, self.node_type, self.node_seq, self.port, self.inst_class, self.inst_series, self.inst_seq = None, None, None, None, None, None, None, None, None
        self.site_rd, self.subsite_rd, self.node_rd, self.port_rd, self.inst_rd, self.series_rd, self.subseries_rd = None, None, None, None, None, None, None
        # Type dataproduct
        self.dataproduct = None
        self.dataproduct_level = None

        if re.match('^[A-Z0-9]{5}$', rdstr):
            self.rd_type = "inst_class"
            self.inst_class = rdstr
        elif re.match('^[A-Z0-9_]{7}_(L\d)$', rdstr):
            self.rd_type = "dataproduct"
            self.rd_subtype = "level"
            self.dataproduct = rdstr[:7]
            self.dataproduct_level = rdstr[8:]
        elif re.match('^[A-Z0-9_]{7}$', rdstr):
            self.rd_type = "dataproduct"
            self.rd_subtype = "class"
            self.dataproduct = rdstr
        else:
            #              <array   >   <site >   <subs >   -<nodetype>   <nodeseq    >   -<port#      >   -<instclass  ><series  ><seq>
            m = re.match(r'^([A-Z]{2})(?:(\d{2})(?:(\w{4})(?:-([A-Z]{2})(?:([A-Z0-9]{3})(?:-([A-Z0-9]{2})(?:-([A-Z0-9]{5})([A-Z0-9])(\d{3})?)?)?)?)?)?)?$', rdstr)
            if m:
                self.rd_type = "asset"
                self.array, self.site, self.subsite, self.node_type, self.node_seq, self.port, self.inst_class, self.inst_series, self.inst_seq = m.groups()
                if self.inst_class:
                    self.inst_rd = rdstr
                    self.series_rd = self.inst_class + self.inst_series
                    self.subseries_rd = self.inst_class + self.inst_series + "01"  # !!! Underspecified !!!
                    self.rd_subtype = "instrument"
                if self.port:
                    self.port_rd = rdstr[:17]
                    self.rd_subtype = self.rd_subtype or "port"
                if self.node_type:
                    self.node_rd = rdstr[:14]
                    self.rd_subtype = self.rd_subtype or "node"
                if self.subsite:
                    self.subsite_rd = rdstr[:8]
                    self.rd_subtype = self.rd_subtype or "subsite"
                if self.site:
                    self.site_rd = rdstr[:4]
                    self.rd_subtype = self.rd_subtype or "site"
                self.rd_subtype = self.rd_subtype or "array"
                if self.array == "CI":
                    self.marine_io = "CI"
                elif self.array == "CE":
                    self.marine_io = "EA"
                elif self.array.startswith("C") or self.array.startswith("G"):
                    self.marine_io = "CG"
                elif self.array.startswith("R"):
                    self.marine_io = "RSN"
                else:
                    self.marine_io = "OOI"
            else:
                self.error = True

    def __str__(self):
        return self.rd

    def __repr__(self):
        return "OOIReferenceDesignator('%s')" % self.rd
