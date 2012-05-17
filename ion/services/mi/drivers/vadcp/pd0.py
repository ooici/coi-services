#!/usr/bin/env python

"""
@package 
@file 
@author Carlos Rueda
@brief 
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'


ID_HEADER = 0x7F
ID_DATA_SOURCE = 0x7F
ID_FIXED_LEADER = 0x0000
ID_VELOCITY = 0x0100
ID_CORRELATION_MAGNITUDE = 0x0200
ID_ECHO_INTENSITY = 0x0300
ID_PERCENT_GOOD = 0x0400
ID_VARIABLE_LEADER = 0x0080
DATATYPE_FIXED_LEADER = 1
DATATYPE_VARIABLE_LEADER = 2

class InvalidEnsemble(Exception):
    pass

class PD0DataStructure(object):
    """
    Encapsulates access to the workhorses binary data format.
    Adapted from SIAM.
    """

    def __init__(self, data):
        if data is None:
            raise ValueError("data is None")

        data = bytearray(data)

        def error(m):
            raise InvalidEnsemble("Invalid workhorse data ensemble: %s" % m)

        if data[0] != ID_HEADER :
            error("data[0]=%d is not %d" % (data[0], ID_HEADER))

        if data[1] != ID_DATA_SOURCE :
            error("data[1]=%d is not %d" % (data[1], ID_DATA_SOURCE))

        header_len = self.getHeaderLength(data)
        if header_len + 2 >= len(data):
            raise InvalidEnsemble("index header_len + 2 = %d out of range" %
                                  (header_len + 2))

        if self.getShort(header_len + 1, data) != ID_FIXED_LEADER:
            self.data = data
        else:
            raise ValueError("Not a valid workhorse data ensemble")

    def __str__(self):
        s = []
        s.append("NumberOfBytesInEnsemble = %d" % \
                     self.getNumberOfBytesInEnsemble())
        s.append("HeaderLength = %d" % \
                     self.getHeaderLength())
        s.append("NumberOfBeams = %d" % self.getNumberOfBeams())
        s.append("NumberOfDataTypes = %d" % self.getNumberOfDataTypes())
        for i in range(self.getNumberOfDataTypes()):
            t = i + 1
            s.append("OffsetForDataType %d = %d" %
                     (t, self.getOffsetForDataType(t)))

        return "\n".join(s)

    def getShort(self, index, data=None):
        data = data or self.data
        lsb = int(data[index] & 0xFF)
        msb = int(data[index + 1] & 0xFF)
        return int(lsb | (msb << 8))

    def getNumberOfDataTypes(self, data=None):
        data = data or self.data
        if 5 >= len(data):
            raise InvalidEnsemble("index 5 out of range")
        return int(data[5])

    def getHeaderLength(self, data=None):
        data = data or self.data
        return 2 * self.getNumberOfDataTypes(data) + 6

    def getNumberOfBytesInEnsemble(self, data=None):
        data = data or self.data
        return self.getShort(2, data)

    def getOffsetForDataType(self, i):
        """
         * @param i The data type number this can range form 1-6. The exact upper
         *          limit is returned
         * @return The offset for data type #i. Adding '1' to this offset number
         *         gives the absolute byte number in the ensemble where data type #i
         *         begins.
        """
        ndt = self.getNumberOfDataTypes()
        if i <= 0 or i > ndt:
            raise ValueError("Invalid i=%d value not in [1..%d]" % (i, ndt))
        return self.getShort(6 + (i - 1) * 2)

    #######################
    # Fixed Leader Info
    #######################

    def getOffsetForFixedLeader(self):
        return int(self.getOffsetForDataType(DATATYPE_FIXED_LEADER))

    def getNumberOfBeams(self):
        return int(self.data[self.getOffsetForFixedLeader() + 8]) & 0xFF

    def getNumberOfCells(self):
        return int(self.data[self.getOffsetForFixedLeader() + 9])

    def getPingsPerEnsemble(self):
        return self.getShort(self.getOffsetForFixedLeader() + 10)

    def getDepthCellLength(self):
        return self.getShort(self.getOffsetForFixedLeader() + 12)

    def getErrorVelocityMaximum(self):
        return self.getShort(self.getOffsetForFixedLeader() + 20)

    def getBinOneDistance(self):
        return self.getShort(self.getOffsetForFixedLeader() + 32)

    #######################
    # Variable Leader Info
    #######################

    def getOffsetForVariableLeader(self):
        return int(self.getOffsetForDataType(DATATYPE_VARIABLE_LEADER))

    def getSpeedOfSound(self):
        """
        @return speed of sound (m/s)
        """
        return self.getShort(self.getOffsetForVariableLeader() + 14)

    def getDepthOfTransducer(self):
        """
        @return depth in meters
        """
        return self.getShort(self.getOffsetForVariableLeader() + 14) * 0.1

    def getHeading(self):
        """
        @return heading in degrees (0 -360)
        """
        return self.getShort(self.getOffsetForVariableLeader() + 18) * 0.01

    def getPitch(self):
        """
        @return pitch in degrees (-20 - 20)
        """
        return self.getShort(self.getOffsetForVariableLeader() + 20) * 0.01

    def getRoll(self):
        """
        @return roll in degrees(-20 - 20)
        """
        return self.getShort(self.getOffsetForVariableLeader() + 22) * 0.01

    def getSalinity(self):
        """
        @return roll in degrees(-20 - 20)
        """
        return self.getShort(self.getOffsetForVariableLeader() + 24)

    def getTemperature(self):
        """
        @return temperature in celsius (-5 - 40 degrees)
        """
        return self.getShort(self.getOffsetForVariableLeader() + 26) * 0.01

    #######################
    # Variable Leader Info
    #######################

    def getVelocity(self, beam):
        """
         @param beam The beam number to return (values are 1-4)
         @return An array of velocities for the beam. (mm/s along beam axis).
                  None is returned if no velocity data was found
        """
        if beam < 1 or beam > 4:
            raise ValueError(
                    "Beam number must be between 1 and 4. You specified %s" %
                    beam)

        
