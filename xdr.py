import struct
import binascii
import util

class NVPair:
    DATA_TYPE_BOOLEAN = 1
    DATA_TYPE_BYTE = 2
    DATA_TYPE_INT16 = 3
    DATA_TYPE_UINT16 = 4
    DATA_TYPE_INT32 = 5
    DATA_TYPE_UINT32 = 6
    DATA_TYPE_INT64 = 7
    DATA_TYPE_UINT64 = 8
    DATA_TYPE_STRING = 9
    DATA_TYPE_BYTE_ARRAY = 10
    DATA_TYPE_INT16_ARRAY = 11
    DATA_TYPE_UINT16_ARRAY = 12
    DATA_TYPE_INT32_ARRAY = 13
    DATA_TYPE_UINT32_ARRAY = 14
    DATA_TYPE_INT64_ARRAY = 15
    DATA_TYPE_UINT64_ARRAY = 16
    DATA_TYPE_STRING_ARRAY = 17
    DATA_TYPE_HRTIME = 18
    DATA_TYPE_NVLIST = 19
    DATA_TYPE_NVLIST_ARRAY = 20
    DATA_TYPE_BOOLEAN_VALUE = 21
    DATA_TYPE_INT8 = 22
    DATA_TYPE_UINT8 = 23
    DATA_TYPE_BOOLEAN_ARRAY = 24
    DATA_TYPE_INT8_ARRAY = 25
    DATA_TYPE_UINT8_ARRAY = 26
    DATA_TYPE_DOUBLE = 27

class XDR:
    @staticmethod
    def frombytes(data):
        xdr = XDR()
        xdr.data = data
        return xdr

    def read(self, l):
        r = self.data[:l]
        if l % 4 != 0:
            l += (4 - l % 4)
        self.data = self.data[l:]
        return r

    def read_int(self):
        r, = struct.unpack_from(">i", self.data)
        self.data = self.data[4:]
        return r

    def read_uint(self):
        r, = struct.unpack_from(">I", self.data)
        self.data = self.data[4:]
        return r

    def read_uhint(self):
        r, = struct.unpack_from(">Q", self.data)
        self.data = self.data[8:]
        return r

    def read_string(self):
        l = self.read_uint()
        d = self.read(l)
        return d

    def advance(self, offset):
        self.data = self.data[offset:]

class XDRNVList:
    @staticmethod
    def fromxdr(xdr):
        nvl = XDRNVList()
        nvl.version = xdr.read_int()
        nvl.flag = xdr.read_uint()
        nvl.pairs = []
        while True:
            encoded_size = xdr.read_uint()
            decoded_size = xdr.read_uint()
            if encoded_size == 0 and decoded_size == 0:
                break
            nvp_data = xdr.read(encoded_size - 8)
            nvp_data = XDR.frombytes(nvp_data)
            name = nvp_data.read_string()
            nvp_type = nvp_data.read_int()
            nvp_nelem = nvp_data.read_int()
            nvp = NVPair()
            nvp.name = name
            nvp.type = nvp_type
            if nvp_nelem == 0:
                continue
            if nvp_type == NVPair.DATA_TYPE_UINT64:
                nvp.value = nvp_data.read_uhint()
            elif nvp_type == NVPair.DATA_TYPE_STRING:
                nvp.value = nvp_data.read_string()
            elif nvp_type == NVPair.DATA_TYPE_NVLIST:
                nvp.value = XDRNVList.frombytes(nvp_data.data)
            elif nvp_type == NVPair.DATA_TYPE_NVLIST_ARRAY:
                nvp.value = [None] * nvp_nelem
                for i in range(nvp_nelem):
                    nvp.value[i] = XDRNVList.fromxdr(nvp_data)
            nvl.pairs += [nvp]
        return nvl

    @staticmethod
    def frombytes(data):
        return XDRNVList.fromxdr(XDR.frombytes(data))

    def list(self):
        return [nvp.name.decode('ascii') for nvp in self.pairs]

    def get(self, name):
        if type(name) == str:
            name = name.encode('ascii')
        for nvp in self.pairs:
            if nvp.name == name:
                return nvp.value

    def __str__(self):
        s = ""
        for nvp in self.pairs:
            if nvp.type == NVPair.DATA_TYPE_NVLIST:
                s += nvp.name.decode('ascii') + ":\n"
                s += util.shift(str(nvp.value), 1) + "\n"
            elif nvp.type == NVPair.DATA_TYPE_NVLIST_ARRAY:
                s += nvp.name.decode('ascii') + ":\n"
                for i in range(len(nvp.value)):
                    s += util.shift("[%d]:" % (i), 1) + "\n"
                    s += util.shift(str(nvp.value[i]), 2) + "\n"
            else:
                s += "%s (%s): %s\n" % (nvp.name.decode('ascii'), nvp.type, nvp.value)
        return s[:-1]
