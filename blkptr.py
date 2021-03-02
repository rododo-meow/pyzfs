import struct
import dmu_constant
import binascii

class DVA:
    @staticmethod
    def frombytes(s):
        me = DVA()
        me.raw = struct.unpack("<QQ", s)
        me.vdev = me.raw[0] >> 32
        me.offset = (me.raw[1] & 0x7fffffffffffffff) * 512
        me.asize = (me.raw[0] & 0xffffff) * 512
        me.gang = (me.raw[1] >> 63) != 0
        return me

    def __str__(self):
        return ("G:" if self.gang else "") + "%d:%x:%x" % (self.vdev, self.offset, self.asize)

class BlkPtr:
    ETYPE_DATA = 0
    SIZE = 128

    @staticmethod
    def frombytes(s):
        me = BlkPtr()
        me.raw = s
        tmp = struct.unpack("<Q", me.raw[48:56])[0]
        me.embedded = ((tmp >> 39) & 1) == 1
        if me.embedded:
            me.etype = (tmp >> 40) & 0xff
            me.lsize = (tmp & 0x1fff) + 1
            me.psize = ((tmp >> 25) & 0x7f) + 1
            me.comp = (tmp >> 32) & 0x7f
        else:
            me.dva = [None] * 3
            me.dva[0] = DVA.frombytes(me.raw[0:16])
            me.dva[1] = DVA.frombytes(me.raw[16:32])
            me.dva[2] = DVA.frombytes(me.raw[32:48])
            me.lsize = ((tmp & 0xffff) + 1) * 512
            me.psize = (((tmp >> 16) & 0xffff) + 1) * 512
            me.comp = (tmp >> 32) & 0x7f
            me.cksum = (tmp >> 40) & 0xff
            me.type = (tmp >> 48) & 0xff
            me.lvl = (tmp >> 56) & 0x7f
            me.endian = tmp >> 63
            me.fill = struct.unpack("<Q", me.raw[88:96])[0]
            me.checksum = me.raw[96:128]
        me.birth = struct.unpack("<Q", me.raw[80:88])[0]
        return me

    @staticmethod
    def at(dev, off):
        return BlkPtr.frombytes(dev.read(off, 128))

    def ref_to(self, off):
        return self.dva[0].offset == off or self.dva[1].offset == off or self.dva[2].offset == off

    def decode(self):
        if not self.embedded:
            raise TypeError("Not an embedded blkptr")
        if self.etype != BlkPtr.ETYPE_DATA:
            raise TypeError("Invalid embedded blkptr")
        data = [0] * self.psize
        j = 0
        for i in range(self.psize):
            if i % 8 == 0:
                if j > len(self.raw):
                    raise IndexError("Bad embedded blkptr")
                w, = struct.unpack_from("<Q", self.raw[j:j + 8])
                j += 8
                if j == 48 or j == 80:
                    j += 8
            data[i] = (w >> ((i % 8) * 8)) & 0xff
        data = bytes(data)
        return data

    def __str__(self):
        if self.embedded:
            return "EMBEDDED"
        else:
            return """DVA[0]: %s
DVA[1]: %s
DVA[2]: %s
LSIZE: %x PSIZE: %x
ENDIAN: %s TYPE: %s
BIRTH: %d LEVEL: %d FILL: %d
EMBEDDED: %s
CKFUNC: %s COMP: %s(%x)
CKSUM: %s""" % (self.dva[0], self.dva[1], self.dva[2],
                    self.lsize, self.psize,
                    ["BIG", "LITTLE"][self.endian], dmu_constant.TYPES[self.type],
                    self.birth, self.lvl, self.fill,
                    "TRUE" if self.embedded else "FALSE",
                    dmu_constant.CKFUNC[self.cksum] if self.cksum < len(dmu_constant.CKFUNC) else self.cksum, dmu_constant.COMP[self.comp] if self.comp < len(dmu_constant.COMP) else self.comp, self.comp,
                    binascii.b2a_hex(self.checksum))

    def summary(self):
        return "%s %xL/%xP F=%d B=%d cksum=%s" % (self.dva[0], self.lsize, self.psize, self.fill, self.birth, binascii.b2a_hex(self.checksum).decode('ascii'))
