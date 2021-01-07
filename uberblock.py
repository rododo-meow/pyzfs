import struct
from blkptr import BlkPtr

class Uberblock:
    @staticmethod
    def at(dev, off):
        me = Uberblock()
        me.dev = dev
        me.off = off
        me.magic, me.version, me.txg, me.guid_sum, me.timestamp = struct.unpack("<QQQQQ", dev.read(off, 40))
        me.rootbp = BlkPtr.at(dev, off + 40)
        return me

    def __str__(self):
        return ("""magic = %016x
version = %016x
txg = %d
guid_sum = %016x
timestamp = %d
""" % (self.magic, self.version, self.txg, self.guid_sum, self.timestamp)) + \
            str(self.rootbp)
