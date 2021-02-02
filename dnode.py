from blkptr import BlkPtr
import struct
import dmu_constant
import util

class Dnode:
    SIZE = 512

    @staticmethod
    def frombytes(s, pool=None):
        dnode = Dnode()
        dnode.pool = pool
        dnode.type, dnode.indblkshift, dnode.nlevels, dnode.nblkptr, dnode.bonustype, \
                dnode.checksum, dnode.compress, dnode.flags, dnode.datablkszsec, dnode.bonuslen, \
                dnode.extra_slots, pad, dnode.maxblkid, dnode.secphys = struct.unpack_from("<BBBBBBBBHHB3sQQ", s)
        dnode.blkptr = [None] * dnode.nblkptr
        for i in range(dnode.nblkptr):
            dnode.blkptr[i] = BlkPtr.frombytes(s[64 + i * 128:64 + i * 128 + 128])
        dnode.bonus = s[64 + dnode.nblkptr * 128:64 + dnode.nblkptr * 128 + dnode.bonuslen]
        if dnode.bonustype != 0:
            if dnode.bonustype >= len(Dnode.BONUS):
                print("Bonus " + str(dnode.bonustype) + " not implemented")
            elif type(Dnode.BONUS[dnode.bonustype]) == str:
                print("Bonus " + Dnode.BONUS[dnode.bonustype] + " not implemented")
            else:
                dnode.bonus = Dnode.BONUS[dnode.bonustype](dnode.bonus)
                dnode.bonus.dnode = dnode
        if dnode.type >= len(Dnode.PROMOTE):
            raise NotImplementedError("Dnode type " + str(dnode.type) + " not implemented")
        if type(Dnode.PROMOTE[dnode.type]) == str:
            raise NotImplementedError(Dnode.PROMOTE[dnode.type] + " not implemented")
        return Dnode.PROMOTE[dnode.type](dnode, s)

    def inherit(self, parent):
        self.type, self.indblkshift, self.nlevels, self.nblkptr, self.bonustype, \
                self.checksum, self.compress, self.flags, self.datablkszsec, self.bonuslen, \
                self.extra_slots, self.maxblkid, self.secphys, self.blkptr, self.pool, self.bonus = \
        parent.type, parent.indblkshift, parent.nlevels, parent.nblkptr, parent.bonustype, \
                parent.checksum, parent.compress, parent.flags, parent.datablkszsec, parent.bonuslen, \
                parent.extra_slots, parent.maxblkid, parent.secphys, parent.blkptr, parent.pool, parent.bonus

    def __str__(self):
        s = """TYPE: %s
INDBLKSHIFT: %d
NLEVELS: %d
NBLKPTR: %d
BONUSTYPE: %d
CHECKSUM: %d
COMPRESS: %d
FLAGS: %x
DATABLKSZSEC: %x
BONUSLEN: %d
MAXBLKID: %x
SECPHYS: %x
""" % (dmu_constant.TYPES[self.type],
        self.indblkshift,
        self.nlevels,
        self.nblkptr,
        self.bonustype,
        self.checksum,
        self.compress,
        self.flags,
        self.datablkszsec * 512,
        self.bonuslen,
        self.maxblkid,
        self.secphys)
        for i in range(self.nblkptr):
            s += "PTR[%d]: \n%s\n" % (i, util.shift(str(self.blkptr[i]), 1))
        if self.bonus != None:
            s += str(self.bonus) + "\n"
        return s[:-1]

Dnode.PROMOTE = dmu_constant.TYPES.copy()
Dnode.PROMOTE += [None] * (256 - len(Dnode.PROMOTE))
Dnode.PROMOTE[10] = lambda x, y: x # DNODE

Dnode.BONUS = [None] * 45
Dnode.BONUS[4] = "PACKED_NVLIST_SIZE"
Dnode.BONUS[7] = "SPACE_MAP_HEADER"
Dnode.BONUS[17] = "ZNODE"
Dnode.BONUS[44] = "SA"