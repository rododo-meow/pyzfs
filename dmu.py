import struct
from blkptr import BlkPtr
from dnode import Dnode
import dmu_constant
import util
from dsl import DslDataset
from zap import ZAPObj
from bonus import BonusDslDataset, BonusDslDir

class ZilHeader:
    SIZE = 192

    @staticmethod
    def frombytes(s):
        header = ZilHeader()
        header.claim_txg, header.replay_seq, header.log_ptr, header.claim_blk_seq, header.flags, header.claim_lr_seq = struct.unpack_from("<QQ128sQQQ", s)
        header.log_ptr = BlkPtr.frombytes(header.log_ptr)
        return header

class ObjSet:
    OS_TYPE_NONE = 0
    OS_TYPE_META = 1
    OS_TYPE_ZFS = 2
    OS_TYPE_ZVOL = 3

    @staticmethod
    def frombytes(s):
        objset = ObjSet()
        objset.metadnode = Dnode.frombytes(s)
        objset.zil_header = ZilHeader.frombytes(s[Dnode.SIZE:])
        objset.type, objset.flags, objset.portable_mac, objset.local_mac = struct.unpack_from("<QQ32s32s", s[Dnode.SIZE + ZilHeader.SIZE:])
        return objset

    def get_os_type_str(self):
        if self.type == ObjSet.OS_TYPE_NONE:
            return "NONE"
        elif self.type == ObjSet.OS_TYPE_META:
            return "DSL"
        elif self.type == ObjSet.OS_TYPE_ZFS:
            return "ZPL"
        elif self.type == ObjSet.OS_TYPE_ZVOL:
            return "ZVOL"

    def __str__(self):
        return str(self.metadnode) + "\n" + str(self.zil_header) + "\n" + "OS_TYPE: %s(%d)" % (self.get_os_type_str(), self.type)

    def read_object(self, objid):
        blkid = objid * Dnode.SIZE // (self.metadnode.datablkszsec * 512)
        dnode = self.pool.read_block(self.metadnode, blkid)
        dnode = dnode[objid * Dnode.SIZE % (self.metadnode.datablkszsec * 512):]
        dnode = dnode[:Dnode.SIZE]
        dnode = Dnode.frombytes(dnode, self.pool)
        return dnode

class ObjDir(ZAPObj):
    @staticmethod
    def promote(parent, s):
        objdir = ObjDir()
        ZAPObj.inherit(objdir, parent)
        return objdir

Dnode.PROMOTE = dmu_constant.TYPES.copy()
Dnode.PROMOTE += [None] * (256 - len(Dnode.PROMOTE))
Dnode.PROMOTE[1] = ObjDir.promote
Dnode.PROMOTE[10] = lambda x, y: x
Dnode.PROMOTE[11] = lambda x, y: x
Dnode.PROMOTE[12] = DslDataset.promote
Dnode.PROMOTE[20] = ZAPObj.promote
Dnode.PROMOTE[21] = ZAPObj.promote
Dnode.PROMOTE[196] = lambda x, y: x

Dnode.BONUS = [None] * 45
Dnode.BONUS[4] = "PACKED_NVLIST_SIZE"
Dnode.BONUS[7] = "SPACE_MAP_HEADER"
Dnode.BONUS[12] = BonusDslDir.frombytes
Dnode.BONUS[16] = BonusDslDataset.frombytes
Dnode.BONUS[17] = "ZNODE"
Dnode.BONUS[44] = "SA"