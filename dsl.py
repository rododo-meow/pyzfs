import struct
from blkptr import BlkPtr
from dnode import Dnode

class DslDataset(Dnode):
    @staticmethod
    def promote(parent, s):
        ds = DslDataset()
        ds.inherit(parent)
        return ds

    def __str__(self):
        return Dnode.__str__(self) + "\n" + """CREATE: %x
ACTIVE DATASET: %d""" % (self.bonus.creation_time, self.bonus.head_datset_obj)

    def get_active_dataset(self):
        return self.bonus.head_datset_obj
Dnode.PROMOTE[12] = DslDataset.promote

class BonusDslDir:
    @staticmethod
    def frombytes(s):
        bonus = BonusDslDir()
        bonus.creation_time, bonus.head_datset_obj, bonus.parent_obj, bonus.clone_parent_obj, \
                bonus.child_dir_zapobj, bonus.used_bytes, bonus.compressed_bytes, \
                bonus.uncompressed_bytes, bonus.quota, bonus.reserved, bonus.props_zapobj = \
            struct.unpack_from("<11Q", s)
        return bonus
Dnode.BONUS[12] = BonusDslDir.frombytes

class BonusDslDataset:
    @staticmethod
    def frombytes(s):
        bonus = BonusDslDataset()
        bonus.dir_obj, bonus.prev_snap_obj, bonus.prev_snap_txg, bonus.next_snap_obj, \
                bonus.snapnames_zapobj, bonus.num_children, bonus.creation_time, \
                bonus.creation_txg, bonus.deadlist_obj, bonus.used_bytes, \
                bonus.compressed_bytes, bonus.uncompressed_bytes, bonus.unique_bytes, \
                bonus.fsid_guid, bonus.guid, bonus.flags = \
            struct.unpack_from("<16Q", s)
        bonus.bp = BlkPtr.frombytes(s[128:])
        return bonus

    def __str__(self):
        return """DIR_OBJ: %d
BP: %s""" % (self.dir_obj, self.bp.summary())
Dnode.BONUS[16] = BonusDslDataset.frombytes
