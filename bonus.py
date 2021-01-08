from blkptr import BlkPtr
import struct

class BonusDslDir:
    @staticmethod
    def frombytes(s):
        bonus = BonusDslDir()
        bonus.creation_time, bonus.head_datset_obj, bonus.parent_obj, bonus.clone_parent_obj, \
                bonus.child_dir_zapobj, bonus.used_bytes, bonus.compressed_bytes, \
                bonus.uncompressed_bytes, bonus.quota, bonus.reserved, bonus.props_zapobj = \
            struct.unpack_from("<11Q", s)
        return bonus

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