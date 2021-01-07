import struct
from blkptr import BlkPtr
import dmu_constant
import util
import binascii

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
            if type(Dnode.BONUS[dnode.bonustype]) == str:
                raise NotImplementedError("Bonus " + Dnode.BONUS[dnode.bonustype] + " not implemented")
            dnode.bonus = Dnode.BONUS[dnode.bonustype](dnode.bonus)
            dnode.bonus.dnode = dnode
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
        return s[:-1]

class ZilHeader:
    SIZE = 192

    @staticmethod
    def frombytes(s):
        header = ZilHeader()
        header.claim_txg, header.replay_seq, header.log_ptr, header.claim_blk_seq, header.flags, header.claim_lr_seq = struct.unpack_from("<QQ128sQQQ", s)
        header.log_ptr = BlkPtr.frombytes(header.log_ptr)
        return header

class ObjSet:
    @staticmethod
    def frombytes(s):
        objset = ObjSet()
        objset.metadnode = Dnode.frombytes(s)
        objset.zil_header = ZilHeader.frombytes(s[Dnode.SIZE:])
        objset.type, objset.flags, objset.portable_mac, objset.local_mac = struct.unpack_from("<QQ32s32s", s[Dnode.SIZE + ZilHeader.SIZE:])
        return objset

    def __str__(self):
        return str(self.metadnode) + "\n" + str(self.zil_header) + "\n" + "OS_TYPE: %d" % (self.type)

    def read_object(self, objid):
        blkid = objid * Dnode.SIZE // (self.metadnode.datablkszsec * 512)
        dnode = self.pool.read_object(self.metadnode, blkid)
        dnode = dnode[objid * Dnode.SIZE % (self.metadnode.datablkszsec * 512):]
        dnode = dnode[:Dnode.SIZE]
        dnode = Dnode.frombytes(dnode, self.pool)
        return dnode

class ZAPLeafChunk:
    SIZE = 24
    CHUNK_ENTRY = 252
    CHUNK_ARRAY = 251
    CHUNK_FREE = 253

    @staticmethod
    def frombytes(s):
        chunk = ZAPLeafChunk()
        chunk.type = s[0]
        if chunk.type == ZAPLeafChunk.CHUNK_ENTRY:
            chunk.type, chunk.int_size, chunk.next, chunk.name_chunk, \
                    chunk.name_len, chunk.value_chunk, chunk.value_len, chunk.cd, \
                    pad, chunk.hash = struct.unpack("<BBHHHHHH2sQ", s)
        elif chunk.type == ZAPLeafChunk.CHUNK_ARRAY:
            chunk.type, chunk.array, chunk.next = struct.unpack("<B21sH", s)
        elif chunk.type == ZAPLeafChunk.CHUNK_FREE:
            chunk.type, pad, chunk.next = struct.unpack("<B21sH", s)
        return chunk

    def get_name(self):
        if self.type != ZAPLeafChunk.CHUNK_ENTRY:
            raise TypeError("Not an entry chunk")
        data = b''
        p = self.name_chunk
        while len(data) < self.name_len:
            if self.leaf.chunks[p].type != ZAPLeafChunk.CHUNK_ARRAY:
                raise TypeError("Linked to a non-array chunk")
            data += self.leaf.chunks[p].array
            p = self.leaf.chunks[p].next
        return data[:self.name_len]

    def get_value(self):
        if self.type != ZAPLeafChunk.CHUNK_ENTRY:
            raise TypeError("Not an entry chunk")
        data = b''
        p = self.value_chunk
        while len(data) < self.value_len * self.int_size:
            if p == 0xffff:
                raise ValueError("Reach the end of chunk chain")
            if self.leaf.chunks[p].type != ZAPLeafChunk.CHUNK_ARRAY:
                raise TypeError("Linked to a non-array chunk")
            data += self.leaf.chunks[p].array
            p = self.leaf.chunks[p].next
        if self.int_size == 1:
            return data[:self.value_len * self.int_size]
        elif self.int_size == 8:
            return [struct.unpack(">Q", data[i*8:i*8+8])[0] for i in range(self.value_len)]

class ZAPLeaf:
    @staticmethod
    def frombytes(s):
        leaf = ZAPLeaf()
        leaf.block_type, leaf.next, leaf.prefix, leaf.magic, leaf.nfree, leaf.nentries, leaf.prefix_len, leaf.freelist = \
                struct.unpack_from("<QQQIHHHH", s)
        nhash = len(s) // 32
        leaf.hash = struct.unpack_from("<" + str(nhash) + "H", s[48:])
        chunk_data = s[48 + 2 * nhash:]
        leaf.chunks = [None] * (len(chunk_data) // ZAPLeafChunk.SIZE)
        for i in range(len(chunk_data) // ZAPLeafChunk.SIZE):
            leaf.chunks[i] = ZAPLeafChunk.frombytes(chunk_data[i * ZAPLeafChunk.SIZE:(i + 1) * ZAPLeafChunk.SIZE])
            leaf.chunks[i].leaf = leaf
        return leaf

    def list(self):
        result = {}
        for i in range(len(self.chunks)):
            if self.chunks[i].type == ZAPLeafChunk.CHUNK_ENTRY:
                result[self.chunks[i].get_name()] = self.chunks[i].get_value()
        return result

class FatZAP:
    SIZE = 128 * 1024

    @staticmethod
    def frombytes(s):
        zap = FatZAP()
        zap.block_type, zap.magic, zap.blk, zap.numblks, zap.shift, \
                zap.nextblk, zap.blks_copied, zap.freeblk, zap.num_leafs, \
                zap.num_entries, zap.salt = struct.unpack_from("<QQQQQQQQQQQ", s)
        if zap.numblks == 0:
            tbl_len = len(s) // 2
            zap.pointer_tbl = struct.unpack_from("<" + str(tbl_len // 8) + "Q", s[-tbl_len:])
        return zap

    def __str__(self):
        return """MAGIC: %016x
BLK: %d
NUMBLKS: %d
SHIFT: %d
NEXTBLK: %d
BLKS_COPIED: %d
FREEBLK: %d
NUM_LEAFS: %d
NUM_ENTRIES: %d
SALT: %016x""" % (self.magic, self.blk, self.numblks, self.shift, self.nextblk, self.blks_copied, self.freeblk, self.num_leafs, self.num_entries, self.salt)

    def list(self):
        visited = []
        result = {}
        for i in range(len(self.pointer_tbl)):
            blkid = self.pointer_tbl[i]
            if blkid != 0 and not blkid in visited:
                leaf = self.pool.read_object(self.obj, blkid)
                leaf = ZAPLeaf.frombytes(leaf)
                result.update(leaf.list())
                visited += [blkid]
        return result

    def get(self, name):
        # TODO: use hash
        try:
            return self.list()[name.encode('ascii') + b'\0']
        except KeyError:
            return None

class ZAPObj(Dnode):
    ZBT_MICRO = 0x8000000000000003
    ZBT_HEADER = 0x8000000000000001
    ZBT_LEAF = 0x8000000000000000

    @staticmethod
    def promote(parent, s):
        zap = ZAPObj()
        zap.inherit(parent)
        return zap

    def inherit(self, parent):
        Dnode.inherit(self, parent)
        self.zap_type, = struct.unpack("<Q", self.pool.read_object(self, 0)[:8])
        if self.zap_type == ZAPObj.ZBT_HEADER:
            data = self.pool.read_object(self, 0)
            self.fatzap = FatZAP.frombytes(data)
            self.fatzap.pool = self.pool
            self.fatzap.obj = self

    def __str__(self):
        s = Dnode.__str__(self) + "\n"
        return s
        if self.zap_type == ZAPObj.ZBT_MICRO:
            s += "TYPE: MICRO"
        elif self.zap_type == ZAPObj.ZBT_HEADER:
            s += "TYPE: HEADER"
        elif self.zap_type == ZAPObj.ZBT_LEAF:
            s += "TYPE: LEAF"
        else:
            s += "TYPE: UNKNOWN"
        if self.zap_type == ZAPObj.ZBT_HEADER:
            s += "\n" + str(self.fatzap)
        return s

    def list(self):
        if self.zap_type == ZAPObj.ZBT_HEADER:
            return self.fatzap.list()

    def get(self, name):
        if self.zap_type == ZAPObj.ZBT_HEADER:
            return self.fatzap.get(name)

class ObjDir(ZAPObj):
    @staticmethod
    def promote(parent, s):
        objdir = ObjDir()
        ZAPObj.inherit(objdir, parent)
        return objdir

class DslDataset(Dnode):
    @staticmethod
    def promote(parent, s):
        ds = DslDataset()
        ds.inherit(parent)
        return ds

    def __str__(self):
        return "CREATE: %x" % (self.bonus.creation_time)

class BonusDslDir:
    @staticmethod
    def frombytes(s):
        bonus = BonusDslDir()
        bonus.creation_time, bonus.head_datset_obj, bonus.parent_obj, bonus.clone_parent_obj, \
                bonus.child_dir_zapobj, bonus.used_bytes, bonus.compressed_bytes, \
                bonus.uncompressed_bytes, bonus.quota, bonus.reserved, bonus.props_zapobj = \
            struct.unpack_from("<11Q", s)
        return bonus

Dnode.PROMOTE = (
        "NONE",
        ObjDir.promote,
        "OBJ_ARR",
        "NVLIST",
        "NVLIST_SIZE",
        "BPLIST",
        "BPLIST_HDR",
        "SPACE_MAP_HEADER",
        "SPACE_MAP",
        "ILOG",
        lambda x, y: x,
        lambda x, y: x,
        DslDataset.promote,
        "DSL_DATASET_CHILD_MAP",
        "OBJSET_SNAP_MAP",
        "DSL_PROPS",
        "DSL_OBJSET",
        "ZNODE",
        "ACL",
        "FILE_CONTENT",
        "DIR_CONTENT",
        "MASTER_NODE",
        "DELETE_QUEUE",
        "ZVOL",
        "ZVOL_PROP"
)

Dnode.BONUS = [None] * 18
Dnode.BONUS[4] = "PACKED_NVLIST_SIZE"
Dnode.BONUS[7] = "SPACE_MAP_HEADER"
Dnode.BONUS[12] = BonusDslDir.frombytes
Dnode.BONUS[16] = "DSL_DATASET"
Dnode.BONUS[17] = "ZNODE"