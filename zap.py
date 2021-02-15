from dnode import Dnode
import struct

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
                result[self.chunks[i].get_name().decode('utf8')] = self.chunks[i].get_value()
        return result

class FatZAP:
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
                leaf = self.pool.read_block(self.obj, blkid)
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

class MicroZAPEntry:
    @staticmethod
    def frombytes(s):
        entry = MicroZAPEntry()
        entry.value, entry.cd, pad, entry.name = struct.unpack("<QIH50s", s)
        entry.name = entry.name[:entry.name.find(b'\0') + 1]
        return entry

class MicroZAP:
    @staticmethod
    def frombytes(s):
        zap = MicroZAP()
        zap.block_type, zap.salt = struct.unpack_from("<QQ", s)
        zap.array = [None] * ((len(s) - 64) // 64)
        for i in range(len(zap.array)):
            zap.array[i] = MicroZAPEntry.frombytes(s[64 + i * 64:128 + i * 64])
        return zap

    def list(self):
        result = {}
        for entry in self.array:
            if entry.name[0] != b'\0':
                result[entry.name.decode('utf8')] = entry.value
        return result

    def get(self, name):
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
        data = self.pool.read_block(self, 0)
        self.zap_type, = struct.unpack("<Q", data[:8])
        if self.zap_type == ZAPObj.ZBT_HEADER:
            self.fatzap = FatZAP.frombytes(data)
            self.fatzap.pool = self.pool
            self.fatzap.obj = self
        elif self.zap_type == ZAPObj.ZBT_MICRO:
            self.microzap = MicroZAP.frombytes(data)
            self.microzap.pool = self.pool

    def __str__(self):
        s = Dnode.__str__(self) + "\n"
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
        elif self.zap_type == ZAPObj.ZBT_MICRO:
            return self.microzap.list()

    def get(self, name):
        if self.zap_type == ZAPObj.ZBT_HEADER:
            return self.fatzap.get(name)
        elif self.zap_type == ZAPObj.ZBT_MICRO:
            return self.microzap.get(name)
Dnode.PROMOTE[20] = ZAPObj.promote # DIR_CONTENT
Dnode.PROMOTE[21] = ZAPObj.promote # MASTER_NODE

class ObjDir(ZAPObj):
    @staticmethod
    def promote(parent, s):
        objdir = ObjDir()
        ZAPObj.inherit(objdir, parent)
        return objdir
Dnode.PROMOTE[1] = ObjDir.promote # OBJ_DIR
