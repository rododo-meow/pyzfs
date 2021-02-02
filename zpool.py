import zio
from blkptr import BlkPtr
from dmu import Dnode, ObjSet
import binascii

PARSER = (
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        Dnode.frombytes,
        ObjSet.frombytes,
)

class ZPool:
    def __init__(self, vdevs):
        self.vdevs = vdevs

    def read_raw(self, bp):
        return zio.read(self.vdevs, bp)

    def read_block(self, dnode, blkid, nblk = 1):
        if not isinstance(dnode, Dnode):
            raise TypeError("Require Dnode")
        curr_level = dnode.blkptr
        for i in range(dnode.nlevels - 1, -1, -1):
            data = b''
            for idx in range(len(curr_level)):
                ptr = curr_level[idx]
                start = idx << (i * (dnode.indblkshift - 7))
                end = (idx + 1) << (i * (dnode.indblkshift - 7))
                if end <= blkid or start >= blkid + nblk:
                    continue
                if ptr.birth == 0:
                    blk = b''
                else:
                    blk = self.read_raw(ptr)
                if i == 0:
                    blk += b'\0' * (len(blk) - dnode.datablkszsec * 512)
                else:
                    blk += b'\0' * (len(blk) - (1 << dnode.indblkshift))
                data += blk
            blkid = blkid & ((1 << (i * (dnode.indblkshift - 7))) - 1)
            if i != 0:
                curr_level = [None] * (len(data) // 128)
                for j in range(len(curr_level)):
                    curr_level[j] = BlkPtr.frombytes(data[j * 128:j * 128 + 128])
        if len(data) < nblk * dnode.datablkszsec * 512:
            data += b'\0' * (nblk * dnode.datablkszsec * 512)
        return data

    def read_bp(self, bp):
        obj = PARSER[bp.type](self.read_raw(bp))
        obj.pool = self
        return obj

    def read(self, p):
        if isinstance(p, BlkPtr):
            return self.read_bp(p)
