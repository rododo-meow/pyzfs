import zio
import dmu
from blkptr import BlkPtr
from dnode import Dnode
from objset import ObjSet
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
        for level in range(dnode.nlevels - 1, -1, -1):
            start = blkid >> (level * (dnode.indblkshift - 7))
            end = (blkid + nblk) >> (level * (dnode.indblkshift - 7))
            if (blkid + nblk) & ((1 << (level * (dnode.indblkshift - 7))) - 1) != 0:
                end += 1
            cache = dnode.ptr_cache[level][start:end]
            for i in range(len(cache)):
                if cache[i] == None:
                    # Load indirect block
                    ptr = dnode.ptr_cache[level + 1][(start + i) >> (dnode.indblkshift - 7)]
                    if ptr.birth == 0:
                        blk = b''
                    else:
                        blk = self.read_raw(ptr)
                    if i == 0:
                        blk += b'\0' * (len(blk) - dnode.datablkszsec * 512)
                    else:
                        blk += b'\0' * (len(blk) - (1 << dnode.indblkshift))
                    _start = ((start + i) >> (dnode.indblkshift - 7)) << (dnode.indblkshift - 7)
                    for j in range(1 << (dnode.indblkshift - 7)):
                        if _start + j >= len(dnode.ptr_cache[level]):
                            break
                        dnode.ptr_cache[level][_start + j] = BlkPtr.frombytes(blk[j * 128:j * 128 + 128])
                    cache = dnode.ptr_cache[level][start:end]
        data = b''
        for i in range(len(cache)):
            ptr = cache[i]
            if ptr.birth == 0:
                blk = b''
            else:
                blk = self.read_raw(ptr)
            if i == 0:
                blk += b'\0' * (len(blk) - dnode.datablkszsec * 512)
            else:
                blk += b'\0' * (len(blk) - (1 << dnode.indblkshift))
            data += blk
        if len(data) < nblk * dnode.datablkszsec * 512:
            data += b'\0' * (nblk * dnode.datablkszsec * 512)
        return data

    def read_bp(self, bp):
        obj = PARSER[bp.type](self.read_raw(bp), self)
        return obj

    def read(self, p):
        if isinstance(p, BlkPtr):
            return self.read_bp(p)
