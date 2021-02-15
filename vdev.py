import struct
from xdr import XDRNVList

class Vdev:
    def read64le(self, off):
        return struct.unpack("<Q", self.read_raw(off, 8))

    def read(self, off, len, abd=None):
        raise TypeError("Dummy interface")

def abd_put(abd, off, data):
    # TODO: real abd
    abd.scatter[0][0][abd.scatter[0][1] + off:abd.scatter[0][1] + off + len(data)] = data

class FileVdev(Vdev):
    def __init__(self, f):
        self.f = f

    def read(self, off, len, abd=None):
        self.f.seek(off)
        if abd == None:
            return self.f.read(len)
        else:
            abd_put(abd, 0, self.f.read(len))

    def read_nvpairs(self):
        data = self.read(16 * 1024, 112 * 1024)
        data = XDRNVList.frombytes(data[4:])
        return data

class CyclicCache:
    def __init__(self, nentries):
        self.nentries = nentries
        self.cache = [None] * nentries
        self.head = 0
        self.quickmap = {}

    def get(self, addr):
        if addr in self.quickmap:
            return self.cache[self.quickmap[addr]][1]
        else:
            return None

    def feed(self, addr, data):
        if addr in self.quickmap:
            self.cache[self.quickmap[addr]] = (addr, data)
        else:
            if self.cache[self.head] != None:
                del self.quickmap[self.cache[self.head][0]]
            self.quickmap[addr] = self.head
            self.cache[self.head] = (addr, data)
            self.head = (self.head + 1) % self.nentries

class CachedVdev(Vdev):
    def __init__(self, vdev, cache, prefetch):
        self.low = vdev
        self.cache = cache
        self.prefetch = prefetch
        self.access = 0
        self.hit = 0

    def get_hit_rate(self):
        print("Access %d" % (self.access))
        print("Hit %d" % (self.hit))
        return self.hit / self.access if self.access != 0 else 1.0

    def clear_hit_rate(self):
        self.access = 0
        self.hit = 0

    def __check_and_read(self, addr):
        self.access += 1
        data = self.cache.get(addr)
        if data == None:
            data = self.low.read(addr, self.prefetch * 512)
            for i in range(self.prefetch):
                self.cache.feed(addr + (i << 9), data[i << 9:(i + 1) << 9])
        else:
            self.hit += 1
        return data[:512]

    def read(self, off, len, abd=None):
        if abd == None:
            r = []
            if off & 0x1ff != 0:
                # Handle special offset
                base = (off >> 9) << 9
                data = self.__check_and_read(base)
                r += data[off - base:]
                len -= 512 - (off - base)
                off = base + 512
            while len >= 512:
                r += self.__check_and_read(off)
                len -= 512
                off += 512
            if len != 0:
                r += self.__check_and_read(off)[:len]
            return bytes(r)
        else:
            abd_off = 0
            if off & 0x1ff != 0:
                # Handle special offset
                base = (off >> 9) << 9
                data = self.__check_and_read(base)
                abd_put(abd, abd_off, data[off - base:])
                abd_off += 512 - (off - base)
                len -= 512 - (off - base)
                off = base + 512
            while len >= 512:
                data = self.__check_and_read(off)
                abd_put(abd, abd_off, data)
                abd_off += 512
                len -= 512
                off += 512
            if len != 0:
                data = self.__check_and_read(off)
                abd_put(abd, abd_off, data[:len])
