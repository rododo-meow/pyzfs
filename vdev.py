import struct

class Vdev:
    def read64le(self, off):
        return struct.unpack("<Q", self.read_raw(off, 8))

    def read(self, off, len, abd=None):
        raise TypeError("Dummy interface")

class FileVdev(Vdev):
    def __init__(self, f):
        self.f = f

    def read(self, off, len, abd=None):
        self.f.seek(off)
        if abd == None:
            return self.f.read(len)
        else:
            abd.scatter[0][0][abd.scatter[0][1]:abd.scatter[0][1] + abd.scatter[0][2]] = self.f.read(len)
