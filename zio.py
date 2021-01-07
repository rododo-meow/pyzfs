import struct
import lz4
import binascii
from dmu import Dnode, ObjSet

def fletcher2(data):
    return False

def fletcher4(data):
    a, b, c, d = 0, 0, 0, 0
    for i in range(len(data) // 4):
        f = struct.unpack_from("<I", data[i * 4:])[0]
        a = (a + f) & 0xffffffffffffffff
        b = (b + a) & 0xffffffffffffffff
        c = (c + b) & 0xffffffffffffffff
        d = (d + c) & 0xffffffffffffffff
    return struct.pack("<QQQQ", a, b, c, d)

CKFUNC = (
        None,
        "ON",
        "OFF",
        "LABEL",
        "GANG_HEADER",
        "ZILOG",
        fletcher2,
        fletcher4,
        "SHA-256",
        "ZILOG2",
        "NOPARITY",
        "SHA-512",
        "SKEIN",
        "EDONR",
)

def checksum(cksum, data):
    return CKFUNC[cksum](data)

def lz4_compress(data):
    raise NotImplementedError("LZ4 compress not implemented")

def lz4_decompress(data):
    ilen = struct.unpack_from(">I", data)[0]
    if len(data) < 4 + ilen:
        raise IndexError("Buffer too short")
    return lz4.decompress(data[4:4 + ilen])

COMPFUNC = (
        "INHERIT",
        "LZJB",
        "NONE",
        "LZJB",
        "EMPTY",
        "GZ-1",
        "GZ-2",
        "GZ-3",
        "GZ-4",
        "GZ-5",
        "GZ-6",
        "GZ-7",
        "GZ-8",
        "GZ-9",
        "ZLE",
        (lz4_compress, lz4_decompress),
)

def compress(comp, data):
    return COMPFUNC[comp][0](data)

def decompress(comp, data):
    return COMPFUNC[comp][1](data)

def read(vdev, bp):
    dva = bp.dva[0]
    data = vdev.read(dva.offset, bp.psize)
    data = data[:bp.psize]
    if bp.checksum != checksum(bp.cksum, data):
        for i in range(len(data) // 512):
            print(binascii.b2a_hex(data[i*512:i*512+512]))
        print("Bad checksum")
        print("BP: %s" % (binascii.b2a_hex(bp.checksum)))
        print("My: %s" % (binascii.b2a_hex(checksum(bp.cksum, data))))
    data = decompress(bp.comp, data)
    return data