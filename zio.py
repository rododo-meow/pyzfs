import struct
import lz4.block
import binascii
from blkptr import BlkPtr

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
    if type(CKFUNC[cksum]) == str:
        raise NotImplementedError("Checksum " + CKFUNC[cksum] + " not implemented")
    return CKFUNC[cksum](data)

def lz4_compress(data):
    raise NotImplementedError("LZ4 compress not implemented")

def lz4_decompress(data):
    ilen = struct.unpack_from(">I", data)[0]
    if len(data) < 4 + ilen:
        raise IndexError("Buffer too short")
    return lz4.block.decompress(data[4:4 + ilen], 128 * 1024)

COMPFUNC = (
        "INHERIT",
        "LZJB",
        (lambda x: x, lambda x: x),
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
    if type(COMPFUNC[comp]) == str:
        raise NotImplementedError(COMPFUNC[comp] + " not implemented")
    return COMPFUNC[comp][1](data)

def read(vdevs, bp):
    if bp.embedded:
        data = bp.decode()
        return COMPFUNC[bp.comp][1](data)
    else:
        for i in range(3):
            dva = bp.dva[i]
            if dva.asize == 0:
                continue
            data = vdevs[dva.vdev].read(dva.offset, bp.psize)
            data = data[:bp.psize]
            if bp.checksum != checksum(bp.cksum, data):
                print("Bad checksum")
                print("BP: %s" % (binascii.b2a_hex(bp.checksum)))
                print("My: %s" % (binascii.b2a_hex(checksum(bp.cksum, data))))
                continue
            data = decompress(bp.comp, data)
            return data
        return None
