from uberblock import Uberblock
from vdev import FileVdev, CachedVdev, CyclicCache
from raidz import RaidZVdev
from zpool import ZPool
import binascii
from zio import lz4_decompress
from dnode import Dnode
import dmu_constant
import util
import struct

def open_vdev():
    global v1, v2, v3, raiddev, pool
    v1 = FileVdev(open("/dev/disk/by-id/wwn-0x5000c500bdd2232d-part1", "rb"))
    v1 = CachedVdev(v1, CyclicCache(2048 * 128), 2048)
    v2 = FileVdev(open("/dev/disk/by-id/wwn-0x5000c500bdb88220-part1", "rb"))
    v2 = CachedVdev(v2, CyclicCache(2048 * 128), 2048)
    v3 = FileVdev(open("/dev/disk/by-id/wwn-0x5000c500c717c7b7-part1", "rb"))
    v3 = CachedVdev(v3, CyclicCache(2048 * 128), 2048)
    raiddev = RaidZVdev([v1, v2, v3], 1, 12)
    pool = ZPool([raiddev])

def scan():
    i = 0x1c1ba4f000//4096
    while i < 9*1024*1024*1024*1024//4096:
        if i * 4096 % (1024 * 1024 * 1024) == 0:
            print("Scanned %d GB" % (i * 4096 // (1024*1024*1024)))
            print("Hit rate 1: %f%%" % v1.get_hit_rate())
            print("Hit rate 2: %f%%" % v2.get_hit_rate())
            print("Hit rate 3: %f%%" % v3.get_hit_rate())
            v1.clear_hit_rate()
            v2.clear_hit_rate()
            v3.clear_hit_rate()
        dnode = raiddev.read(i * 4096, 128 * 1024)
        input_size, = struct.unpack_from(">I", dnode)
        if input_size > 128 * 1024:
            i += 1
            continue
        try:
            dnode = lz4_decompress(dnode)
        except:
            i += 1
            continue
        try:
            print("Found at 0x%x" % (i * 4096))
            for j in range(len(dnode) // Dnode.SIZE):
                dnoden = Dnode.frombytes(dnode[j * Dnode.SIZE:(j + 1) * Dnode.SIZE], pool)
                if dnoden.type != 0 and dnoden.type < len(dmu_constant.TYPES):
                    print("    [%d]: %s" % (j, dmu_constant.TYPES[dnoden.type]))
                if dnoden.type == 20:
                    print(dnoden.list())
                elif dnoden.type == 19:
                    print("        filelen: %d" % (dnoden.secphys if (dnoden.flags & 1 != 0) else (dnoden.secphys * 512)))
        except Exception as e:
            pass
            print("Bad at 0x%x" % (i * 4096))
            print(e)
        i += raiddev.get_asize(4 + input_size) // 4096

def dump():
    ub = [Uberblock.at(v1, 128*1024+i*1024) for i in range(128)]
    best_ub = None
    for i in range(128):
        if best_ub == None or ub[i].txg > best_ub.txg:
            best_ub = ub[i]
    mos = pool.read(best_ub.rootbp)
    dir = mos.read_object(1)
    root_dataset = dir.get('root_dataset')[0]
    root_dataset = mos.read_object(root_dataset)
    active_dataset = root_dataset.get_active_dataset()
    active_dataset = mos.read_object(active_dataset)
    zpl = pool.read(active_dataset.bonus.bp)
    master = zpl.read_object(1)
    root = master.get("ROOT")
    root = zpl.read_object(root)

open_vdev()
scan()
