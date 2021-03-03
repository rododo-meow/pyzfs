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
from blkptr import BlkPtr
import sys
import traceback
import sys

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
    ashift = raiddev.ashift
    i = 0
    while i < 9*1024*1024*1024*1024 >> ashift:
        if (i << ashift) % (1024 * 1024 * 1024) == 0:
            print("Scanned %d GB" % ((i << ashift) // (1024*1024*1024)))
            print("Hit rate 1: %f%%" % v1.get_hit_rate())
            print("Hit rate 2: %f%%" % v2.get_hit_rate())
            print("Hit rate 3: %f%%" % v3.get_hit_rate())
            v1.clear_hit_rate()
            v2.clear_hit_rate()
            v3.clear_hit_rate()
        block = raiddev.read(i << ashift, 1 << ashift)
        input_size, = struct.unpack_from(">I", block)
        if input_size > 128 * 1024:
            i += 1
            continue
        block = raiddev.read(i << ashift, 4 + input_size)
        try:
            block = lz4_decompress(block)
        except:
            i += 1
            continue
        try:
            print("Found at 0x%x" % (i << ashift))
            for j in range(len(block) // Dnode.SIZE):
                dnode = Dnode.frombytes(block[j * Dnode.SIZE:(j + 1) * Dnode.SIZE], pool)
                if dnode.type != 0 and dnode.type < len(dmu_constant.TYPES) and dmu_constant.TYPES[dnode.type] != None:
                    print("    [%d]: %s" % (j, dmu_constant.TYPES[dnode.type]))
                if dnode.type == 20:
                    print(dnode.list())
                elif dnode.type == 19:
                    print("        filelen: %d" % (dnode.secphys if (dnode.flags & 1 != 0) else (dnode.secphys * 512)))
            for j in range(len(block) // BlkPtr.SIZE):
                ptr = BlkPtr.frombytes(block[j * BlkPtr.SIZE:(j + 1) * BlkPtr.SIZE])
                if ptr.embedded and ptr.etype == BlkPtr.ETYPE_DATA:
                    print("    [%d]: %s" % (j, dmu_constant.TYPES[dnode.type]))
                elif not ptr.embedded and ptr.dva[0].vdev == 0 and ptr.dva[0].offset & 0x1ff == 0 and ptr.dva[0].asize & 0xfff == 0 and (ptr.comp == 15 or ptr.comp == 2) and ptr.type == 20:
                    print("    [%d]:" % (j,))
                    print(util.shift(str(ptr), 2))
        except Exception as e:
            pass
            print("Bad at 0x%x" % (i << ashift))
            traceback.print_exc(file=sys.stdout)
        i += raiddev.get_asize(4 + input_size) >> ashift

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

def scan_log():
    f = open(sys.argv[1], 'r')
    line = f.readline()
    while line != None and line != "":
        if line.startswith('Found at '):
            line = line.strip()
            try:
                addr = int(line[9:], 16)
            except:
                pass
            line = f.readline()
        elif line.startswith('    [') and line.endswith(':\n'):
            line = f.readline()
            block = raiddev.read(addr, 4096)
            input_size, = struct.unpack_from(">I", block)
            block = raiddev.read(addr, 4 + input_size)
            block = lz4_decompress(block)
            try:
                for j in range(len(block) // BlkPtr.SIZE):
                    ptr = BlkPtr.frombytes(block[j * BlkPtr.SIZE:(j + 1) * BlkPtr.SIZE])
                    if ptr.embedded and ptr.etype == BlkPtr.ETYPE_DATA:
                        pass
                    elif not ptr.embedded and ptr.dva[0].vdev == 0 and ptr.dva[0].offset & 0x1ff == 0 and ptr.dva[0].asize & 0xfff == 0 and (ptr.comp == 15 or ptr.comp == 2) and ptr.type == 20:
                        if ptr.type == 20 and ptr.nlevel != 0:
                            print("%x[%d]:" % (addr, j,))
            except:
                pass
            while line != None and line.startswith('        '):
                line = f.readline()
        else:
            line = f.readline()

def scan_log2():
    f = open(sys.argv[1], 'r')
    line = f.readline()
    while line != None and line != "":
        if line.startswith('Found at '):
            line = line.strip()
            try:
                addr = int(line[9:], 16)
            except:
                pass
            line = f.readline()
        elif line.endswith(': FILE_CONTENT\n'):
            index = int(line.strip()[1:-15])
            line = f.readline()
            try:
                size = int(line.strip()[9:]) / 1024 / 1024 / 1024
                print("%x:%d:%.2f" % (addr, index, size))
            except:
                pass
            while line != None and line.startswith('        '):
                line = f.readline()
        else:
            line = f.readline()

def recover_file():
    line = sys.stdin.readline()
    line = line.strip()
    addr,j,size = line.split(':')
    addr = int(addr, 16)
    j = int(j)
    block = raiddev.read(addr, 4096)
    input_size, = struct.unpack_from(">I", block)
    block = raiddev.read(addr, 4 + input_size)
    block = lz4_decompress(block)
    dnode = Dnode.frombytes(block[j * Dnode.SIZE:(j + 1) * Dnode.SIZE], pool)
    outf = open('test.mp4', 'wb')
    i = 0
    while i < dnode.secphys:
        if i + 1024 * 1024 > dnode.secphys:
            outf.write(dnode.read(i, dnode.secphys - i))
            i += 1024 * 1024
        else:
            outf.write(dnode.read(i, 1024 * 1024))
            i += 1024 * 1024
    outf.close()

def collect_filenames():
    f = open(sys.argv[1], 'r')
    line = f.readline()
    files = {}
    while line != None and line != "":
        if line.startswith('{'):
            line = line.strip()
            line = eval(line)
            if '.Parent\x00' in line:
                del line['.Parent\x00']
            for name in line:
                if type(line[name]) == list:
                    line[name] = line[name][0]
                objid = line[name] & 0xffffffff
                if (line[name] >> 56) == 0x80:
                    if objid in files:
                        if not name in files[objid]:
                            files[objid] += [name]
                    else:
                        files[objid] = [name]
                elif (line[name] >> 56) == 0x40:
                    if objid in files:
                        if not name in files[objid]:
                            files[objid] += [name]
                    else:
                        files[objid] = [name]
            line = f.readline()
        else:
            line = f.readline()
    ids = list(files.keys())
    ids.sort()
    for id in ids:
        print("%d: %s" % (id, files[id]))
    
def dump_block():
    line = sys.stdin.readline()
    line = line.strip()
    addr = int(line, 16)
    block = raiddev.read(addr, 4096)
    input_size, = struct.unpack_from(">I", block)
    if input_size > 128 * 1024:
        return
    print("Guessed psize=0x%x" % (4+input_size,))
    block = raiddev.read(addr, 4+input_size)
    block = lz4_decompress(block)
    is_ptr = input("Is ptr block? (y/n)")
    if is_ptr == 'n':
        birth = 0
        for j in range(len(block) // Dnode.SIZE):
            dnode = Dnode.frombytes(block[j * Dnode.SIZE:(j + 1) * Dnode.SIZE], pool)
            if dnode.get_birth() > birth:
                birth = dnode.get_birth()
            if dnode.type != 0 and dnode.type < len(dmu_constant.TYPES) and dmu_constant.TYPES[dnode.type] != None:
                print("    [%d]: %s (@%d)" % (j, dmu_constant.TYPES[dnode.type], dnode.get_birth()))
            if dnode.type == 20:
                print(dnode.list())
            elif dnode.type == 19:
                print("        filelen: %d" % (dnode.secphys if (dnode.flags & 1 != 0) else (dnode.secphys * 512)))
        print("Birth: %d" % (birth,))
    else:
        birth = 0
        for j in range(len(block) // BlkPtr.SIZE):
            ptr = BlkPtr.frombytes(block[j * BlkPtr.SIZE:(j + 1) * BlkPtr.SIZE])
            if ptr.birth > birth:
                birth = ptr.birth
            if ptr.embedded and ptr.etype == BlkPtr.ETYPE_DATA:
                print("    [%d]: EMBEDDED" % (j,))
            else:
                print("    [%d]:" % (j,))
                print(util.shift(str(ptr), 2))
        print("Birth: %d" % (birth,))

def dump_birth():
    inf = open(sys.argv[1], 'r')
    outf = open(sys.argv[2], 'w')
    line = inf.readline()
    while line != None and line != '':
        line = line.strip()
        addr = int(line, 16)
        block = raiddev.read(addr, 4096)
        input_size, = struct.unpack_from(">I", block)
        block = raiddev.read(addr, 4 + input_size)
        block = lz4_decompress(block)
        birth = 0
        for j in range(len(block) // BlkPtr.SIZE):
            ptr = BlkPtr.frombytes(block[j * BlkPtr.SIZE:(j + 1) * BlkPtr.SIZE])
            if ptr.birth > birth:
                birth = ptr.birth
        print("0x%x: @%d" % (addr, birth), file=outf)
        line = inf.readline()
    outf.close()
    inf.close()

def _dump_dnode_block(addr, block, base):
    for j in range(len(block) // Dnode.SIZE):
        try:
            dnode = Dnode.frombytes(block[j * Dnode.SIZE:(j + 1) * Dnode.SIZE], pool)
            if dnode.type != 0 and dnode.type < len(dmu_constant.TYPES) and dmu_constant.TYPES[dnode.type] != None:
                print("[%d] (0x%x[%d]): %s (@%d)" % (base * 32 + j, addr, j, dmu_constant.TYPES[dnode.type], dnode.get_birth()))
        except:
            traceback.print_exc(file=sys.stdout)

def _dump_tree(addr, base):
    if type(addr) == int:
        block = raiddev.read(addr, 4096)
        input_size, = struct.unpack_from(">I", block)
        block = raiddev.read(addr, 4 + input_size)
        block = lz4_decompress(block)
    else:
        if addr.endian == 0:
            return
        try:
            block = pool.read_raw(addr)
        except:
            print("Read this ptr failed:")
            print(util.shift(str(addr), 1))
            traceback.print_exc(file=sys.stdout)
            return
    for j in range(len(block) // BlkPtr.SIZE):
        try:
            ptr = BlkPtr.frombytes(block[j * BlkPtr.SIZE:(j + 1) * BlkPtr.SIZE])
            if ptr.embedded and ptr.etype == BlkPtr.ETYPE_DATA:
                print("    [%d]: EMBEDDED" % (j,))
            elif ptr.birth == 0:
                continue
            elif ptr.lvl == 0:
                addr = ptr.dva[0].offset
                nblock = pool.read_raw(ptr)
                if nblock == None:
                    continue
                _dump_dnode_block(addr, nblock, base * 1024 + j)
            else:
                _dump_tree(ptr, base * 1024 + j)
        except:
            pass
    
def dump_tree():
    line = input("Root ptr: ")
    line = line.strip()
    addr = int(line, 16)
    line = input("Out: ")
    line = line.strip()
    sys.stdout = open(line, 'w')
    print("Root 0x%x" % (addr,))
    try:
        _dump_tree(addr, 0)
    except:
        traceback.print_exc(file=sys.stdout)
    
def dump_0():
    line = input("Root block addr: ")
    while line != '':
        line = line.strip()
        addr = int(line, 16)
        print("Root 0x%x" % (addr,))
        block = raiddev.read(addr, 4096)
        input_size, = struct.unpack_from(">I", block)
        block = raiddev.read(addr, 4 + input_size)
        block = lz4_decompress(block)
        ptr = BlkPtr.frombytes(block[0:BlkPtr.SIZE])
        print(util.shift(str(ptr), 1))
        line = input("Root block addr: ")

def dump_raw():
    line = input("Addr: ")
    line = line.strip()
    addr = int(line, 16)
    line = input("Size: ")
    line = line.strip()
    size = int(line, 16)
    block = raiddev.read(addr, size)
    outf = open("%x-%x.block" % (addr, size), "wb")
    outf.write(block)
    outf.close()

def dump_raidz_raw():
    line = input("Addr: ")
    line = line.strip()
    addr = int(line, 16)
    line = input("Size: ")
    line = line.strip()
    size = int(line, 16)
    strips = raiddev.read_strips(addr, size)
    for i in range(len(strips)):
        outf = open("%x-%x.block.%d" % (addr, size, i), "wb")
        outf.write(strips[i])
        outf.close()

def dump_disk_meta():
    ub = [Uberblock.at(v1, 128*1024+i*1024) for i in range(128)]
    best_ub = None
    for i in range(128):
        if best_ub == None or ub[i].txg > best_ub.txg:
            best_ub = ub[i]
    print(best_ub)
    ub = [Uberblock.at(v2, 128*1024+i*1024) for i in range(128)]
    best_ub = None
    for i in range(128):
        if best_ub == None or ub[i].txg > best_ub.txg:
            best_ub = ub[i]
    print(best_ub)
    ub = [Uberblock.at(v3, 128*1024+i*1024) for i in range(128)]
    best_ub = None
    for i in range(128):
        if best_ub == None or ub[i].txg > best_ub.txg:
            best_ub = ub[i]
    print(best_ub)

def dump_dnode():
    line = sys.stdin.readline()
    line = line.strip()
    addr,j = line.split(':')
    addr = int(addr, 16)
    j = int(j)
    block = raiddev.read(addr, 4096)
    input_size, = struct.unpack_from(">I", block)
    if input_size > 128 * 1024:
        return
    print("Guessed psize=0x%x" % (4+input_size,))
    block = raiddev.read(addr, 4+input_size)
    block = lz4_decompress(block)
    dnode = Dnode.frombytes(block[j * Dnode.SIZE:(j + 1) * Dnode.SIZE], pool)
    print(dnode)

open_vdev()
recover_file()
