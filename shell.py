from zio import lz4_decompress
from blkptr import BlkPtr
import traceback
import struct
from dnode import Dnode
import os

root_ptr = 0
root_blk = b''
pwd_objid = []
pwd_obj = []
pwd_path = []
pool = None
alter_roots = []

def _get_dnode(objid):
    lvl = BlkPtr.frombytes(root_blk[:BlkPtr.SIZE])
    lvl = lvl.lvl
    blk = root_blk
    while lvl >= 0:
        idx = objid // (32 * 1024 ** lvl) % 1024
        ptr = BlkPtr.frombytes(blk[idx * BlkPtr.SIZE:(idx + 1) * BlkPtr.SIZE])
        blk = pool.read_raw(ptr)
        lvl -= 1
    idx = objid % 32
    dnode = blk[idx * Dnode.SIZE:(idx + 1) * Dnode.SIZE]
    dnode = Dnode.frombytes(dnode, pool)
    return dnode

def _get_dnode_with_alter(objid):
    global root_blk, root_ptr
    root_ptr_bak = root_ptr
    root_blk_bak = root_blk
    final_e = None
    try:
        return _get_dnode(objid)
    except Exception as e:
        final_e = e
    for alter in alter_roots:
        new_root = alter
        try:
            block = pool.vdevs[0].read(new_root, 4096)
            input_size, = struct.unpack_from(">I", block)
            if input_size > 128 * 1024:
                continue
            block = pool.vdevs[0].read(new_root, 4 + input_size)
            root_blk = lz4_decompress(block)
            root_ptr = new_root
            dnode = _get_dnode(objid)
            return dnode
        except Exception as e:
            final_e = e
        finally:
            root_ptr = root_ptr_bak
            root_blk = root_blk_bak
    raise Exception("get dnode with alter failed") from final_e

def _shell_root(new_root):
    global root_ptr, root_blk
    old_root = root_ptr
    root_ptr = new_root
    block = pool.vdevs[0].read(root_ptr, 4096)
    input_size, = struct.unpack_from(">I", block)
    block = pool.vdevs[0].read(root_ptr, 4 + input_size)
    root_blk = lz4_decompress(block)
    print("Root pointer set to 0x%x, orig 0x%x" % (root_ptr, old_root))

def _shell_help():
    print("root, help, cd, goto, recover, pwd, ls, exit")

def _shell_cd():
    pass

def _recover_file(objid, path):
    if os.path.exists(path):
        return
    try:
        dnode = _get_dnode_with_alter(objid)
        with open(path, 'wb') as outf:
            i = 0
            while i < dnode.secphys:
                if i + 1024 * 1024 > dnode.secphys:
                    outf.write(dnode.read(i, dnode.secphys - i))
                    i += 1024 * 1024
                else:
                    outf.write(dnode.read(i, 1024 * 1024))
                    i += 1024 * 1024
    except:
        outf = open(path + ".fail", 'w')
        traceback.print_exc(file=outf)
        outf.close()

def _recover_dir(objid, path):
    try:
        dnode = _get_dnode_with_alter(objid)
        lst = dnode.list()
        names = lst.keys()
    except:
        outf = open(path + ".fail", 'w')
        traceback.print_exc(file=outf)
        outf.close()
        return
    try:
        os.makedirs(path)
    except FileExistsError:
        pass
    for name in names:
        child_id = lst[name]
        if type(child_id) == list:
            child_id = child_id[0]
        child_type = child_id >> 56
        child_id = child_id & ((1 << 56) - 1)
        name = name[:-1]
        if name == '':
            continue
        if child_type == 0x40:
            # Directory
            _recover_dir(child_id, path + "/" + name)
        elif child_type == 0x80:
            # File
            _recover_file(child_id, path + "/" + name)
        else:
            print("Unknown child type " + str(child_type))

def _shell_recover(argv):
    confirm = input("Recover %s to %s? (y/n): " % ("/" + "/".join(pwd_path), argv[0]))
    if confirm != 'y':
        return
    _recover_dir(pwd_objid[-1], argv[0] + "/" + "/".join(pwd_path))

def _shell_goto(objid):
    global pwd_objid, root_objid, pwd_path, pwd_obj
    root_objid = objid
    pwd_objid = [objid]
    pwd_obj = [_get_dnode_with_alter(objid)]
    pwd_path = []

def _shell_pwd():
    print('/' + '/'.join(pwd_path))
    print('/'.join([str(objid) for objid in pwd_objid]))

def _show(lst):
    if len(lst) == 0:
        return
    width = max([len(s) for s in lst]) + 5
    ncols = os.get_terminal_size().columns // width
    height = (len(lst) + ncols - 1) // ncols
    cols = [None] * ncols
    for i in range(ncols):
        if len(lst) % ncols == 0 or i < len(lst) % ncols:
            cols[i] = lst[i * height:(i + 1) * height]
        else:
            cols[i] = lst[i * height - i - len(lst) % ncols + 1:(i + 1) * height - i - len(lst) % ncols]
    lines = zip(*cols)
    for line in lines:
        print((("%-" + str(width) + "s") * ncols) % line)

def _shell_ls(argv):
    lst = pwd_obj[-1].list()
    if '\x00' in lst:
        del lst['\x00']
    names = list(lst.keys())
    names.sort()
    lst = [name[:-1] + ":" + str(lst[name] & 0x00ffffffffffffff) for name in names]
    _show(lst)

def _shell_show_dnode(argv):
    if len(argv) == 2:
        pass
    else:
        dnode = _get_dnode_with_alter(int(argv[0]))
        print(dnode)

def hexdump(s):
    for i in range((len(s) + 15) // 16):
        line = "%04x: " % (i * 16)
        for j in range(i * 16, i * 16 + 16):
            if j >= len(s):
                line += "   "
            else:
                line += "%02x " % (s[i])
        print(line)

def _shell_show_block(argv):
    if '-d' in argv:
        decompress = True
        argv = filter(lambda x:x != '-d', argv)
    else:
        decompress = False
    addr = int(argv[0], 16)
    if decompress:
        block = pool.vdevs[0].read(addr, 4096)
        input_size, = struct.unpack_from(">I", block)
        if input_size > 128 * 1024:
            print("Decompress failed")
            return
        block = pool.vdevs[0].read(addr, 4 + input_size)
        block = lz4_decompress(block)
        if len(argv) >= 2:
            block = block[:int(argv[1])]
    else:
        size = int(argv[1], 16)
        block = pool.vdevs[0].read(addr, size)
        if len(argv) >= 3:
            block = block[:int(argv[2])]
    hexdump(block)

def _shell_alter_root_file(argv):
    global alter_roots
    alter_roots = []
    f = open(argv[0], 'r')
    line = f.readline()
    while line != None and line != '':
        addr = int(line.strip(), 16)
        alter_roots += [addr]
        line = f.readline()

def shell(_pool):
    global pool
    pool = _pool
    while True:
        try:
            line = input("# ")
            line = line.strip()
            line = line.split(' ')
            if line[0] == 'root':
                _shell_root(int(line[1], 16))
            elif line[0] == 'help':
                _shell_help()
            elif line[0] == 'cd':
                _shell_cd(line[1])
            elif line[0] == 'goto':
                _shell_goto(int(line[1]))
            elif line[0] == 'recover':
                _shell_recover(line[1:])
            elif line[0] == 'pwd':
                _shell_pwd()
            elif line[0] == 'ls':
                _shell_ls(line[1:])
            elif line[0] == 'exit':
                break
            elif line[0] == 'show_dnode':
                _shell_show_dnode(line[1:])
            elif line[0] == 'show_block':
                _shell_show_block(line[1:])
            elif line[0] == 'alter_root_file':
                _shell_alter_root_file(line[1:])
            else:
                _shell_help()
        except:
            traceback.print_exc()
