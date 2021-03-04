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

def _shell_goto(objid):
    global pwd_objid, root_objid, pwd_path, pwd_obj
    root_objid = objid
    pwd_objid = [objid]
    pwd_obj = [_get_dnode(objid)]
    pwd_path = []

def _shell_pwd():
    print('/' + '/'.join(pwd_path))
    print('/'.join([str(objid) for objid in pwd_objid]))

def _show(lst):
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
                pass
            elif line[0] == 'pwd':
                _shell_pwd()
            elif line[0] == 'ls':
                _shell_ls(line[1:])
            elif line[0] == 'exit':
                break
            else:
                _shell_help()
        except:
            traceback.print_exc()
