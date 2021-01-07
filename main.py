from uberblock import Uberblock
from vdev import FileVdev
from raidz import RaidZVdev
from zpool import ZPool
import binascii

v1 = FileVdev(open("../vdev1", "rb"))
v2 = FileVdev(open("../vdev2", "rb"))
v3 = FileVdev(open("../vdev3", "rb"))
raiddev = RaidZVdev([v1, v2, v3], 1, 9)
pool = ZPool([raiddev])
ub = [Uberblock.at(v1, 128*1024+i*1024) for i in range(128)]
best_ub = None
for i in range(128):
    if best_ub == None or ub[i].txg > best_ub.txg:
        best_ub = ub[i]
mos = pool.read(best_ub.rootbp)
dir = mos.read_object(1)
root_dataset = dir.get('root_dataset')[0]
root_dataset = mos.read_object(root_dataset)
print(root_dataset)
#print(binascii.b2a_hex(od))
