from uberblock import Uberblock
from vdev import FileVdev
from raidz import RaidZVdev
from zpool import ZPool
import binascii

v1 = FileVdev(open("vdev1", "rb"))
v2 = FileVdev(open("vdev2", "rb"))
v3 = FileVdev(open("vdev3", "rb"))
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
active_dataset = root_dataset.get_active_dataset()
active_dataset = mos.read_object(active_dataset)
zpl = pool.read(active_dataset.bonus.bp)
master = zpl.read_object(1)
root = master.get("ROOT")
root = zpl.read_object(root)
test_obj = zpl.read_object(root.get('test'))
print(hex(root.get("test")))