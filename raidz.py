from vdev import Vdev
import binascii
from abd import ABD

class RaidZMap:
    def __str__(self):
        return ", ".join([("P:" if i < self.rm_firstdatacol else "D:") + str(self.rm_col[i]) for i in range(self.rm_cols)])

class RaidZCol:
    def __str__(self):
        return "%d:%x:%x to %x" % (self.rc_devidx, self.rc_offset, self.rc_size, self.rc_abd.scatter[0][1])

def roundup(a, b):
    if a % b == 0:
        return a
    else:
        return a + (b - a % b)

def MIN(a, b):
    return a if a < b else b

def raidz_map_alloc(abd, offset, size, ashift, dcols, nparity):
    # The starting RAIDZ (parent) vdev sector of the block.
    b = offset >> ashift
    # The zio's size in units of the vdev's minimum sector size.
    s = (size + (1 << ashift) - 1) >> ashift
    # The first column for this stripe.
    f = b % dcols
    # The starting byte offset on each child vdev.
    o = (b // dcols) << ashift
    # "Quotient": The number of data sectors for this stripe on all but
    # the "big column" child vdevs that also contain "remainder" data.
    q = s // (dcols - nparity)
    # "Remainder": The number of partial stripe data sectors in this I/O.
    # This will add a sector to some, but not all, child vdevs.
    r = s - q * (dcols - nparity)
    # The number of "big columns" - those which contain remainder data.
    bc = 0 if r == 0 else r + nparity
    # The total number of data and parity sectors associated with
    # this I/O.
    tot = s + nparity * (q + (0 if r == 0 else 1));

    # acols: The columns that will be accessed.
    # scols: The columns that will be accessed or skipped.
    if q == 0:
        # Our I/O request doesn't span all child vdevs.
        acols = bc
        scols = MIN(dcols, roundup(bc, nparity + 1))
    else:
        acols = dcols
        scols = dcols

    if not acols <= scols:
        raise ValueError("assert(acols <= scols)")

    rm = RaidZMap()
    rm.rm_cols = acols
    rm.rm_scols = scols
    rm.rm_bigcols = bc
    rm.rm_skipstart = bc
    rm.rm_missingdata = 0
    rm.rm_missingparity = 0
    rm.rm_firstdatacol = nparity
    rm.rm_abd_copy = None
    rm.rm_reports = 0
    rm.rm_freed = 0
    rm.rm_ecksuminjected = 0
    rm.rm_col = [None] * scols
    for i in range(scols):
        rm.rm_col[i] = RaidZCol()

    asize = 0

    for c in range(scols):
        col = f + c
        coff = o
        if col >= dcols:
            col -= dcols
            coff += 1 << ashift
        rm.rm_col[c].rc_devidx = col
        rm.rm_col[c].rc_offset = coff
        rm.rm_col[c].rc_abd = None
        rm.rm_col[c].rc_gdata = None
        rm.rm_col[c].rc_error = 0
        rm.rm_col[c].rc_tried = 0
        rm.rm_col[c].rc_skipped = 0

        if c >= acols:
            rm.rm_col[c].rc_size = 0
        elif c < bc:
            rm.rm_col[c].rc_size = (q + 1) << ashift
        else:
            rm.rm_col[c].rc_size = q << ashift

        asize += rm.rm_col[c].rc_size

    if not asize == tot << ashift:
        raise ValueError("assert(asize == tot << ashift)")

    rm.rm_asize = roundup(asize, (nparity + 1) << ashift);
    rm.rm_nskip = roundup(tot, nparity + 1) - tot;
    if not rm.rm_asize - asize == rm.rm_nskip << ashift:
        raise ValueError("assert(rm.rm_asize - asize == rm.rm_nskip << ashift)")
    if not rm.rm_nskip <= nparity:
        raise ValueError("assert(rm.rm_nskip <= nparity)")

    c = 0
    while c < rm.rm_firstdatacol:
        rm.rm_col[c].rc_abd = ABD.allocate(rm.rm_col[c].rc_size)
        c += 1

    rm.rm_col[c].rc_abd = ABD.allocate(rm.rm_col[c].rc_size) if abd == None else abd.sub(0, rm.rm_col[c].rc_size)
    off = rm.rm_col[c].rc_size;

    c = c + 1
    while c < acols:
        rm.rm_col[c].rc_abd = ABD.allocate(rm.rm_col[c].rc_size) if abd == None else abd.sub(off, rm.rm_col[c].rc_size)
        off += rm.rm_col[c].rc_size;
        c += 1

    # If all data stored spans all columns, there's a danger that parity
    # will always be on the same device and, since parity isn't read
    # during normal operation, that device's I/O bandwidth won't be
    # used effectively. We therefore switch the parity every 1MB.
    #
    # ... at least that was, ostensibly, the theory. As a practical
    # matter unless we juggle the parity between all devices evenly, we
    # won't see any benefit. Further, occasional writes that aren't a
    # multiple of the LCM of the number of children and the minimum
    # stripe width are sufficient to avoid pessimal behavior.
    # Unfortunately, this decision created an implicit on-disk format
    # requirement that we need to support for all eternity, but only
    # for single-parity RAID-Z.
    #
    # If we intend to skip a sector in the zeroth column for padding
    # we must make sure to note this swap. We will never intend to
    # skip the first column since at least one data and one parity
    # column must appear in each row.
    if not rm.rm_cols >= 2:
        raise ValueError("assert(rm.rm_cols >= 2)")
    if not rm.rm_col[0].rc_size == rm.rm_col[1].rc_size:
        raise ValueError("rm.rm_col[0].rc_size == rm.rm_col[1].rc_size")

    if rm.rm_firstdatacol == 1 and offset & (1 << 20) != 0:
        devidx = rm.rm_col[0].rc_devidx;
        o = rm.rm_col[0].rc_offset;
        rm.rm_col[0].rc_devidx = rm.rm_col[1].rc_devidx;
        rm.rm_col[0].rc_offset = rm.rm_col[1].rc_offset;
        rm.rm_col[1].rc_devidx = devidx;
        rm.rm_col[1].rc_offset = o;

        if rm.rm_skipstart == 0:
            rm.rm_skipstart = 1

    return rm

class RaidZVdev(Vdev):
    def __init__(self, vdevs, nparity, ashift):
        self.vdevs = vdevs
        self.nparity = nparity
        self.ashift = ashift

    def read(self, offset, size):
        abd = ABD.allocate(size)
        p = raidz_map_alloc(abd, offset, size, self.ashift, len(self.vdevs), self.nparity)
        for i in range(p.rm_firstdatacol, p.rm_cols):
            self.vdevs[p.rm_col[i].rc_devidx].read(p.rm_col[i].rc_offset + 4*1024*1024, p.rm_col[i].rc_size, p.rm_col[i].rc_abd)
        return abd.get()

    def read_strips(self, offset, size):
        p = raidz_map_alloc(None, offset, size, self.ashift, len(self.vdevs), self.nparity)
        abds = [col.rc_abd for col in p.rm_col]
        for i in range(p.rm_cols):
            print("Read %d:%x:%x" % (p.rm_col[i].rc_devidx, p.rm_col[i].rc_offset, p.rm_col[i].rc_size))
            self.vdevs[p.rm_col[i].rc_devidx].read(p.rm_col[i].rc_offset + 4*1024*1024, p.rm_col[i].rc_size, abds[i])
        return [abd.get() for abd in abds]

    def get_asize(self, psize):
        ashift = self.ashift
        cols = len(self.vdevs)
        nparity = self.nparity
        asize = ((psize - 1) >> ashift) + 1
        asize += nparity * ((asize + cols - nparity - 1) // (cols - nparity))
        asize = roundup(asize, nparity + 1) << ashift
        return asize
