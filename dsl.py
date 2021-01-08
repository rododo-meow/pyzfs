from dnode import Dnode

class DslDataset(Dnode):
    @staticmethod
    def promote(parent, s):
        ds = DslDataset()
        ds.inherit(parent)
        return ds

    def __str__(self):
        return """CREATE: %x
ACTIVE DATASET: %d""" % (self.bonus.creation_time, self.bonus.head_datset_obj)

    def get_active_dataset(self):
        return self.bonus.head_datset_obj