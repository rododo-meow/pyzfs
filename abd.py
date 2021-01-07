class ABD:
    @staticmethod
    def allocate(size):
        abd = ABD()
        abd.scatter = [([0] * size, 0, size)]
        return abd

    def sub(self, off, size):
        abd = ABD()
        abd.scatter = [(self.scatter[0][0], off, size)]
        return abd

    def get(self):
        b = bytes()
        for s in self.scatter:
            b += bytes(s[0])
        return b
