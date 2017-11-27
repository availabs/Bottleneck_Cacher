class Nestor:
    def __init__(self):
        self.keyFuncs = []
        
    def key(self, k):
        self.keyFuncs.append(k)
        return self
        
    def makeDict(self, data, _dict = {}):
        return self._makeDict(_dict, 0, data)
        
    def makeList(self, data):
        return self._makeList(0, data)
            
    def _makeDict(self, group, level, data):
        if level >= len(self.keyFuncs):
            return data.pop()
        for d in data:
            k = self.keyFuncs[level](d)
            if k not in group:
                group[k] = {}
            group[k] = self._makeDict(group[k], level + 1, [d])
        return group
        
    def _makeList(self, level, data):
        if level >= len(self.keyFuncs):
            return data.pop()
        nest = []
        keyMap = {}
        for d in data:
            k = self.keyFuncs[level](d)
            if k not in keyMap:
                keyMap[k] = len(nest)
                nest.append({ "key": k, "values": [] })
            nest[keyMap[k]]["values"].append(self._makeList(level + 1, [d]))
        return nest
