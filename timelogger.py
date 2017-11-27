import time
class TimeLogger:
    def __init__(self, tag = None):
        self.tag = tag
        self.loggers = {}
        
    def log(self, msg):
        if self.tag:
            print "<{}> {}".format(self.tag, msg)
        else:
            print msg
        return self
        
    def start(self, key):
        self.loggers[key] = time.time()
        return self
        
    def end(self, key):
        prevTime = self.loggers.pop(key, None)
        if prevTime is not None:
            if self.tag:
                print "<{}> {}: {} seconds".format(self.tag, key, time.time() - prevTime)
            else:
                print "{}: {} seconds".format(key, time.time() - prevTime)
        return self
