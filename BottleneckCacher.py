# -*- coding: utf-8 -*-

import psycopg2, time, urllib, urllib2, json, datetime, calendar, threading, sys
import argparse
from connection_data import hermesConnectionData
from auth_login import login
types = {'all':'all_vehicles', 'pass':'passenger_vehicles', 'trucks':'freight_trucks'}
parser = argparse.ArgumentParser(description='generates bottleneck metric from the data')
parser.add_argument('state', type=str, help='Enter the state that the calculation should be run with')
parser.add_argument('--clear', dest='clear', help='Set flag to clear the database')
parser.add_argument('--dates', type=str, dest='dates', help='''Simple json map 
to set year and month limits e.g. {2016:12}
means the year of 2016 with all 12 months
''')
parser.add_argument ('--type', type=str, dest='aadt_type', default='all', help='Optional aadt type parameter')


args = parser.parse_args()
print (args.state)
print (args.clear)
print (args.dates)
print (args.aadt_type)
years = [2017, 2016]
year_max = {2017:8, 2016:12}
def parse_dates(dates) :
    if not dates:
        return
    jdata = json.loads(dates)
    global years
    global year_max

    years = map(int,jdata.keys())
    for year in years:
        year_max[year] = jdata[str(year)]

parse_dates(args.dates)
aadt_type = types[args.aadt_type.lower()]
#API_HOST = "http://localhost:12222/"
AADT_HOST = "http://ares.availabs.org:12222/"
API_HOST = "https://staging.npmrds.availabs.org/api/"
auth_header = {}
last_refresh = 0
TOKEN_REFRESH_THRESH = 250
def check_auth():
    if 'Authorization' in auth_header and ((time.time() - last_refresh) < TOKEN_REFRESH_THRESH):
        return
    else:
        print(time.time() - last_refresh)
        set_header()
def set_header():
    print ('Aquiring Auth Token')
    params = urllib.urlencode({'email':login['email'], 'password':login['password']})
    request = urllib2.Request('https://aauth.availabs.org/login/auth', params)
    token = ''
    try :
        resp = urllib2.urlopen(request)
        j = json.loads(resp.read())
        print (j)
        token = str(j['token'])
        print (token)
    except urllib2.URLError as e :
        print ('Error Fetching Url', request)
        raise e

    auth_header['Authorization'] = "Bearer "+ token
    global last_refresh
    last_refresh = time.time()


MAX_THREADS = 1
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
            
def checkSpeed(speed, isInterstate):
    if speed is None and isInterstate:
        return 55
    if speed is None and not isInterstate:
        return 40
    return speed
            
MULT = 1.0 / (60.0 * 60.0)
def travelTimeToSpeed(tt, length):
    return length / (float(tt) * MULT + MULT)
    
def epochToTime(epoch):
    minutes = epoch * 5
    hour = str(minutes // 60).zfill(2)
    minutes = str(minutes % 60).zfill(2)
    return "{}{}".format(hour, minutes)
    
def yearAndMonthToDate(year, month, endDate=False):
    if month is 0 and not endDate:
        month = 1
    elif month is 0 and endDate:
        month = 12
    date = datetime.date(year, month, 1)
    if endDate:
        monthRange = calendar.monthrange(year, month)
        date = datetime.date(year, month, monthRange[1])
    return str(date)

def getAverageTravelTime(travelTimeData, epoch):

    tt = travelTimeData[epoch][2]

    count = 1
    if (epoch + 1) in travelTimeData and travelTimeData[epoch+1][2]:
        tt += travelTimeData[epoch + 1][2]
        count += 1
    if (epoch + 2) in travelTimeData and travelTimeData[epoch+2][2]:
        tt += travelTimeData[epoch + 2][2]
        count += 1
    return tt / count

class TmcThreader(threading.Thread):
    
    tmcList = []
    listLock = threading.Lock()
    
    totalBottlenecks = 0
    
    tmcStats = {}
    tmcGraph = {}
    TMC_BLACKLIST = set()
        
    HOURLY_DELAY_URL = API_HOST + "hppm/custom-query/phed/ny/tmcs/{}" +\
        "?resHierarchyStr=EPOCH" +\
        "&startDate={}" +\
        "&endDate={}" +\
        "&startTime={}" +\
        "&endTime={}"
    #USE SPECIAL AADT HOST FOR CUSTOM AADT TYPE
    TRAFFIC_VOLUME_URL = AADT_HOST + 'data/custom-query/traffic-count-distributions/ny/tmcs/{}' +\
        "?resHierarchyStr=EPOCH" +\
        "&startDate={}" +\
        "&endDate={}" +\
        "&startTime={}" +\
        "&endTime={}"
    GRAPH_URL = API_HOST + 'networkgraph/parent/from/tmc/{}/depth/5'
        
    MAX_BOTTLENECK_DEPTH = 10
    
    def __init__(self, num, connection):
        threading.Thread.__init__(self)
        self.id = "thread-{}".format(num)
        
        self.connection = connection
        
        self.logger = TimeLogger(self.id)
        
        self.countyTmcs = []
        self.tmcPairs = []
        self.tmcTuple = None
        self.year = 2016
        self.month = 0
        self.tmcTravelTimes = {}
        self.bottlenecks = []
        self.hoursOfDelay = {}
        self.trafficVolumes = {}
        
    @staticmethod
    def initThreader(tmcList):
        TmcThreader.tmcList = tmcList
        
    def __del__(self):
        self.logger.log("Exiting thread")
        
    def run(self):
        self.logger.log("Starting thread")
        TMC_LIMIT = 10
        while True:

            with TmcThreader.listLock:
                if len(TmcThreader.tmcList):
                    length = len(TmcThreader.tmcList)
                    self.countyTmcs = TmcThreader.tmcList[max(0, length - TMC_LIMIT) :]
                    TmcThreader.tmcList = TmcThreader.tmcList[0 : max(0, length - TMC_LIMIT)]
                else:
                    break
                # end if
            self.logger.log("Processing {} TMCs".format(len(self.countyTmcs)))
            self.processTmcs()
        # end while
            
    def processTmcs(self):
        self.logger.start("Pairs")
        self.queryTmcPairs()
        self.logger.end("Pairs")
        
        self.logger.start("Tuples")
        self.makeTmcTuple()
        self.logger.end("Tuples")
        
        self.logger.start("Stats")
        self.queryTmcStats()
        self.logger.end("Stats")
        
        self.tmcTravelTimes = {}
        self.bottlenecks = []
        self.hoursOfDelay = {}
        self.trafficVolumes = {}
        
        months = range(0, 13)
            
        for year in years:
            for month in months:
                if month > year_max[year]:
                    continue
                self.logger.log("checking year: {}, month: {}".format(year, month))
                self.setDate(year, month)
                
                self.logger.start("TMC travel times")
                self.queryTravelTimes()
                self.logger.end("TMC travel times")
                
                self.logger.start("Bottlenecks")
                self.checkForBottlenecks()
                self.logger.end("Bottlenecks")
                self.logger.log("Found {} bottlenecks".format(len(self.bottlenecks)))
                
                self.logger.start("Depths")
                self.calcBottleneckDepths()
                self.logger.end("Depths")
                
                self.logger.start("Delay")
                self.queryHourlyDelay()
                self.logger.end("Delay")
                
                self.logger.start("Volume")
                self.queryTrafficVolume()
                self.logger.end("Volume")
                
                self.logger.start("Updating")
                self.updateBottlenecks()
                self.logger.end("Updating")
                
                self.logger.start("Inserting")
                self.insertBottlenecks()
                self.logger.end("Inserting")
                
            # end for
        # end for
    
    def queryTmcPairs(self):
        with self.connection.cursor() as cursor:
            sql = """
                SELECT base, child
                FROM tmc_children
                WHERE base IN %s
            """
            cursor.execute(sql, (self.countyTmcs, ))
            #print (cursor.query)
            self.tmcPairs = [x for x in cursor]
            self.connection.commit()
    
    def makeTmcTuple(self):
        tmcSet = set()
        for pair in self.tmcPairs:
            tmcSet.add(pair[0])
            tmcSet.add(pair[1])
        self.tmcTuple = tuple(tmcSet)
        
    def setDate(self, year, month):
        self.year = year
        self.month = month
        
    def queryTmcStats(self, tmcTuple = None):
        with self.connection.cursor() as cursor:
            sql = """
                SELECT tmc, avg_speedlimit, length, is_interstate
                FROM tmc_attributes
                WHERE tmc IN %s
            """
            tmcTuple = tmcTuple or self.tmcTuple
            cursor.execute(sql, (tmcTuple, ))

            for row in cursor:
                TmcThreader.tmcStats[row[0]] = (checkSpeed(row[1], row[3]), row[2], row[3])
            self.connection.commit()

    def queryTravelTimes(self, tmcTuple = None, startEpoch = 72, endEpoch = 240):
        with self.connection.cursor() as cursor:
            second = '' if aadt_type=='all_vehicles' else ',AVG(travel_time_all_vehicles)'
            sql = """
                SELECT npmrds.tmc, epoch,
                    COALESCE(AVG(travel_time_{}) {} )
                FROM {}.npmrds as npmrds
                WHERE npmrds.tmc IN %s
                AND {}
                AND epoch >= {}
                AND epoch < {}
                AND extract(DOW FROM date) IN (1, 2, 3, 4, 5)
                GROUP BY npmrds.tmc, epoch
            """.format(aadt_type, second, args.state, self.getDateQuery(), startEpoch, endEpoch)

            #print (sql)
            tmcTuple = tmcTuple or self.tmcTuple
            cursor.execute(sql, (tmcTuple, ))
            Nestor() \
                .key(lambda x: x[0]) \
                .key(lambda x: x[1]) \
                .makeDict([row for row in cursor], self.tmcTravelTimes)
            self.connection.commit()

    def getDateQuery(self):
        if self.month is 0:
            return "date >= '{}-01-01' and date < '{}-12-31'".format(self.year, self.year)
        
        fin_year = (self.year + 1) if self.month == 12 else self.year
        fin_month = (self.month + 1) if self.month < 12 else "01"
        
        return "date >= '{}-{}-01' and date < '{}-{}-01'".format(self.year , self.month, fin_year, fin_month)
    
    def checkForBottlenecks(self):
        self.bottlenecks = []
        keys = set()
        
        for pair in self.tmcPairs:
            baseTmc = pair[0]
            childTmc = pair[1]
            
            if baseTmc not in self.tmcStats or \
                baseTmc not in self.tmcTravelTimes or \
                childTmc not in self.tmcStats or \
                childTmc not in self.tmcTravelTimes:
                    continue
            
            baseStats = self.tmcStats[baseTmc]
            baseTravelTimeData = self.tmcTravelTimes[baseTmc]
            
            childStats = self.tmcStats[childTmc]
            childTravelTimeData = self.tmcTravelTimes[childTmc]
            
            epoch = 72
            
            startEpoch = -1
            peakSeverity = 0
            peakEpoch = -1
            
            prevBn = None
            
            duration = 2
            
            while epoch < 240 - duration:
                if epoch in baseTravelTimeData and epoch in childTravelTimeData:
                    
                    key = "{}-{}-{}-{}".format(baseTmc, epoch, self.year, self.month)
                    if key in keys:
                        epoch += 1
                        continue              
                    baseTt = getAverageTravelTime(baseTravelTimeData, epoch)
                    #baseTt = baseTravelTimeData[epoch][2]
                    baseLength = baseStats[1]
                    baseSpeed = travelTimeToSpeed(baseTt, baseLength)
                    baseLimit = baseStats[0]
                    threshold = baseLimit * 0.666
                    
                    childTt = getAverageTravelTime(childTravelTimeData, epoch)
                    #childTt = childTravelTimeData[epoch][2]
                    childLength = childStats[1]
                    childSpeed = travelTimeToSpeed(childTt, childLength)
                    
                    if (baseSpeed <= threshold) and ((baseSpeed / childSpeed) <= 0.666):
                        keys.add(key)
                        severity = baseLimit / baseSpeed
                        if startEpoch is -1:
                            if prevBn and prevBn[1] >= epoch:
                                # RESTART PREVIOUS BOTTLENECK
                                startEpoch = prevBn[0]
                                if severity > prevBn[2]:
                                    peakSeverity = severity
                                    peakEpoch = epoch
                                else:
                                    peakSeverity = prevBn[2]
                                    peakEpoch = prevBn[3]
                                self.bottlenecks.pop()
                            else:
                                # START NEW BOTTLENECK
                                startEpoch = peakEpoch = epoch
                                peakSeverity = severity
                        else:
                            # CONTINUE BOTTLENECK
                            if severity > peakSeverity:
                                peakSeverity = severity
                                peakEpoch = epoch
                        # end if
                    elif startEpoch is not -1:
                        # END BOTTLENECK
                        self.bottlenecks.append([baseTmc,
                                                 startEpoch,
                                                 epoch + duration,
                                                 self.year,
                                                 self.month,
                                                 peakSeverity,
                                                 peakEpoch])
                        prevBn = (startEpoch, epoch + duration, peakSeverity, peakEpoch)
                        startEpoch = peakEpoch = -1
                        peakSeverity = 0
                    elif prevBn and epoch > prevBn[1]:
                        prevBn = None
                    # end if
                # end if
                epoch += 1
            # end while
            bottlenecks = filter(lambda x: x[0] == baseTmc, self.bottlenecks)
            if len(bottlenecks) > 1:
                self.mergeBottlenecks(bottlenecks)
        # end for
        TmcThreader.totalBottlenecks += len(self.bottlenecks)
        
    def mergeBottlenecks(self, bottlenecks):
        newBns = []
        bottlenecks.sort(lambda a, b: a[1] - b[1])
        current = bottlenecks[0]
        for bn in bottlenecks[1:]:
            if bn[1] >= current[1] and bn[1] <= current[2]:
                current[2] = max(bn[2], current[2])
                if bn[5] > current[5]:
                    current[5] = bn[5]
                    current[6] = bn[6]
            else:
                newBns.append(current)
                current = bn
        newBns.append(current)
        if len(newBns) < len(bottlenecks):
            tmc = newBns[0][0]
            self.bottlenecks = filter(lambda x: x[0] != tmc, self.bottlenecks)
            self.bottlenecks += newBns
        
    def queryHourlyDelay(self, startEpoch = 72, endEpoch = 240):
        tmcs = set(map(lambda x: x[0], self.bottlenecks))
        minEpoch = reduce(lambda a, c: min(a, c[1]), self.bottlenecks, 240)
        maxEpoch = reduce(lambda a, c: max(a, c[2]), self.bottlenecks, 72)
        
        url = TmcThreader.HOURLY_DELAY_URL \
                .format("".join(tmcs),
                        yearAndMonthToDate(self.year, self.month),
                        yearAndMonthToDate(self.year, self.month, True),
                        epochToTime(minEpoch),
                        epochToTime(maxEpoch))
        check_auth()
        request = urllib2.Request(url, headers=auth_header)
        try:
            response = urllib2.urlopen(request)
            crapData = json.loads(response.read())
            for state in crapData:
                if crapData[state] == None:
                     continue
                for tmc in crapData[state]["phed_data_by_tmc"]:
                    if tmc not in self.hoursOfDelay:
                        self.hoursOfDelay[tmc] = {}
                    d = crapData[state]["phed_data_by_tmc"][tmc]["by_epoch"]
                    for epoch in d:
                        self.hoursOfDelay[tmc][epoch] = d[epoch]["phed"]
        except urllib2.URLError as e:
            self.logger.log("<queryHourlyDelay>: {}".format(e.reason))
            self.logger.log("{}".format(url))
        
    def queryTrafficVolume(self, startEpoch = 72, endEpoch = 240):
        tmcs = set(map(lambda x: x[0], self.bottlenecks))
        minEpoch = reduce(lambda a, c: min(a, c[1]), self.bottlenecks, 240)
        maxEpoch = reduce(lambda a, c: max(a, c[2]), self.bottlenecks, 72)
        
        url = TmcThreader.TRAFFIC_VOLUME_URL \
                .format("".join(tmcs),
                        yearAndMonthToDate(self.year, self.month),
                        yearAndMonthToDate(self.year, self.month, True),
                        epochToTime(minEpoch),
                        epochToTime(maxEpoch))
        check_auth()
        request = urllib2.Request(url, headers=auth_header)
        try:
            response = urllib2.urlopen(request)
            crapData = json.loads(response.read())
            for state in crapData:
                for tmc in crapData[state]:
                    if tmc not in self.trafficVolumes:
                        self.trafficVolumes[tmc] = {}
                    d = crapData[state][tmc]["traffic_count_estimates"]["by_epoch"]
                    for epoch in d:
                        self.trafficVolumes[tmc][epoch] = d[epoch]["ct"]
        except urllib2.URLError as e:
            self.logger.log("<queryTrafficVolume>: {}".format(e.reason))
            
    def updateBottlenecks(self):
        for bn in self.bottlenecks:
            tmc = bn[0]
            bn.append([])
            bn.append([])
            updateHoursOfDelay = tmc in self.hoursOfDelay
            updateTrafficVolume = tmc in self.trafficVolumes
            for epoch in range(bn[1], bn[2]):
                epoch = str(epoch)
                if updateHoursOfDelay and epoch in self.hoursOfDelay[tmc]:
                    bn[8].append(self.hoursOfDelay[tmc][epoch])
                if updateTrafficVolume and epoch in self.trafficVolumes[tmc]:
                    bn[9].append(self.trafficVolumes[tmc][epoch])
            bn.append(args.aadt_type.lower())
        
    def calcBottleneckDepths(self):
        for bottleneck in self.bottlenecks:
            tmc = bottleneck[0]
                
            depth = self.calcBottleneckDepth(tmc, bottleneck[6])
            bottleneck.append(depth)
    
    def calcBottleneckDepth(self, tmc, epoch, depth = 0):
        if depth >= TmcThreader.MAX_BOTTLENECK_DEPTH:
            return depth
            
        if tmc not in self.tmcGraph and tmc not in self.TMC_BLACKLIST:
            parentTmcs = self.extendTmcGraph(tmc)
            
            newTmcTuple = tuple([t for t in parentTmcs if t not in self.tmcStats])
            if len(newTmcTuple):
                self.queryTmcStats(newTmcTuple)
            newTmcTuple = tuple([t for t in parentTmcs if t not in self.tmcTravelTimes])
            if len(newTmcTuple):
                self.queryTravelTimes(newTmcTuple)
        
        if tmc not in self.tmcGraph:
            self.TMC_BLACKLIST.add(tmc)
            return depth
                
        newDepth = depth
            
        parents = self.tmcGraph[tmc]
        for parent in parents:
            if parent not in self.tmcTravelTimes:
                break
            ttData = self.tmcTravelTimes[parent]
            if epoch in ttData:
                speed = travelTimeToSpeed(ttData[epoch][2], self.tmcStats[parent][1])
                speedLimit = self.tmcStats[parent][0]
                threshold = speedLimit * 0.666
                if speed < threshold:
                    newDepth = self.calcBottleneckDepth(parent, epoch, depth + 1)
            if newDepth >= 10:
                break
        return newDepth
        
    def extendTmcGraph(self, tmc):
        url = TmcThreader.GRAPH_URL.format(tmc)
        check_auth()
        request = urllib2.Request(url, headers=auth_header)
        newTmcs = set()
        try:
            response = urllib2.urlopen(request)
            for r in json.loads(response.read()):
                if r['depth'] > 0:
                    child = r['target']
                    parent = r['source']
                    newTmcs.add(parent)
                    if child not in self.tmcGraph:
                        self.tmcGraph[child] = set()
                    self.tmcGraph[child].add(parent)
        except urllib2.URLError as e:
            self.logger.log("<extendTmcGraph>: {}".format(e.reason))
        return newTmcs
    
    def insertBottlenecks(self):
        with self.connection.cursor() as cursor:
            d = "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            sql = """
                INSERT INTO {}.bottlenecks
                VALUES {}
            """.format(args.state, ",".join(map(lambda x: cursor.mogrify(d, x), self.bottlenecks)))
            cursor.execute(sql)
            self.connection.commit()

def main():    
    logger = TimeLogger().start("Running time")
    
    with getConnection(hermesConnectionData) as connection:
        if args.clear and args.state:
            del_table(connection)
            return
        elif args.clear:
            print ("need table space to delete from")
            return 
            
        init_table(connection)
        
        threaders = [TmcThreader(d, connection) for d in range(MAX_THREADS)]
        
        TmcThreader.initThreader(queryTmcs(connection))
        
        for threader in threaders:
            threader.start()
        for threader in threaders:
            threader.join()
        
    logger.log("==========")
    logger.end("Running time")
    logger.log("Total number of bottlenecks: {}".format(TmcThreader.totalBottlenecks))
    
def getConnection(connectionData):
    connection = None
    
    try:
        connection = psycopg2.connect(**connectionData)
    except Exception as e:
        print "Could not establish a connection with {}." \
            .format(connectionData["database"]), e.message
        raise e
    
    if (connection):
        print "Connection establised with {}" \
            .format(connectionData["database"])
        
    return connection
    
def queryTmcs(connection):
    #return ('120P05865', '120P05864', '120P05863', '120P05862', '120P05861')
    result = None
    with connection.cursor() as cursor:
        sql = """
            SELECT DISTINCT tmc
            FROM tmc_attributes
            WHERE tmc NOT IN (SELECT DISTINCT tmc FROM {}.bottlenecks WHERE aadttype = '{}')
            AND state = '{}'
        """
        cursor.execute(sql.format(args.state, args.aadt_type, args.state))
        #print(cursor.query)
        result = tuple([row[0] for row in cursor])
    connection.commit()
    return result

def del_table(connection):
    with connection.cursor() as cursor:
        sql = """
        DROP TABLE IF EXISTS {}.bottlenecks;
        """
        cursor.execute(sql.format(args.state))
    connection.commit()
    return
def init_table(connection):
    with connection.cursor() as cursor:
        sql = """
        CREATE TABLE IF NOT EXISTS {}.bottlenecks (
          tmc character varying(9) NOT NULL,
          start_epoch smallint NOT NULL,
          end_epoch smallint NOT NULL,
          year smallint NOT NULL,
          month smallint NOT NULL,
          peak_severity real NOT NULL,
          peak_epoch smallint NOT NULL,
          peak_depth smallint NOT NULL,
          hours_of_delay_dist real[] NOT NULL,
          traffic_volume_dist real[] NOT NULL,
          aadttype varchar NOT NULL
        );
        """
        cursor.execute(sql.format(args.state))
    connection.commit()
    

if __name__ == "__main__":    
    main()

    print('Finished')
