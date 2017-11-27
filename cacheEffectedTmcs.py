import psycopg2 , time, urllib, urllib2, json, datetime, sys
import argparse
from timelogger import TimeLogger
from nestor import Nestor
from connection_data import hermesConnectionData as aresConnectionData
from auth_login import login
from tqdm import tqdm
types = {
    'total':'all_vehicles',
    'pass':'passenger_vehicles',
    'truck':'freight_trucks',
    'combi':'freight_trucks',
    'singl':'freight_trucks'
}

parser = argparse.ArgumentParser(description='generates bottleneck metric from the data')
parser.add_argument('state', type=str, help='Enter the state that the calculation should be run with')
parser.add_argument('--dates', type=str, dest='dates', help='''Simple json map
to set year and month limits e.g. {2016:12}
means the year of 2016 with all 12 months
''')
parser.add_argument ('--type', type=str, dest='aadt_type', default='total', help='Optional aadt type parameter')
args = parser.parse_args()

print (args.state)
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
MULT = 1.0 / (60.0 * 60.0)
def travelTimeToSpeed(tt, length):
    return length / (float(tt) * MULT + MULT)

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


def queryParents(connection) :
    nodemap = {}
    with connection.cursor() as cursor:
        sql = """ select base, child from tmc_children;"""
        cursor.execute(sql)
        results = tuple([row for row in cursor])
        for base, child in results:
            if child not in nodemap:
                nodemap[child] = set()
            nodemap[child].add(base)
    return nodemap

def checkSpeed(speed, isInterstate):
    if speed is None and isInterstate:
        return 55
    if speed is None and not isInterstate:
        return 40
    return speed

def queryTmcStats(connection) :
    stats = {}
    with connection.cursor() as cursor:
        sql = """
        SELECT tmc, avg_speedlimit, length, is_interstate
                FROM tmc_attributes
        """
        cursor.execute(sql)
        for row in cursor:
            stats[row[0]] = (checkSpeed(row[1], row[3]), row[2], row[3])
    return stats

def queryTravelTimes(conn, year, month, tmcTuple = None, startEpoch = 72, endEpoch = 240, aadt_type='all_vehicles'):
        with conn.cursor() as cursor:
            second = '' if aadt_type=='all_vehicles' else ',AVG(travel_time_all_vehicles)'
            sql = """
                SELECT npmrds.tmc, epoch,
                    COALESCE(AVG(travel_time_{}) {} )
                FROM {}.npmrds as npmrds
                WHERE {}
                AND epoch >= {}
                AND epoch < {}
                AND extract(DOW FROM date) IN (1, 2, 3, 4, 5)
                GROUP BY npmrds.tmc, epoch
            """.format(aadt_type, second, args.state, getDateQuery(year, month), startEpoch, endEpoch)
            print(sql)
            cursor.execute(sql)
            diction = {}
            Nestor() \
                .key(lambda x:x[0]) \
                .key(lambda x:x[1]) \
                .makeDict([row for row in cursor], diction)
            return diction

def getDateQuery(year,month):
    if month is 0:
        return "date >= '{}-01-01' and date < '{}-12-31'".format(year, year)

    fin_year = (year + 1) if month == 12 else year
    fin_month = (month + 1) if month < 12 else "01"

    return "date >= '{}-{}-01' and date < '{}-{}-01'".format(year , month, fin_year, fin_month)

def getBottlenecks(connection, year, month):
    with connection.cursor() as cursor:
        sql = """
        SELECT tmc, start_epoch, peak_epoch, peak_depth, id from {}.bottlenecks where year={} and month={} and aadttype='{}'"""
        print('bneck query', (args.state, year, month, args.aadt_type))
        sql = sql.format(args.state, year, month, args.aadt_type)
        cursor.execute(sql)
        return tuple([row for row in cursor])

def calcBottleneckDepths(conn, graph, tt, stats, bncks):
    fails = []
    tuples = []
    for bottleneck in tqdm(bncks):
        tmc = bottleneck[0]
        depth, parents = calcBottleneckDepth(graph, tt, stats, tmc, bottleneck[2])
        tuples.append((bottleneck[4], depth, parents))
        if depth != bottleneck[3]:
            fails.append(depth - bottleneck[3])
        if len(tuples) == 1000:
            insert_affected(conn, tuples)
            tuples = []

    if len(tuples) > 0:
        insert_affected(conn, tuples)

    print('num fails: {}'.format(len(fails)), min(fails), max(fails), sum(fails)/len(fails))

MAX_DEPTH = 10
def calcBottleneckDepth(graph, tt, stats, tmc, epoch, depth=0, parentlist=None):
    if parentlist is None:
        parentlist = []
    if depth >= MAX_DEPTH:
        return depth,parentlist
    if tmc not in graph:
        return depth, parentlist
    newDepth = depth
    newparentlist = parentlist
    parents = graph[tmc]
    for parent in parents:
        assert parent in stats, '{} is not in stats'.format(parent)
        if parent not in tt:
            break
        ttData = tt[parent]
        if epoch in ttData:
            speed = travelTimeToSpeed(ttData[epoch][2], stats[parent][1])
            speedLimit = stats[parent][0]
            threshold = speedLimit * 0.666
            if speed < threshold:
                parentlist.append(parent)
                newDepth,newparentlist = calcBottleneckDepth(graph, tt, stats, parent,  epoch, depth+1, parentlist)
        if newDepth >= 10:
            break
    return newDepth, newparentlist

def insert_affected(connection, rows):
    with connection.cursor() as cursor:
        d = "(%s, %s, %s)"
        sql = """
            INSERT INTO {}.bottleneck_effects
            VALUES {}
        """.format(args.state, ','.join(map(lambda x: cursor.mogrify(d, x), rows)) )
        cursor.execute(sql)
        connection.commit()

def init_table(connection):
    with connection.cursor() as cursor:
        sql = """
        CREATE TABLE IF NOT EXISTS {}.bottleneck_effects (
            id integer NOT NULL,
            depth smallint NOT NULL,
            affected text[][] NOT NULL
        );
        """.format(args.state)
        cursor.execute(sql)
    connection.commit()


nodemap = None
stats = None
def main () :
    with getConnection(aresConnectionData) as connection:
        months = range(0, 13)
        init_table(connection)
        for year in years:
            for month in months:
                if month > year_max[year]:
                    continue
                print('Starting year:{} month:{}'.format(year,month))
                tlog.start('gquery')
                nodemap = queryParents(connection)
                tlog.end('gquery')
                tlog.start('squery')
                stats = queryTmcStats(connection)
                tlog.end('squery')
                tlog.start('ttquery')
                tt = queryTravelTimes(connection, year, month)
                tlog.end('ttquery')
                tlog.start('bnquery')
                bnecks = getBottlenecks(connection, year, month)
                tlog.end('bnquery')
                print(sys.getsizeof(nodemap), sys.getsizeof(stats), sys.getsizeof(bnecks))

                calcBottleneckDepths(connection, nodemap, tt, stats, bnecks)


tlog = TimeLogger()
if __name__ == '__main__':

    main()
