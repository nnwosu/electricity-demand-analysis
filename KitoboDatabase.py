import configparser
from datetime import datetime
import numpy as np
import pymongo
from pymongo.database import Database
from pymongo import MongoClient
from statistics import stdev
from scipy import stats

defaultSamplingInterval = 'fiveMinutes'
outCollectionPrefix = 'aggregateLoadStats'
loadFactorCollectionName = 'loadFactorSamples'

class KitoboDatabase:

    def appendSample(self,k,ind):

        cursor = self.db.loadAggregationSamples.find({'_id': k})
        if cursor.count() < 1:
            sampleList = {
                '_id': k,
                'ind': []
            }
        else:
            sampleList = cursor.next()

        sampleList['ind'].append(ind)
        self.db.loadAggregationSamples.update(
            {'_id': k},
            {'$set': {'ind': sampleList['ind']}},
            upsert=True
        )
        return sampleList['ind']

    #This aggregates the power consumption from the sample users (specified by ind)
    #over the window from startTime to endTime (at initial writing this is a month)
    #at the fifteenMinute resolution, and adds them together. Summary stats of this
    #timeseries and the load factor are calculated and stored in [outCollectionName]
    def calculateAggregateLoadStats(self,ind,overwrite=False,sampleIndex=-1):

        filterMonitoringDeviceIds = [self.monitoringDeviceIds[i] for i in ind]
        if overwrite:
            self.db[self.outCollectionName].delete({'monitoringDeviceIds': filterMonitoringDeviceIds})

        cursor = self.db[self.outCollectionName].find(
            {
                'startTime': self.startTime,
                'endTime': self.endTime,
                'sampleIndex': sampleIndex,
                'monitoringDeviceIds': filterMonitoringDeviceIds
            }
        )
        if cursor.count() > 0:
            stats = cursor.next()
            return stats

        cursor = self.db[self.samplingInterval].aggregate([
            {
                '$match': {
                    'deviceId': {'$in': filterMonitoringDeviceIds},
                    'tag': 'activePwr',
                    'time': {
                        '$gte': self.startTime,
                        '$lt': self.endTime
                    }
                }
            },
            {
                '$group': {
                    '_id': '$time',
                    'totalPower': {'$sum': '$avg'},
                    'cnt': {'$sum': 1}
                }
            },
            {
                '$match': {
                    'cnt': len(ind)
                }
            },
            {
                '$group': {
                    '_id': {
                        'numUsers': {'$literal': len(ind)},
                        'sampleIndex': {'$literal': sampleIndex},
                    },
                    'max': {'$max': '$totalPower'},
                    'min': {'$min': '$totalPower'},
                    'avg': {'$avg': '$totalPower'},
                    #'std': {'$stdDevPop': '$totalPower'},
                    'cnt': {'$sum': 1}
                }
            },
            {
                '$project': {
                    'numUsers': {'$literal': len(ind)},
                    'monitoringDeviceIds': {'$literal': filterMonitoringDeviceIds},
                    'userIndices': {'$literal': ind},
                    'startTime': {'$literal': self.startTime},
                    'endTime': {'$literal': self.endTime},
                    'samplingInterval': {'$literal': self.samplingInterval},
                    'sampleIndex': {'$literal': sampleIndex},
                    'loadFactor': {'$divide': ['$avg','$max']},
                    'max': True,
                    'min': True,
                    'avg': True,
                    'cnt': True
                }
            }
        ])

        try:
            stats = cursor.next()
        except StopIteration:
            raise IndexError('No power consumption data matching indexes')

        self.db[self.outCollectionName].update(
            {
                'startTime': self.startTime,
                'endTime': self.endTime,
                'sampleIndex': sampleIndex,
                'monitoringDeviceIds': filterMonitoringDeviceIds
            },
            {
                '$set': {
                    'numUsers': len(ind),
                    'monitoringDeviceIds': filterMonitoringDeviceIds,
                    'loadFactor': stats['loadFactor'],
                    'max': stats['max'],
                    'min': stats['min'],
                    'avg': stats['avg'],
                    'cnt': stats['cnt']
                }
            },
            True,
            False
        )
        return stats

    def calculateAutocorrelation(self,ind,n,overwrite=False,sampleIndex=-1):
        #n is the time lag for the autocorrelation
        filterMonitoringDeviceIds = [self.monitoringDeviceIds[i] for i in ind]

        #ensure that there is a stats object there. Not the most elegant; ideally both calculateAutocorrelation and calculateAggregateLoadStats (and future methods computing a statistical property) should upsert
        #self.calculateAggregateLoadStats(ind,overwrite=False)
        #if overwrite:
        #    self.db[self.outCollectionName].delete({'monitoringDeviceIds': filterMonitoringDeviceIds})

        cursor = self.db[self.outCollectionName].find(
            {
                'startTime': self.startTime,
                'endTime': self.endTime,
                'sampleIndex': sampleIndex,
                'monitoringDeviceIds': filterMonitoringDeviceIds
            }
        )
        if cursor.count() > 0:
            stats = cursor.next()
            try:
                R = stats[('autocorrelation_{0}').format(n)]
                return R
            except:
                pass

        cursor = self.getAggregateLoadProfile(filterMonitoringDeviceIds)

        totalPower = [x['totalPower'] for x in list(cursor)]

        #See http://greenteapress.com/thinkdsp/html/thinkdsp006.html section 5.2 for calculation reference
        R = np.corrcoef(totalPower[n:],totalPower[:len(totalPower)-n])[0, 1]

        results = self.db[self.outCollectionName].update(
            {
                'monitoringDeviceIds': filterMonitoringDeviceIds,
                'startTime': self.startTime,
                'endTime': self.endTime,
                'sampleIndex': sampleIndex
            },
            {
                '$set': {
                    ('autocorrelation_{0}'.format(n)): R,
                    'numUsers': len(ind),
                }
            },
            True,
            False
        )
        return R

    def calculateLoadFactor(self,ind):

        stats = self.calculateAggregateLoadStats(ind)
        loadFactor = stats.loadFactor

    def calculateMetricStdDev(self,k,sampleIndex,metricName):

        if sampleIndex < 1:
            return 0

        cursor = self.db[self.outCollectionName].find(
            {
                'numUsers': k,
                'sampleIndex': {'$lte': sampleIndex}
            },
            {
                '_id': 0,
                metricName: 1
            }
        )

        metric = [x[metricName] for x in list(cursor)]
        return stdev(metric)

    def calculateCOV(self,ind,overwrite=False,sampleIndex=-1):
        #n is the time lag for the autocorrelation
        filterMonitoringDeviceIds = [self.monitoringDeviceIds[i] for i in ind]

        cursor = self.db[self.outCollectionName].find(
            {
                'startTime': self.startTime,
                'endTime': self.endTime,
                'sampleIndex': sampleIndex,
                'monitoringDeviceIds': filterMonitoringDeviceIds
            }
        )
        if cursor.count() > 0:
            s = cursor.next()
            try:
                return s['cov']
            except:
                pass

        cursor = self.getAggregateLoadProfile(filterMonitoringDeviceIds)

        totalPower = [x['totalPower'] for x in list(cursor)]

        cov = stats.variation(totalPower)
        print(cov)

        results = self.db[self.outCollectionName].update(
            {
                'monitoringDeviceIds': filterMonitoringDeviceIds,
                'startTime': self.startTime,
                'endTime': self.endTime,
                'sampleIndex': sampleIndex
            },
            {
                '$set': {
                    'cov': cov,
                    'numUsers': len(ind),
                }
            },
            True,
            False
        )
        return cov

    def connect(self):

        config = configparser.ConfigParser()
        config.read('config.ini')

        username = config['DEFAULT']['MONGO_USERNAME']
        password = config['DEFAULT']['MONGO_PASSWORD']
        host = config['DEFAULT']['MONGO_HOST']
        database = config['DEFAULT']['MONGO_DATABASE_NAME']

        self.client = MongoClient(host)
        self.db = Database(self.client, database)
        self.db.authenticate(username, password)

    def disconnect(self):

        self.client.close()

    def getAggregateLoadProfile(self,filterMonitoringDeviceIds):

        cursor = self.db[self.samplingInterval].aggregate([
            {
                '$match': {
                    'deviceId': {'$in': filterMonitoringDeviceIds},
                    'tag': 'activePwr',
                    'time': {
                        '$gte': self.startTime,
                        '$lt': self.endTime
                    }
                }
            },
            {
                '$group': {
                    '_id': '$time',
                    'totalPower': {'$sum': '$avg'},
                    'cnt': {'$sum': 1}
                }
            },
            {
                '$match': {
                    'cnt': len(filterMonitoringDeviceIds)
                }
            },
            {
                '$sort': {'_id': 1}
            }
        ])
        return cursor

    def getMedianLoadProfile(self,k,metricName):
        cursor = self.db[self.outCollectionName].find(
            {
                'numUsers': k,
                'sampleIndex': {'$ne': -1}
            },
            {
                metricName: True,
                'monitoringDeviceIds': True
            }
        ).sort(metricName,pymongo.ASCENDING)
        cursorList = list(cursor)
        medianInd = int(len(cursorList)/2)

        cursor = self.getAggregateLoadProfile(cursorList[medianInd]['monitoringDeviceIds'])
        c = list(cursor)
        totalPower = [x['totalPower'] for x in c]
        time =  [x['_id'] for x in c]
        return time,totalPower,cursorList[medianInd][metricName]

    def getMetricSamples(self,k,metricName,sort=0):
        cursor = self.db[self.outCollectionName].find(
            {
                'numUsers': k,
                'sampleIndex': {'$ne': -1},
                metricName: {'$exists': True}
            },
            {
                metricName: True
            }
        )
        if sort > 0:
            cursor = cursor.sort({metricName: pymongo.ASCENDING})
        elif sort < 0:
            cursor = cursor.sort({metricName: pymongo.DESCENDING})
        return [x[metricName] for x in list(cursor)]

    def getMonitoringDeviceIds(self):
        return self.monitoringDeviceIds

    def getNumberUsers(self):
        return len(self.monitoringDeviceIds)

    def getSampleList(self,k):

        cursor = self.db.loadAggregationSamples.find({'_id': k})
        if cursor.count() < 1:
            return []

        return cursor.next()['ind']

    def getSampleStats(self,k):
        cursor = self.db[self.outCollectionName].find(
            {
                'numUsers':k,
                'sampleIndex': {'$ne': -1}
            }#,
            #{
            #    'max': True,
            #    'min': True,
            #    'avg': True,
            #    'cnt': True,
            #    'loadFactor': True
            #}
        )
        return list(cursor)

    def removeSample(self,k,ind):

        self.db.loadAggregationSamples.update(
            {'_id': k},
            {'$pull': {'ind': ind}}
        )

    def setupLoadAggregationCalculations(self,samplingInterval='fiveMinutes'):

        self.samplingInterval = samplingInterval
        self.outCollectionName = outCollectionPrefix + samplingInterval.capitalize()

        #Build indexes
        ind = [
            ('startTime', pymongo.ASCENDING),
            ('endTime', pymongo.ASCENDING),
            ('numUsers',pymongo.ASCENDING),
            ('sampleIndex', pymongo.ASCENDING),
        ]
        self.db[self.outCollectionName].create_index(ind,unique=True)
        ind = [
            ('numUsers', pymongo.ASCENDING),
            ('sampleIndex', pymongo.ASCENDING)
        ]
        self.db[self.outCollectionName].create_index(ind,unique=True)

        # Get all the meter Ids we will be working with for this system
        cursor = self.db.powerSystem.aggregate([
            {
                '$match': {'internalId': 'kit1'}
            },
            {
                '$project': {
                    '_id': 0,
                    'monitoringDeviceIds': '$serviceConnections.monitoringDeviceId'
                }
            }
        ])

        self.monitoringDeviceIds = cursor.next()['monitoringDeviceIds']

        #Find the month that has the most complete data
        cursor = self.db.month.aggregate([
            {
                '$match': {
                    'deviceId': {'$in': self.monitoringDeviceIds},
                    'tag': 'activePwr'
                }
            },
            {
                '$group': {
                    '_id': '$time',
                    'totalSamples': {'$sum': '$cnt'}
                }
            },

        ])
        sampleCounts = sorted(list(cursor), key = lambda t: t['totalSamples'])

        self.startTime = sampleCounts[-1]['_id']

        if self.samplingInterval == 'fiveMinutes' or self.samplingInterval == 'minute': #we only look at one month
            self.endTime = datetime(self.startTime.year, self.startTime.month + 1, 1) if self.startTime.month < 12 else datetime(self.startTime.year + 1, 1, 1)
        elif self.samplingInterval == 'day':
            self.endTime = datetime(self.startTime.year, self.startTime.month + 3, 1) if self.startTime.month < 10 else datetime(self.startTime.year + 1, self.startTime.month - 9)
        else:
            error(('Sampling interval {0} not supported').format(self.samplingInterval))
