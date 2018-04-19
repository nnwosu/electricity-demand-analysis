import configparser
from datetime import datetime
from pymongo.database import Database
from pymongo import MongoClient
from statistics import stdev

timeInterval = 'fiveMinutes'
outCollectionName = 'aggregateLoadStats' + timeInterval.capitalize()
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
            self.db[outCollectionName].delete({'monitoringDeviceIds': filterMonitoringDeviceIds})

        cursor = self.db[outCollectionName].find({'monitoringDeviceIds': filterMonitoringDeviceIds})
        if cursor.count() > 0:
            stats = cursor.next()
            return stats

        cursor = self.db[timeInterval].aggregate([
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
                    'timeInterval': {'$literal': timeInterval},
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

        self.db[outCollectionName].insert(stats)
        return stats

    def calculateLoadFactor(self,ind):

        stats = self.calculateAggregateLoadStats(ind)
        loadFactor = stats.loadFactor

    def calculateLoadFactorStats(self,k,sampleIndex,overwrite=False):

        q = {
            '_id.numUsers': k,
            '_id.sampleIndex': sampleIndex
        }
        if overwrite:
            self.db[loadFactorCollectionName].delete(q)

        cursor = self.db[loadFactorCollectionName].find(q) #Stats for up to this sample index have already been found so return
        if cursor.count() > 0:
            return cursor.next()

        cursor = self.db[outCollectionName].aggregate([
            {
                '$match': {
                    'numUsers': k,
                    'sampleIndex': {'$lte': sampleIndex}
                }
            },
            {
                '$group': {
                    '_id': {
                        'numUsers': {'$literal': k},
                        'sampleIndex': {'$literal': sampleIndex}
                    },
                    'avg': {'$avg': '$loadFactor'},
                    #'std': {'$stdDevSamp': '$loadFactor'},
                    'max': {'$max': '$loadFactor'},
                    'min': {'$min': '$loadFactor'},
                    'cnt': {'$sum': 1}
                }
            },
            {
                '$project': {
                    'avg': True,
                    'max': True,
                    'min': True,
                    'cnt': True,
                    'numUsers': {'$literal': k},
                    'sampleIndex': {'$literal': sampleIndex}
                }
            }
        ])

        try:
            stats = cursor.next()
        except StopIteration:
            raise IndexError('No results in ' + outCollectionName + ' with k=' + k + ',sampleIndex<=' + sampleIndex)

        if stats['cnt'] <= sampleIndex:
            raise IndexError('sampleIndex beyond number of aggregate load profiles')
        self.db[loadFactorCollectionName].insert(stats)
        return stats

    def calculateLoadFactorStdDev(self,k,sampleIndex):

        if sampleIndex < 1:
            return 0

        cursor = self.db[outCollectionName].find(
            {
                'numUsers': k,
                'sampleIndex': {'$lte': sampleIndex}
            },
            {
                '_id': 0,
                'loadFactor': 1
            }
        )
        loadFactor = [x['loadFactor'] for x in list(cursor)]
        return stdev(loadFactor)

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
        cursor = self.db[outCollectionName].find(
            {
                'numUsers':k,
                'sampleIndex': {'$ne': -1}
            },
            {
                'max': True,
                'min': True,
                'avg': True,
                'cnt': True,
                'loadFactor': True
            }
        )
        return list(cursor)

    def getLoadFactorSamples(self,k):
        cursor = self.db[outCollectionName].find(
            {
                'numUsers': k,
                'sampleIndex': {'$ne': -1}
            },
            {
                'loadFactor': True
            }
        )
        return [x['loadFactor'] for x in list(cursor)]

    def getLoadFactorStats(self,numUsers,numSamples):

        return self.db[loadFactorCollectionName].find_one({
            '_id': {
                'numUsers': numUsers,
                'numSamples': numSamples
            }
        })

    def removeSample(self,k,ind):

        self.db.loadAggregationSamples.update(
            {'_id': k},
            {'$pull': {'ind': ind}}
        )

    def setupLoadAggregationCalculations(self):

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
        self.endTime = datetime(self.startTime.year, self.startTime.month + 1, 1) if self.startTime.month < 12 else datetime(self.startTime.year + 1, 1, 1)
