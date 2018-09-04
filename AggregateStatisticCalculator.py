from math import factorial
from math import floor

import numpy
import itertools

from KitoboDatabase import KitoboDatabase

class AggregateStatisticCalculator:

    def __init__(self,dataSource,samplingInterval,statistic,statisticParameters=[],maxIterations=1000,tol=0.0001):
        if not (dataSource in ['kitobo']):
            raise ArgumentException('Data source not recognized')
        if not (statistic in [
            'autocorrelation', 'cov', 'loadFactor', 'loadFactorPercentile'
        ]):
            raise ArgumentException('Statistic not recognized')
        if not (samplingInterval in [
            'second', 'fiveSeconds', 'fifteenSeconds', 'minute',
            'fiveMinutes', 'fifteenMinutes', 'hour', 'day', 'week', 'month',
            'year'
        ]):
            raise ArgumentException('Sampling interval not recognized')

        self.dataSource = dataSource
        self.samplingInterval = samplingInterval
        self.statistic = statistic
        self.statisticParameters = statisticParameters
        self.maxIterations = maxIterations
        self.tol = tol

    def connect(self):
        if (self.dataSource == 'kitobo'):
            self.db = KitoboDatabase()
            self.db.connect()
            self.db.setupLoadAggregationCalculations(samplingInterval=self.samplingInterval)
            self.N = self.db.getNumberUsers()

    def disconnect(self):
        self.db.disconnect

    def generateSamples(self,startK=1):

        metricName = self.statistic
        if (self.statistic == 'autocorrelation'):
            calculateStatistic = lambda ind,numIter: self.db.calculateAutocorrelation(ind,self.statisticParameters[0],sampleIndex=numIter)
            metricName = 'autocorrelation_{}'.format(self.statisticParameters[0])
        elif (self.statistic == 'loadFactor'):
            calculateStatistic = lambda ind,numIter: self.db.calculateAggregateLoadStats(ind,sampleIndex=numIter)
        elif (self.statistic == 'cov'):
            calculateStatistic = lambda ind,numIter: self.db.calculateCOV(ind,sampleIndex=numIter)
        elif (self.statistic == 'loadFactorPercentile'):
            calculateStatistic = lambda ind, numIter: (
                self.db.calculateLoadFactorPercentile(
                    ind, self.statisticParameters[0], sampleIndex=numIter
                )
            )
            metricName = 'loadFactor_{}'.format(
                self.statisticParameters[0][-1]  # Use the last percentile
            )

        for k in range(startK,self.N+1):
            print('k={}'.format(k))
            sampleList = self.db.getSampleList(k) #Sample list saves a randomly generated list of load profiles that are sampled
            numIter = 0
            numCombinations = floor(factorial(self.N)/factorial(k)/factorial(self.N-k))

            prevStdDev = 0
            prevT = self.tol
            while numIter < min(self.maxIterations,numCombinations): #change to check OR convergence in standard deviation
                #Find a sample that hasn't been used before
                if (numIter >= len(sampleList)): #then we need to generate new samples
                    while True:
                        ind = [int(x) for x in sorted(numpy.random.choice(numpy.arange(0, self.N), size=k, replace=False))]
                        if ind not in sampleList:
                            sampleList = self.db.appendSample(k,ind)
                            break
                else:
                    ind = sampleList[numIter]
                try:
                    calculateStatistic(ind,numIter)
                except IndexError:
                    self.db.removeSample(k,ind)
                    sampleList = self.db.getSampleList(k)
                    print('Invalid ind: ' + str(ind))
                    continue

                newStdDev = self.db.calculateMetricStdDev(k,numIter,metricName)

                if prevStdDev > 0:
                    t = abs(newStdDev-prevStdDev)/prevStdDev
                    if numIter % 100 == 0:
                        print('k='+str(k)+',numIter='+str(numIter))

                    if t < self.tol and prevT < self.tol:
                        break
                    prevT = t

                prevStdDev = newStdDev

                numIter += 1

    def getSamplesByNumberUsers(self, metricNames=[]):
        if (metricNames == []):
            metricNames = self.statistic
        samples = []
        for k in range(1,self.N+1):
            samples.append(self.db.getMetricSamples(k, metricNames))

        return samples

    def getSampleStatsBasic(self):
        basicStats = []
        for k in range(1, self.N+1):
            basicStats.append(self.db.getSampleStats(k))

        return basicStats
