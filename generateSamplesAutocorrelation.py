from math import factorial
from math import floor

import numpy
import itertools

from KitoboDatabase import KitoboDatabase


maxIter = 1000 #absolute max number of samples
tol= 0.001 #tolerance for autocorrelation convergence

startK = 1
n = 1 #order of autocorrelation

#Open connection
db = KitoboDatabase()
db.connect()
db.setupLoadAggregationCalculations(samplingInterval='day')
N = db.getNumberUsers()

for k in range(startK,N+1):
    sampleList = db.getSampleList(k) #Sample list saves a randomly generated list of load profiles that are sampled
    numIter = 0
    numCombinations = floor(factorial(N)/factorial(k)/factorial(N-k))

    prevStdDev = 0
    prevT = tol
    while numIter < min(maxIter,numCombinations): #change to check OR convergence in standard deviation
        #Find a sample that hasn't been used before
        if (numIter >= len(sampleList)): #then we need to generate new samples
            while True:
                ind = [int(x) for x in sorted(numpy.random.choice(numpy.arange(0, N), size=k, replace=False))]
                if ind not in sampleList:
                    sampleList = db.appendSample(k,ind)
                    break
        else:
            ind = sampleList[numIter]
        try:
            db.calculateAutocorrelation(ind,n,sampleIndex=numIter) #Calculates (and saves) statistics of the aggregated load
        except IndexError:
            db.removeSample(k,ind)
            sampleList = db.getSampleList(k)
            print('Invalid ind: ' + str(ind))
            continue

        newStdDev = db.calculateMetricStdDev(k,numIter,('autocorrelation_{0}').format(1))

        if prevStdDev > 0:
            t = abs(newStdDev-prevStdDev)/prevStdDev
            if numIter % 100 == 0:
                print('k='+str(k)+',numIter='+str(numIter))

            if t < tol and prevT < tol:
                break
            prevT = t

        prevStdDev = newStdDev

        numIter += 1

    #save information for k (min, max, mean, std, cnt)

db.disconnect()
