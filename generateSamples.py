from math import factorial
from math import floor

import numpy
import itertools

from KitoboDatabase import KitoboDatabase


maxIter = 10000 #absolute max number of samples
tol= 0.00001 #tolerance for relative standard deviation convergence

startK = 7;

#Open connection
db = KitoboDatabase()
db.connect()
db.setupLoadAggregationCalculations()
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
            db.calculateAggregateLoadStats(ind,sampleIndex=numIter) #Calculates (and saves) statistics of the aggregated load
        except IndexError:
            db.removeSample(k,ind)
            sampleList = db.getSampleList(k)
            print('Invalid ind: ' + str(ind))
            continue

        loadFactorStats = db.calculateLoadFactorStats(k,numIter) #Calculates (and saves) statistics of the load factor from the samples realized
        #insert check to break if stdev is stable

        newStdDev = db.calculateLoadFactorStdDev(k,numIter)

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
