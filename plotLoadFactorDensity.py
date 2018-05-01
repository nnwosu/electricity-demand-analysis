import matplotlib.pyplot as plt
import numpy as np

from KitoboDatabase import KitoboDatabase

db = KitoboDatabase()
db.connect()
db.setupLoadAggregationCalculations()
N = db.getNumberUsers()

lfs = []
totalStats = []
for k in range(1,N):
    lfs.append(db.getMetricSamples(k,'loadFactor'))
    totalStats.append(db.getSampleStats(k))
db.disconnect()

#Plot load facotr density
plt.figure()
plt.boxplot(lfs)
plt.xlabel('Number of users')
plt.ylabel('Load factor (distribution)')
plt.xticks(np.arange(0,N,5))
plt.savefig('Figures/loadFactorDensity.png')
plt.close()

#Plot the samples used
plt.figure()
plt.plot(range(1,N),[len(x) for x in lfs])
plt.xlabel('Number of users')
plt.ylabel('Number of samples drawn')
plt.xticks(np.arange(0,N,5))
plt.savefig('Figures/loadFactorSampleCounts.png')
plt.close()

#Plot number of users vs average power supply cost per users
powerSupplyCost = 0.4 #$/W
cost = []
for k in range(1,len(totalStats)):
    costK = [x['max']*powerSupplyCost/k for x in totalStats[k]]
    cost.append(costK)
plt.figure()
plt.boxplot(cost)
plt.xlabel('Number of users')
plt.ylabel('Average power supply cost ($/user)')
plt.title('Average power supply cost per user with $' + str(powerSupplyCost) + '/W cost')
plt.xticks(np.arange(0,N,5))
plt.savefig('Figures/powerSupplyCostPerUser.png')
plt.close()

#Plot scatter of kW vs kWh/day of individual load profiles
y = [t['max']/1000 for t in totalStats[0]]
x = [t['avg']*24/1000 for t in totalStats[0]]
plt.figure()
plt.scatter(x,y)
ymin, ymax = plt.ylim()
plt.ylim(0,ymax)
xmin, xmax = plt.xlim()
plt.xlim(0,xmax)
plt.xlabel('Average consumption (kWh/day)')
plt.ylabel('Peak consumption (kW)')
plt.title('Average vs peak consumption for individual users in population')
plt.savefig('Figures/averageVsPeak_k_1')
plt.close()

#plot a histogram of load factor for k = 3
for k in range(1,N):
    plt.figure()
    plt.hist(lfs[k-1],bins=50, weights=np.zeros_like(lfs[k-1])+1./len(lfs[k-1]))
    plt.xlabel('Load factor')
    plt.ylabel('Frequency')
    plt.title('Distribution of load factor over samples of ' + str(k) + ' users')
    plt.savefig('Figures/loadFactorHist_' + str(k) + '.png')
    plt.close()
