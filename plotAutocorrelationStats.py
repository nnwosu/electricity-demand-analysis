import matplotlib.pyplot as plt
import numpy as np

from KitoboDatabase import KitoboDatabase

db = KitoboDatabase()
db.connect()
db.setupLoadAggregationCalculations(samplingInterval='day')
N = db.getNumberUsers()

lfs = []
totalStats = []
for k in range(1,N):
    lfs.append(db.getMetricSamples(k,'autocorrelation_1'))
    totalStats.append(db.getSampleStats(k))
db.disconnect()


fig, ax = plt.subplots(2,2)
iterRange = range(5,N,8)
maxLoad = 0
minLoad = float('inf')
i = 1
rowInd = [0, 0, 1, 1]
colInd = [0, 1, 0, 1]
for k in iterRange:
    #ax.append(plt.subplot(2,2,i))
    time,load,R = db.getMedianLoadProfile(k,'autocorrelation_1')
    load = [x/k*24/1000 for x in load]
    ax[rowInd[i-1],colInd[i-1]].plot(time,load)
    maxLoad = np.maximum(maxLoad,np.max(load))
    minLoad = np.minimum(minLoad,np.min(load))
    ax[rowInd[i-1],colInd[i-1]].set_ylabel('kWh/user/day')
    ax[rowInd[i-1],colInd[i-1]].set_title(('Median load profile for \n{} users'+
        '; R={:0.2f}').format(k,R))
    i += 1
for i in range(1,5):
    ax[rowInd[i-1],colInd[i-1]].set_ylim(minLoad,maxLoad)
fig.tight_layout()
fig.autofmt_xdate()
plt.savefig('Figures/autocorrelationMedianProfiles.png')

#Plot load facotr density
plt.figure()
plt.boxplot(lfs)
plt.xlabel('Number of users')
plt.ylabel('Autocorrelation (distribution)')
plt.xticks(np.arange(0,N,5))
plt.savefig('Figures/autoCorrelationDensity.png')
plt.close()

#Plot the samples used
plt.figure()
plt.plot(range(1,N),[len(x) for x in lfs])
plt.xlabel('Number of users')
plt.ylabel('Number of samples drawn')
plt.xticks(np.arange(0,N,5))
plt.savefig('Figures/autocorrelationSampleCounts.png')
plt.close()

#Plot number of users vs average power supply cost per users
# powerSupplyCost = 0.4 #$/W
# cost = []
# for k in range(1,len(totalStats)):
#     costK = [x['max']*powerSupplyCost/k for x in totalStats[k]]
#     cost.append(costK)
# plt.figure()
# plt.boxplot(cost)
# plt.xlabel('Number of users')
# plt.ylabel('Average power supply cost ($/user)')
# plt.title('Average power supply cost per user with $' + str(powerSupplyCost) + '/W cost')
# plt.xticks(np.arange(0,N,5))
# plt.savefig('Figures/powerSupplyCostPerUser.png')
# plt.close()

#Plot scatter of R vs kWh/day of individual load profiles
# y = [t['autocorrelation_1']/1000 for t in totalStats[0]]
# x = [i for i in range(1,len(totalStats[0])+1)]
# plt.figure()
# plt.scatter(x,y)
# ymin, ymax = plt.ylim()
# plt.ylim(0,ymax)
# xmin, xmax = plt.xlim()
# plt.xlim(0,xmax)
# plt.xlabel('Average consumption (kWh/day)')
# plt.ylabel('Autocorrelation Coefficient')
# plt.title('Autocorrelation for individual users in population')
# plt.savefig('Figures/averageVsR_k_1')
# plt.close()

#plot a histogram of load factor for k = 3
for k in range(1,N):
    plt.figure()
    if (k == 1 or k == N-1):
        nbins = 10
    else:
        nbins = 50
    plt.hist(lfs[k-1],bins=nbins, weights=np.zeros_like(lfs[k-1])+1./len(lfs[k-1]))
    plt.xlabel('Autocorrelation Coefficient')
    plt.ylabel('Frequency; N=' + str(len(lfs[k-1])))
    plt.title('Distribution of autocorrelation coefficient over samples of ' + str(k) + ' users')
    plt.savefig('Figures/autocorrelationHist_' + str(k) + '.png')
    plt.close()
