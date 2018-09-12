######################################################
# This file contains various functions to query 
# data from the Pecan Street database and to compute
# statistics on aggregated data. 

# Imports
# Need the following to import the pecanpy library
import sys
sys.path.append('C:\\Users\\mohini\\Documents\\LoadModelingAndAgg\\pecan_street\\PecanPy\\examples')

import datetime as dat
from datetime import timezone
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pecanpy
import scipy.stats as stats
import scipy as sp
import itertools
import numpy.random as random
from matplotlib.pyplot import cm
import configparser as cp

# The following function allows us to efficiently compute different aggregate statistics on 
# many samples of aggregated load. 
def aggLoadStats(loadMat, aggLevels, statList, statArgs=None, samplesPerLevel=100, verbose=False):
# loadMat : [NxT] PANDAS Matrix of load measurements for N customers at T time points. 
# aggLevels: [A] Array of A aggregation levels. max(A) <= N
# statList: [length S list of functions]. Statistics to be computed on the aggregate load curves. 
# Every function in the list should take a (MxT) matrix argument where M is the number of chosen loads and
# a list of optional arguments. 
# samplesPerLevel: For N total customers and an aggregation level of k, we have
# N choose k possibilities. This might be too large a number to compute, so we limit
# the maximum number of aggregate samples we take at a given aggregation level. 
    [N, T] = np.shape(loadMat);
    
    # Some checks of the validity of args
    if max(aggLevels) > N:
        print("Warning: The highest level of aggregation is greater than available loads.")
    if (statArgs != None) and len(statList) != len(statArgs):
        print("Arguments given, but number of stats and number of args unequal.")
    
    nAggLevels = np.size(aggLevels);
    numStats = len(statList); 
    
    # Set up the matrix for gathering results. 
    loadStats = np.nan*np.ones([nAggLevels, samplesPerLevel, numStats]);
    
    for i in range(nAggLevels):
        if verbose:
            print("Agg level: " + str(i))
        m = aggLevels[i];
        # Total number of possible combinations
        Nchoosem = int(sp.special.comb(N, m))
        
        if Nchoosem < samplesPerLevel:
            # Iterate through all possible combinations
            allCombs = itertools.combinations(np.arange(N), m)
            j = 0;
            for chosen in allCombs:
                chosen = np.array(chosen)
                chosenLoad = loadMat.iloc[chosen, :];
                # Compute and save all statistics
                for k in range(numStats):
                    statFunc = statList[k];
                    # Get arguments for this statistic
                    if statArgs == None:
                        argk = None;
                    else: 
                        argk = statArgs[k];
                    loadStats[i, j, k] = statFunc(chosenLoad, arg=argk);
                j = j + 1;
        else:
            # Generate "samplesPerLevel" of random combinations
            for j in range(samplesPerLevel):
                chosen = random.choice(N, size=m, replace=False);
                chosenLoad = loadMat.iloc[chosen, :]
                # Compute and save all statistics
                for k in range(numStats):
                    statFunc = statList[k];
                    # Get arguments for this statistic
                    if statArgs == None:
                        argk = None;
                    else:
                        argk = statArgs[k];
                    loadStats[i, j, k] = statFunc(chosenLoad, arg=argk);
                    
    return loadStats
    
###################################################################
# Statistics we wish to compute on aggregate load
# All these functions take an MxT matrix argument where 
# M = number of loads (some subset of all loads)
# T = number of measurement time points. 
###################################################################

def meanTotalLoad(load, arg=None):
    return np.mean(np.sum(load, axis=0));

def varTotalLoad(load, arg=None):
    return np.var(np.sum(load, axis=0));

def meanTotalLoadPerUser(load, arg=None):
    [M, T] = np.shape(load);
    return np.mean(np.sum(load, axis=0) / float(M));

def varTotalLoadPerUser(load, arg=None):
    [M, T] = np.shape(load);
    avgLoad = np.sum(load, axis=0) / float(M);
    return np.var(avgLoad)
                
def loadFactor(load, arg=None):
    totalLoad = np.sum(load, axis=0);
    maxLoad = np.max(totalLoad);
    meanLoad = np.mean(totalLoad);
    return meanLoad / maxLoad

def cvLoad(load, arg=None):
    totalLoad = np.sum(load, axis=0);
    meanLoad = np.mean(totalLoad);
    sigLoad = np.sqrt(np.var(totalLoad));
    return sigLoad / meanLoad

# This function aims to give us a sense of the predictability of the
# load as aggregation increases. 
def hourlyVar(load, arg=[12]):
    hour = arg[0];
    totalLoad = np.sum(load, axis=0);
    # Get time indices of total load
    times = pd.DatetimeIndex(totalLoad.index)
    # Get indices of measurements at the hour of interest
    hour_idx = (times.hour==hour);
    return np.var(totalLoad.iloc[hour_idx]);

# This function has the same aim as 'hourlyVar' but we normalize
# as so it is a coefficient of variation of load at a given hour
def hourlyCVLoad(load, arg=[12]):
    hour = arg[0];
    totalLoad = np.sum(load, axis=0);
    # Get time indices of total load
    times = pd.DatetimeIndex(totalLoad.index)
    # Get indices of measurements at the hour of interest
    hour_idx = (times.hour==hour);
    # Get load the hour
    hourLoad = totalLoad.iloc[hour_idx];
    hourMean = np.mean(hourLoad); hourSig = np.sqrt(np.var(hourLoad));
    return hourSig / hourMean

# This is a generalization of the load factor which uses a percentile
# rather than the maximum. 
def genLoadFactor(load, arg=[100]):
    percentile = arg[0];
    totalLoad = np.sum(load, axis=0);
    # Get the load at the percentile specified
    perLoad = np.percentile(totalLoad, percentile);
    meanLoad = np.mean(totalLoad);
    return meanLoad / perLoad

# This function is useful for dealing with the NaN values present in the
# output of the aggLoadStats function. This is useful for plotting the results
# without generating errors. 
def removeNans(data):
# data : [numSamples x numAggLevels] Matrix of statistics for numSamples at numAggLevels
# This function converts the 2D input data into a list of arrays where each array corresponds
# to a column in the input. NaNs are removed when converting columns to list elements. 
    mask = ~np.isnan(data)
    filtered_data = [d[m] for d, m in zip(data.T, mask.T)]
    return filtered_data;
