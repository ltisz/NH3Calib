## -*- coding: utf-8 -*-
## DATAHARVESTNH3.py -- Takes raw data from Ammonia/Amine CIMS (In format amin_H_YYYY_MM_DD_HH-MM-SS.txt)
##                     and produces a comma-delimited output file with signal minus backgroud for each
##                     valve cycle. Assumes scrubber valve is V1, 1 = scrubber ON, 0 = scrubber OFF
##                     Also produces concentration data for NH3, C1, C2, C3 amines based on calibration
##                     factors
## Use: Put program in same folder as NH3 CIMS data. Adjust starttime and endtime variables as needed.
##     Output will be a comma-delimited .txt file named NH3MM_DD_YYYY_HHMM-MM_DD_YYYY_HHMM with date/times
##     as start time/end time, respectively.
## Dependencies:
##     Numpy
## Categories:
##     Instrumentation
## Author:
##     Lee Tiszenkel, UAH
## Date created:
##     2/26/2022
## Updated:
##     6/28/2022

import numpy as np
import datetime as dt
import glob
import os
import sys
from statistics import mean

os.chdir(os.path.dirname(sys.argv[0]))  #Change working folder to folder that script is in

starttime = "06-02-2022_0900"           #Data start time
endtime = "06-02-2022_2200"             #Data end time

nh3Factor = 13.11
c1Factor = 8.58
c2Factor = 2.57
c3Factor = 4.34

gmtCorr = dt.timedelta(hours=5)         #CIMS saves data at GMT, this is correction factor (need to change based on DST as needed)

filenameOut = "NH3{}-{}.txt".format(starttime,endtime)                      #Filename is NH3[starttime]-[endtime].txt

def flexMean(chunk):
    #if isinstance(
    return 0

def createMaster(starttime, endtime):
    starttimeDT = dt.datetime.strptime(starttime, "%m-%d-%Y_%H%M%S")+gmtCorr      #Convert starttime and endtime to datetime objects, correct for GMT offset
    endtimeDT = dt.datetime.strptime(endtime, "%m-%d-%Y_%H%M%S")+gmtCorr
    files = glob.glob("amin_H*")            #Grab all "amin_H" files in current folder

    with open(filenameOut, "w+") as f:
        f.write("{},{},{},{},{},{},{},{},{},{},{},{},{}\n".format("time","valve","Hz18","Hz63","Hz47","Hz93","Hz140","Hz30","Hz46","Hz60","Hz74","Hz88","Hz102"))
        
    for file in files:          #Iterate through each file retrieved in glob
        fileTime = dt.datetime.strptime(file.split("H_")[1].split(".txt")[0], "%Y-%m-%d_%H-%M-%S")
        if fileTime > starttimeDT-dt.timedelta(minutes=20) and fileTime < endtimeDT+dt.timedelta(minutes=20):
            print(file)
            dates = np.genfromtxt(file, dtype=str, skip_header=1, usecols = (2,3,4,5,6,7))      #Make numpy array of date/times
            datesList = []
            
            for date in dates[1:]:
                datesList.append(dt.datetime.strptime(" ".join(date), "%Y %m %d %H %M %S"))     #Convert numpy array to list of strings for output

            Hz18 = np.genfromtxt(file, skip_header=1, usecols=27)       #Ammonia       
            Hz63 = np.genfromtxt(file, skip_header=1, usecols=24)       #Ammonia + EtOH
            #Hz110 = np.genfromtxt(file, skip_header=1, usecols=48)     #Ammonia + (EtOH)2
            Hz47 = np.genfromtxt(file, skip_header=1, usecols=15)       #EtOH monomer
            Hz93 = np.genfromtxt(file, skip_header=1, usecols=18)       #EtOH dimer
            Hz140 = np.genfromtxt(file, skip_header=1, usecols=21)      #EtOH trimer
            Hz30 = np.genfromtxt(file, skip_header=1, usecols=30)       #C1 Amine
            Hz46 = np.genfromtxt(file, skip_header=1, usecols=33)       #C2 Amine
            Hz60 = np.genfromtxt(file, skip_header=1, usecols=36)       #C3 Amine
            Hz74 = np.genfromtxt(file, skip_header=1, usecols=39)       #C4 Amine
            Hz88 = np.genfromtxt(file, skip_header=1, usecols=42)       #C5 Amine
            Hz102 = np.genfromtxt(file, skip_header=1, usecols=45)      #C6 Amine
            
            valve = np.genfromtxt(file, skip_header=1, usecols=11)      #Valve on/off (on=scrubber, off=no scrubber)
            i = 0
            while i < len(datesList):   #Start loop through each line of file
                dataLine = "{},{},{},{},{},{},{},{},{},{},{},{},{}\n".format(
                    dt.datetime.strftime(datesList[i]-gmtCorr, "%d-%m-%Y_%H%M%S"), 
                    str(valve[i]), 
                    str(Hz18[i]),
                    str(Hz63[i]),
                    str(Hz47[i]),
                    str(Hz93[i]),
                    str(Hz140[i]),
                    str(Hz30[i]),
                    str(Hz46[i]),
                    str(Hz60[i]),
                    str(Hz74[i]),
                    str(Hz88[i]),
                    str(Hz102[i]))
                if datesList[i] > starttimeDT and datesList[i] < endtimeDT:         #If data matches between start time and end time,
                    with open(filenameOut, "a+") as f:                              #Write to output file
                        f.write(dataLine)
                i = i+1

# calibCalc: Takes bulk file created in createMaster and outputs a file containing columns for Time, 18Hz and 63Hz signals subtracted from background, and total Ethanol signals.
def calibCalc(filenameOut):
    dataDate = np.genfromtxt(filenameOut, dtype=str, delimiter=',', skip_header=1, usecols=0)   #Get dates
    dataValv = np.genfromtxt(filenameOut, delimiter=',', skip_header=1, usecols=1)              #Get valve state (1 = scrubber on, 0 = scrubber off)
    signals = np.genfromtxt(filenameOut, delimiter=',', skip_header=1, usecols=(2,3,4,5,6,7,8,9))     #Get signals (18, 63, 47, 93, 140, 32, 46, 60)

    bgList = []         #list of background measurements
    msList = []         #list of calibration measurements
    bgListChunks = []   #list of measurement "Chunks" - 10 minute segments of time for each valve cycle
    msListChunks = []
    bgListAvgs = []     #list of averages
    msListAvgs = []
    
    #Creating lists of background and measurement based on valve state
    i = 0
    while i<len(dataDate):
        if dataValv[i] == 0:        #valve = 1, A1 to inlet
            bgList.append([dataDate[i], np.ndarray.tolist(signals[i])])
        elif dataValv[i] == 1:      #valve = 0, A1 to vent
            msList.append([dataDate[i], np.ndarray.tolist(signals[i])])
        else:
            pass
        i = i+1
    
    #creating "Chunks"
    i = 0
    bgListTemp = []
    while i<len(bgList)-1:
        if (dt.datetime.strptime(bgList[i+1][0], "%d-%m-%Y_%H%M%S") - dt.datetime.strptime(bgList[i][0],"%d-%m-%Y_%H%M%S") < dt.timedelta(minutes=5)):  #checking to see if measurement is less 
            tempList = []                                                                                                                               #than 5 minutes after next measurement,
            tempList.append(dt.datetime.strptime(bgList[i][0], "%d-%m-%Y_%H%M%S"))                                                                      #boundary between two chunks.
            genx = (x for x in bgList[i][1])                                                                                                            #If next measurement is less than 5 minutes
            for x in genx:                                                                                                                              #it appends that data to a temporary list
                tempList.append(x)
            bgListTemp.append(tempList)
        else:                                       #If the amount of time between measurements IS more than 5 minutes,
            bgListChunks.append(bgListTemp)         #it cuts off the "chunk" and clears the temporary list.
            bgListTemp = []
        i = i+1
    
    #Same loop as before, but for measurement instead of background
    i = 0
    msListTemp = []
    while i<len(msList)-1:
        if (dt.datetime.strptime(msList[i+1][0], "%d-%m-%Y_%H%M%S") - dt.datetime.strptime(msList[i][0],"%d-%m-%Y_%H%M%S") < dt.timedelta(minutes=5)):
            tempList = []
            tempList.append(dt.datetime.strptime(msList[i][0], "%d-%m-%Y_%H%M%S"))
            genx = (x for x in msList[i][1])
            for x in genx:
                tempList.append(x)
            msListTemp.append(tempList)            
        else:
            msListChunks.append(msListTemp)
            msListTemp = []
        i = i+1
    
    #Averaging the measurements in a chunk, omitting first 2 minutes of each measurement
    timeAvgs = []
    timeAvgsbg = []
    bgAvgs = []
    msAvgs = []
    #print(len(bgListChunks))
    #print(msListChunks)
    for chunk in bgListChunks:  #Iterating across each "chunk"
        i=60                    #Starting at i=60, 2 minutes in to "Chunk"
        avgTimebg = []
        avg18bg = []
        avg63bg = []
        avg47bg = []
        avg93bg = []
        avg140bg = []
        avg32bg = []
        avg46bg = []
        avg60bg = []
        while i<len(chunk):                 #Appending each measurement 
            avgTimebg.append(chunk[i][0])
            avg18bg.append(chunk[i][1])
            avg63bg.append(chunk[i][2])
            avg47bg.append(chunk[i][3])
            avg93bg.append(chunk[i][4])
            avg140bg.append(chunk[i][5])
            avg32bg.append(chunk[i][6])
            avg46bg.append(chunk[i][7])
            avg60bg.append(chunk[i][8])
            i=i+1
        timeAvgsbg.append(avgTimebg[0] + (avgTimebg[-1] - avgTimebg[0])/2)      #Averaging time, so each data point is from the center of the valve cycle
        bgAvgs.append([                                                         #Averaging each signal
                      sum(avg18bg)/len(avg18bg),
                      sum(avg63bg)/len(avg63bg),
                      sum(avg47bg)/len(avg47bg),
                      sum(avg93bg)/len(avg93bg),
                      sum(avg140bg)/len(avg140bg),
                      sum(avg32bg)/len(avg32bg),
                      sum(avg46bg)/len(avg46bg),
                      sum(avg60bg)/len(avg60bg)
                      ])

    #Same loop as before, but for measurement chunks
    for chunk in msListChunks:
        try:
            i=60
            avgTime = []
            avg18 = []
            avg63 = []
            avg47 = []
            avg93 = []
            avg140 = []
            avg32 = []
            avg46 = []
            avg60 = []
            while i<len(chunk):
                avgTime.append(chunk[i][0])
                avg18.append(chunk[i][1])
                avg63.append(chunk[i][2])
                avg47.append(chunk[i][3])
                avg93.append(chunk[i][4])
                avg140.append(chunk[i][5])
                avg32.append(chunk[i][6])
                avg46.append(chunk[i][7])
                avg60.append(chunk[i][8])
                i=i+1
            timeAvgs.append(avgTime[0] + (avgTime[-1] - avgTime[0])/2)
            msAvgs.append([
                          sum(avg18)/len(avg18),
                          sum(avg63)/len(avg63),
                          sum(avg47)/len(avg47),
                          sum(avg93)/len(avg93),
                          sum(avg140)/len(avg140),
                          sum(avg32)/len(avg32),
                          sum(avg46)/len(avg46),
                          sum(avg60)/len(avg60)
                          ])
        except:
            print("Invalid chunk: {}".format(chunk))
    #Converting to numpy arrays to make easier to manipulate
    bgAvgsNP = np.array(bgAvgs)
    msAvgsNP = np.array(msAvgs)
    nh3signals = []
    c1signals = []
    c2signals = []
    c3signals = []
    ethsignals = []
    #zipping each list to make tuples containing adjacent average measurement and average background signals
    #Also combining all ethanol signals for that time period
    for combo in zip(bgAvgsNP,msAvgsNP):
        nh3signals.append(combo[1][:2]-combo[0][:2])
        ethsignals.append(np.sum(combo[0][2:5]))
        c1signals.append(combo[1][5]-combo[0][5])
        c2signals.append(combo[1][6]-combo[0][6])
        c3signals.append(combo[1][7]-combo[0][7])
    print(nh3signals)
    print(ethsignals)
    print(len(ethsignals))
    print(len(signals))
    print(len(timeAvgs))
    
    ammoniaNormals = []
    c1normals = []
    c2normals = []
    c3normals = []
    i=0
    while i < len(ethsignals):
        ammoniaConc = ((np.sum(nh3signals[i])/(1e6/ethsignals[i]))/nh3Factor) if np.sum(nh3signals[i]) > 0 else 0
        c1Conc = ((c1signals[i]/(1e6/ethsignals[i]))/c1Factor) if c1signals[i] > 0 else 0
        c2Conc = ((c2signals[i]/(1e6/ethsignals[i]))/c2Factor) if c2signals[i] > 0 else 0
        c3Conc = ((c3signals[i]/(1e6/ethsignals[i]))/c3Factor) if c3signals[i] > 0 else 0
        ammoniaNormals.append(ammoniaConc)
        c1normals.append(c1Conc)
        c2normals.append(c2Conc)
        c3normals.append(c3Conc)
        i += 1
    i=0
    #Writing to nh3calibYYYYMMDD.txt
    #with open("nh3calib{}.txt".format(dt.datetime.strftime(dt.datetime.now(),"%m%d%M"),"w+")) as f:
    with open("nh3calib" + dt.datetime.strftime(dt.datetime.now(),"%m%d%M") + ".txt","w+") as f:
        f.write("date, NH3 signal (Signal-BG), C1 signal (Signal-BG), C2 signal (Signal-BG), C3 signal (Signal-BG), EtOH signal\n")
        while i < len(ethsignals):
            f.write("{},{},{},{},{},{}\n".format(
                dt.datetime.strftime(timeAvgs[i],"%Y/%m/%d %H:%M:%S"),
                np.sum(nh3signals[i]),
                c1signals[i],
                c2signals[i],
                c3signals[i],
                ethsignals[i]))
            i=i+1
    i=0

    with open("nh3concs{}-{}.txt".format(starttime,endtime),"w+") as f:
        f.write("date, [NH3], [MA], [DMA], [TMA]\n")
        while i < len(ethsignals):
            f.write("{},{},{},{},{}\n".format(
                dt.datetime.strftime(timeAvgs[i],"%Y/%m/%d %H:%M:%S"),
                ammoniaNormals[i],
                c1normals[i],
                c2normals[i],
                c3normals[i]
            ))
            i=i+1

    with open("tempbg.txt","w+") as f:
        for bg in bgList:
            f.write("{}, {}\n".format(bg[0],",".join(str(x) for x in bg[1:5])[1:-1]))
    with open("tempms.txt","w+") as f:
        for ms in msList:
            f.write("{}, {}\n".format(ms[0],",".join(str(x) for x in ms[1:5])[1:-1]))
    
    
#createMaster(starttime, endtime)
calibCalc(filenameOut)