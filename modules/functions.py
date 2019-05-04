#aperQL: modules for quicklook pipeline for Apertif data
# E.A.K. Adams 02/05/2019

__author__="E.A.K. Adams"

import os
import numpy as np
import casacore.tables as pt
from apercal.subs.getdata_alta import getdata_alta

def get_cal_tids(calstart,calend,beams):
    """Take starting calibrator taskid, 
    ending calibrator taskid, and beam list
    Return list of task ids for the calibrator
    scans matched to beams
    """
    #first check if cal scan crosses a day or not
    #get date for start & end observation
    datestart = calstart[0:6]
    dateend = calend[0:6]
    cal_tids = np.empty(len(beams),dtype=np.object)
    if datestart == dateend:
        #observations on same day
        #this is the easy case
        scanstart = int(calstart[-3:])
        scannums = beams + scanstart
        for i,scan in enumerate(scannums):
            cid = '{0}{1:0>3}'.format(datestart,scan)
            cal_tids[i] = cid
    else:
        #cal scans cross a day
        #this is the complicated case
        #figure out how many scans are on the end day
        endscan = int(calend[-3:])
        #scan numbers start at one
        #so this is number of scans on second day
        firstscan = int(calstart[-3:])
        #last scan number on first day
        endfirstday = firstscan + 40 - endscan
        nfirstday = 40-endscan #number scans first day
        for i,bm in enumerate(beams):
            if bm < nfirstday:
                #on first day
                scan = firstscan+bm
                cid = '{0}{1:0>3}'.format(datestart,scan)
                cal_tids[i] = cid
            else:
                #on second day
                scan = (bm +1 ) - nfirstday
                cid = '{0}{1:0>3}'.format(dateend,scan)
                cal_tids[i] = cid

    return cal_tids


def get_data(taskid,cal_tids,beams):
    """Takes target taskid, array of calibrator taskids, array of beams.
    Uses apercal (altadata) functionality to copy data over.
    May be better to use altadata repository directory but that
    is not globally available to all happili users like apercal.
    Uses similar data structure to apercal but not the
    subdirectories.
    The location where data is written is hardcoded.
    Future functionality could make this more flexible.
    """
    #first make top level local directory
    #hardcode this to top level on local happili node
    #Make a note that it is QL data
    targetdir = '/data/apertif/{0}_QL'.format(taskid)
    if os.path.exists(targetdir):
        pass
    else:
        os.makedirs(targetdir)
    #now go through and get data for each beam
    for b,cid in zip(beams,cal_tids):
        #make data location
        beamdir = '{0}/{1:0>2}'.format(targetdir,b)
        if os.path.exists(beamdir):
            pass
        else:
            os.makedirs(beamdir)
        #get the target data
        targetdate = taskid[0:6]
        targetscan = taskid[-3:]
        try:
            getdata_alta(int(targetdate),int(targetscan),int(b),targetdir=beamdir)
        except RuntimeError:
            print "Data download for target 'failed' for beam {}".format(b)
        #get source name and rename file
        msfile = "{0}/WSRTA{1}_B{2:0>3}.MS".format(beamdir,taskid,b)
        t_field = pt.table(msfile+"::FIELD", readonly=True)
        target_name = t_field[0]['NAME']
        newfile = "{0}/{1}.MS".format(beamdir,target_name)
        os.rename(msfile,newfile)
        #do the same for calibrator
        #have to split calibrator name
        caldate = cid[0:6]
        calscan = cid[-3:]
        try:
            getdata_alta(int(caldate),int(calscan),int(b),targetdir=beamdir)
        except RuntimeError:
            print "Data download for calibrator 'failed' for beam {}".format(b)
        #strip cal name and update in MS
        msfile = "{0}/WSRTA{1}_B{2:0>3}.MS".format(beamdir,cid,b)
        t_field = pt.table(msfile+"::FIELD", readonly=False)
        cal_string = t_field[0]['NAME']
        name_split = cal_string.split('_')
        calname = name_split[0]
        t_field.putcell("NAME",0,calname)
        print "Tried to update calibrator name"
        t_field.flush()
        print "Clear MS file for update"
        newfile = "{0}/{1}.MS".format(beamdir,calname)
        os.rename(msfile,newfile)
