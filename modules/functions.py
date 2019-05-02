#aperQL: modules for quicklook pipeline for Apertif data
# E.A.K. Adams 02/05/2019

__author__="E.A.K. Adams"

import numpy as np
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
