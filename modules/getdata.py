from apercal.subs.getdata_alta import getdata_alta

#get scan numbers for calibrators
def get_cal_scans(taskid,beams):
    #take taskID number of target
    #Plus arrays of beams
    #Look for earlier observations of calibrator
    #assume ingest happens in order
    #Don't care about calibrator type, will only do flux cal
    #Assume previous observation is 39th cal scan, or target
    #If target, scan before is 39th cal scan
