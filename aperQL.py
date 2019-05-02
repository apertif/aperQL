# aperQL: Quicklook pipeline for Apertif data
# E. A. K. Adams

__author__ = "E.A.K. Adams"


"""
Script to run components of a quick look pipeline
Takes a taskid of target field, plus taskids for start/end
of calibrator scan.
Future development would have the calibrator scans found
automatically.
Usage: 
python aperQL <taskID> <calstartid> <calendid> -N 10
"""

import argparse
import numpy as np
from modules.functions import *

#Argument parsing
parser = argparse.ArgumentParser(description='Run Quick Look pipeline')
parser.add_argument("taskid",help='Task ID of target field',type=str)
parser.add_argument("calstart",help='Task ID of start of calibrator scan',type=str)
parser.add_argument("calend",help='Taks ID of end of calibrator scan',type=str)
parser.add_argument('-N','--number',default=2,type=int,
                    help='Number of random beams')
args = parser.parse_args()


#get random set of beams
beams = np.random.randint(0,40,args.number)

print beams

#get calibrator taskIDs matched to beams
cal_tids = get_cal_tids(args.calstart,args.calend,beams)

print cal_tids

#get data
#Structure is same as apercal

