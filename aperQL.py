# aperQL: Quicklook pipeline for Apertif data
# E. A. K. Adams

__author__ = "E.A.K. Adams"


"""
Script to run components of a quick look pipeline
Takes a scan number of target field
Usage: 
python aperQL <taskID>
"""

import argparse
import numpy as np

#Argument parsing
parser = argparse.ArgumentParser(description='Run Quick Look pipeline')
parser.add_argument("taskid",help='Task ID of target field')
parser.add_argument("calstart",help='Task ID of start of calibrator scan')
parser.add_argument("calend",help='Taks ID of end of calibrator scan')
parser.add_argument('-N','--number',default=2,
                    help='Number of random beams')
args = parser.parse_args()


#get random set of beams
beams = np.random.randit(0,40,args.number)

#get calibrator taskIDs matched to beams


#get data
#Structure is same as apercal

