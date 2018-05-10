import matplotlib
# Force matplotlib to not use any Xwindows backend.
#matplotlib.use('Agg')
import matplotlib.pyplot as plt
from itertools import *
#from time_to_sec import time_to_sec
import sys
import os
import numpy as np
import datetime
#from optics import Optics
#from optics import Point
from sklearn.cluster import DBSCAN
import csv
#My function
import sys
sys.path.append("../query_script")
from Find_peak_point import Find_peak
from Wavelet import wave
from Identify import Identify_IO_App

def plot(IOBW_r, IOBW_w):
    final_result_r = Identify_IO_App(IOBW_r)
    final_result_w = Identify_IO_App(IOBW_w)

    print final_result_r
    print final_result_w
    
    x_r = []
    x_w = []

    for item in final_result_r:
        x_r.append(int(item[5]))

    for item in final_result_w:
        x_w.append(int(item[5]))

    return x_r, x_w

def plot_time(IOBW_r, IOBW_w):
    print IOBW_r
    print IOBW_w
    
    final_result_r = Identify_IO_App(IOBW_r)
    final_result_w = Identify_IO_App(IOBW_w)

    print final_result_r
    print final_result_w

    
    x_r = []
    x_w = []

    time_start_r = []
    time_start_w = []

    time_end_r = []
    time_end_w = []
#    print final_result_r
#    print final_result_w
    for item in final_result_r:
        time_start_r.append(int(item[3]))
        time_end_r.append(int(item[4]))
        x_r.append(int(item[5]))

    for item in final_result_w:
        time_start_w.append(int(item[3]))
        time_end_w.append(int(item[4]))
        x_w.append(int(item[5]))

    return time_start_r, time_end_r, time_start_w, time_end_w
 
def plot_all(IOBW_r, IOBW_w):
    final_result_r = Identify_IO_App(IOBW_r)
    final_result_w = Identify_IO_App(IOBW_w)

#    print final_result_r
#    print final_result_w

    
    time_r = []
    time_w = []

    time_start_r = []
    time_start_w = []

    time_end_r = []
    time_end_w = []
    
    total_volumn_r = []
    total_volumn_w = []
    
    for item in final_result_r:
	total_volumn_r.append(int(item[2]))
        time_start_r.append(int(item[3]))
        time_end_r.append(int(item[4]))
        time_r.append(int(item[5]))

    for item in final_result_w:
	total_volumn_w.append(int(item[2]))
        time_start_w.append(int(item[3]))
        time_end_w.append(int(item[4]))
        time_w.append(int(item[5]))

    return time_start_r, time_end_r, time_start_w, time_end_w, \
    total_volumn_r, total_volumn_w, time_r, time_w
    
if __name__=="__main__":
    plot("/root/yb/yb/yangbin_work/date/8645345_IOBANDWIDTH.csv")
	#pathdir='data/'
	#pathlist=os.listdir(pathdir)
	#for path in pathlist:
	#	st="2017-05-02 16:00:00"
	#	plot(pathdir+path,st)





