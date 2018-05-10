#coding:utf-8
import json
#from pyes import *
#from pyes.aggs import TermsAgg
#from pyes.queryset import  QuerySet
#import matplotlib.pyplot as plt
#import matplotlib.patches as patches
import numpy as np
import sys
sys.path.insert(0,"../../job")
sys.path.append("../query_script")
from job_ip_all import get_re_jobid_CNC_runtime_corehour as get_re_jobid
#sys.path.append("../ELK")
from time_to_sec import time_to_sec_fast
import csv
import exceptions
import es_search
import threading 
import datetime
from deal_generator import deal_all_message
import gc
from Modeling_tyzhang import plot_all
from classify_IOmode import classify_single

node_count = 50
IOBW_affective = 0.1

length = 5000
#conn=ES('20.0.8.20::9200')

def draw_2d(x, y):

    plt.xticks(fontsize=20)
    plt.yticks(fontsize=20)
    plt.plot(x, 'r.')
    plt.plot(y, 'g.')
    #plt.plot([0, 35], [average, average], 'r-')
    #plt.xlabel('Runtime (day)', fontsize = 20)
    #plt.ylabel('IO_Time/Runtime ', fontsize = 20)
    plt.xlabel('index', fontsize = 20)
    #plt.ylabel('IOPS ', fontsize = 20)
    plt.ylabel('', fontsize = 20)
    label = ["read", "write"]
    plt.legend(label, loc = 1, ncol = 1)
    plt.show()








def save_all(jobID, resultr_band, resultw_band, resultr_iops, resultw_iops, resultr_open, resultw_close, pe_r, pe_w, file_open, title):
    f=file('../../results_job_data/collect_data/'+ title+'/all.csv','ab')
    for i in range(len(resultr_band)):
        if(abs(resultr_band[i]) > sys.float_info.epsilon or \
        abs(resultr_band[i]) > sys.float_info.epsilon or \
        abs(resultr_iops[i]) > sys.float_info.epsilon or \
        abs(resultw_iops[i]) > sys.float_info.epsilon or \
        abs(resultr_open[i]) > sys.float_info.epsilon or \
        abs(resultw_close[i]) > sys.float_info.epsilon or \
        abs(pe_r[i]) > sys.float_info.epsilon or \
        abs(pe_w[i]) > sys.float_info.epsilon or \
        abs(len(file_open[i])) > 0):
            write_row = "%s : %d : %f %f %d %d %d %d %d %d %d\n"\
            %(jobID, i, resultr_band[i], resultw_band[i], resultr_iops[i], \
            resultw_iops[i], resultr_open[i], resultw_close[i], pe_r[i], pe_w[i], \
            len(file_open[i]))
            f.write(write_row)
        else:
            continue

def compute_month_day(month):
    if(month == 1 or month == 3 or month == 5 or month == 7 or month == 8 or\
    month == 10 or month == 12):
        month_day = 31
    elif(month == 4 or month == 6 or month == 9 or month == 11):
        month_day = 30
    else:
        month_day = 28
    return month_day

def compute_index(starttime, endtime):
    
    time_start = starttime[0:4] + "." + starttime[5:7] + "." + starttime[8:10]
    time_end = endtime[0:4] + "." + endtime[5:7] + "." + endtime[8:10]

    start_month = int(starttime[5:7]) 
    end_month = int(endtime[5:7]) 
    
    start_day = int(starttime[8:10])
    end_day = int(endtime[8:10])

    month_count = end_month - start_month
    day_count = end_day - start_day
    index = []
    if (month_count == 0):
        for i in xrange(day_count+1):
            if((start_day + i) < 10):
                index.append(starttime[0:4] + "." + starttime[5:7] + ".0" + str(start_day+i))
            else:
                index.append(starttime[0:4] + "." + starttime[5:7] + "." + str(start_day+i))
    elif(month_count == 1):
        start_month_day = compute_month_day(start_month)
        for month_i in xrange(start_day, start_month_day + 1):
            if(month_i < 10):
                index.append(starttime[0:4] + "." + starttime[5:7] + ".0" + str(month_i))
            else:
                index.append(starttime[0:4] + "." + starttime[5:7] + "." + str(month_i))

        for month_j in xrange(1, end_day + 1):
            if(month_j < 10):
                index.append(endtime[0:4] + "." + endtime[5:7] + ".0" + str(month_j))
            else:
                index.append(endtime[0:4] + "." + endtime[5:7] + "." + str(month_j))

    return index

def process(dic,pa,min_time,max_time):
    host=dict()
    con=dict()
    count=0
    for i in range(len(dic)):
        if not  (host.has_key((dic[i][0],dic[i][1]))):
            host[(dic[i][0],dic[i][1])]=dic[i][2]/1024.0
        else:
            host[(dic[i][0],dic[i][1])]=host[(dic[i][0],dic[i][1])]+dic[i][2]/1024.0
    re=[0.0 for i in range(max_time-min_time+5)]
    for key,value in host.items():
        if value >50:
            re[key[1]]+=1
    return re

def compute_pre_with_jobid(jobid):
    #    print time11, time12, jobid
    UTC=datetime.timedelta(hours=8) 
    resu=[]
    resu=get_re_jobid(jobid)
    for val in resu:
        ti=val[0]+" "+val[1]+" "+val[2]
        program_name = val[2]
        CNC = int(val[5])
        run_time = int(val[6])
        corehour = float(val[7])
        time21=val[3]
        time22=val[4]
        node=val[8]
        t21=datetime.datetime.strptime(time21,'%Y-%m-%d %H:%M:%S') 
        t22=datetime.datetime.strptime(time22,'%Y-%m-%d %H:%M:%S') 
        min_time=time_to_sec_fast(time21)
        max_time=time_to_sec_fast(time22)
        tt1=str(t21-UTC)
        tt2=str(t22-UTC)
        time1=tt1[:10]+"T"+tt1[11:]+".000Z"
        time2=tt2[:10]+"T"+tt2[11:]+".000Z"
        iplist=[]
        for no in node:
            a=no.split('-')
            try:
                int(a[0])
            except Exception:
                print a[0]
                print 'null node!!!'
                return
            if len(a)>1:
    #            print a[0],a[1]
                for x in range(int(a[0]),int(a[1])+1):
                    w2=x//1024
                    w3=(x-w2*1024)//8
                    w4=x-w2*1024-w3*8+1
                    ip="172."+str(w2)+"."+str(w3)+"."+str(w4)
                    iplist.append(ip)
            elif len(a)==1:
                    #print a[0]
                w2=int(a[0])//1024
                w3=(int(a[0])-w2*1024)//8
                w4=int(a[0])-w2*1024-w3*8+1
                ip="172."+str(w2)+"."+str(w3)+"."+str(w4)
                iplist.append(ip)
    return jobid, CNC ,run_time, corehour, time1, time2, iplist, min_time, max_time

    
def compute_IOphase_mode(resultr_band, resultw_band, resultr_iops, resultw_iops, \
resultr_open, resultw_close, resultr_size, resultw_size, pe_r, pe_w, file_all_count):
    
    time_r_start, time_r_end, time_w_start, time_w_end, total_volumn_r, total_volumn_w, \
    time_r, time_w = plot_all(resultr_band, resultw_band)

    IO_mode_r = []
    IO_mode_w = []
    
    for i in xrange(len(time_r_start)):
        IOBW = max(resultr_band[time_r_start[i]:time_r_end[i]])
        PE = max(pe_r[time_r_start[i]:time_r_end[i]])
        IOmode = classify_single(IOBW, PE, file_all_count)
        IO_mode_r.append(IOmode)
    
    for i in xrange(len(time_w_start)): 
        IOBW = max(resultw_band[time_w_start[i]:time_w_end[i]])
        PE = max(pe_w[time_w_start[i]:time_w_end[i]])
        IOmode = classify_single(IOBW, PE, file_all_count)
        IO_mode_w.append(IOmode)
    
    return IO_mode_r, IO_mode_w, total_volumn_r, total_volumn_w, \
    time_r_start, time_r_end, time_w_start, time_w_end, time_r, time_w
    
def save_IO(jobID, CNC ,run_time, corehour, IO_mode, IO_volumn, \
time_start, time_end, time, file_name):
    f = open(file_name, "ab")
    for i in range(len(IO_mode)):
        write_row = "%s %d %d %f %s %f %d %d %d \n"\
        %(jobID, CNC ,run_time, corehour, IO_mode[i], IO_volumn[i], \
        time_start[i], time_end[i], time[i])
        f.write(write_row)

def save_IOphase_mode(jobID, CNC ,run_time, corehour, resultr_band, resultw_band, \
resultr_iops, resultw_iops, resultr_open, resultw_close, resultr_size, resultw_size, \
pe_r, pe_w, file_all_count, title):
    
    file_name_r = '../../results_job_data/collect_data/'+ title+'/IO_phase_r.csv'
    file_name_w = '../../results_job_data/collect_data/'+ title+'/IO_phase_w.csv'
    IO_mode_r, IO_mode_w, IO_volumn_r, IO_volumn_w, \
    time_r_start, time_r_end, time_w_start, time_w_end, time_r, time_w = \
    compute_IOphase_mode(resultr_band, resultw_band, resultr_iops, resultw_iops, \
    resultr_open, resultw_close, resultr_size, resultw_size, pe_r, pe_w, file_all_count)


    save_IO(jobID, CNC ,run_time, corehour, IO_mode_r, IO_volumn_r, \
    time_r_start, time_r_end, time_r, file_name_r)

    save_IO(jobID, CNC ,run_time, corehour, IO_mode_w, IO_volumn_w, \
    time_w_start, time_w_end, time_w, file_name_w)

def save_main(jobid, title):
#    print jobid
    try:
        jobID, CNC ,run_time, corehour, time1, time2, \
        iplist, min_time, max_time = compute_pre_with_jobid(jobid)
    except Exception as e:
        print e

    results_message = []
    results_host = []
    count_ip = len(iplist)/node_count
    remainder = len(iplist)%node_count
    
    if (time1[8:10] == time2[8:10]):
        index = [time1[0:4] + "." + time1[5:7] + "." + time1[8:10]]
    else:
        index = compute_index(time1, time2)
    host_t = 1 

    try:
        for lnd in xrange(len(index)):
            if(count_ip > 0):
                for c1 in xrange(count_ip):
                    try:
                        results_message_tmp, results_host_tmp \
                        = es_search.search(time1, time2,\
                        iplist[c1*node_count:c1*node_count+node_count], index[lnd], host_t)
                        results_message += results_message_tmp
                        results_host += results_host_tmp
                    except Exception as e:
                        print e
                if(remainder > 0):
                    results_message_tmp, results_host_tmp = \
                    es_search.search(time1, time2,\
                    iplist[count_ip*node_count:count_ip*node_count+remainder], index[lnd], host_t)
            else:
                results_message_tmp, results_host_tmp = es_search.search(time1, time2,\
                iplist, index[lnd], host_t)
    except Exception as e:
        print e
    
    results_message += results_message_tmp
    results_host += results_host_tmp

    try:
        resultr_band,resultw_band,resultr_iops,resultw_iops,resultr_open, resultw_close, \
        resultr_size, resultw_size, dictr, dictw, file_all_count, file_open \
        = deal_all_message(results_message, results_host, min_time, max_time)
        del results_message
        del results_host
        gc.collect()
    except Exception as e:
        print e
    
    pe_r = process(dictr,'r',min_time,max_time)
    pe_w = process(dictw,'b',min_time,max_time)
    
    del dictr  
    del dictw  
    gc.collect()
    
    save_all(jobID, resultr_band, resultw_band, \
    resultr_iops, resultw_iops, resultr_open, resultw_close, pe_r, pe_w, file_open, title)
    
    save_IOphase_mode(jobID, CNC ,run_time, corehour, resultr_band, resultw_band, \
    resultr_iops, resultw_iops, resultr_open, resultw_close, resultr_size, resultw_size, \
    pe_r, pe_w, file_all_count, title)

    del file_open
    del resultr_band
    del resultw_band
    del resultr_iops 
    del resultw_iops
    del resultr_open
    del resultw_close
    del resultr_size
    del resultw_size
    del pe_r
    del pe_w
    gc.collect()
