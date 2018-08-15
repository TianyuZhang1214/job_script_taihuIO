#coding:utf-8
import json
#from pyes import *
#from pyes.aggs import TermsAgg
#from pyes.queryset import  QuerySet
import numpy as np
import sys
import datetime
sys.path.append("../query_script")
from job_ip_all import get_re_jobid as get_re_jobid_all
sys.path.append("../ELK")
from time_to_sec import time_to_sec
import csv
import exceptions 
from time import clock 
import es_search
from savejob_jobid_modified import deal_csv, comput, compute_pre, compute_index, save_main
from deal_generator import deal_all_message

size_ip = 50

big_job_file_name = '/home/export/mount_test/swstorage/results_job_data/draw_csv/big_job_undone.csv'

    
def save_tmp(resultr_band, resultw_band, resultr_iops, resultw_iops, resultr_open, resultw_close, pe_r, pe_w, tmp_file_name):
     f = open(tmp_file_name, "wb")
     for i in range(len(resultr_band)):
	 if(abs(resultr_band[i]) > sys.float_info.epsilon or \
	 abs(resultr_band[i]) > sys.float_info.epsilon or \
	 abs(resultr_iops[i]) > sys.float_info.epsilon or \
	 abs(resultw_iops[i]) > sys.float_info.epsilon or \
	 abs(resultr_open[i]) > sys.float_info.epsilon or \
	 abs(resultw_close[i]) > sys.float_info.epsilon or \
	 abs(pe_r[i]) > sys.float_info.epsilon or \
	 abs(pe_w[i]) > sys.float_info.epsilon):
             write_row = "%d :%f %f %f %f %f %f %f %f \n"\
             %(i, resultr_band[i], resultw_band[i], resultr_iops[i], \
             resultw_iops[i], resultr_open[i], resultw_close[i], pe_r[i], pe_w[i])
             f.write(write_row)
	 else:
	     continue

def read_csv(read_file):
    reader = csv.reader(read_file)
    read_results = []
#    next(reader)
    for line in reader:
        read_results.append(line)
    read_file.close()
    return read_results

def show_IOBANDWIDTH(resultr,resultw,jobid):
    count_resultr = 0 
    count_resultw = 0
    count_resultrw = 0
    sum_resultr = 0
    sum_resultw = 0
    for i in range(len(resultr)):
#        if (abs(resultr[i]) > sys.float_info.epsilon):
        if (resultr[i] > 1):
#            if (resultr[i] < 1.0):
#                print resultr[i]
            count_resultr += 1
            sum_resultr += resultr[i]
#        if (abs(resultw[i]) > sys.float_info.epsilon):
        if (resultw[i] > 1):
            count_resultw += 1
            sum_resultw += resultw[i]
#        if(abs(resultr[i]) > sys.float_info.epsilon or \
#        abs(resultw[i]) > sys.float_info.epsilon):
        if(abs(resultr[i]) > 1.0 or \
        abs(resultw[i]) > 1.0 ):
#            print i
            count_resultrw += 1
    average_resultr = 0.0
    average_resultw = 0.0
    print ("count_resultr  = %f "%(count_resultr))
    print ("count_resultw  = %f "%(count_resultw))
    print ("count_resultrw = %f "%(count_resultrw))
    print ("sum_resultr  = %f MB "%(sum_resultr))
    print ("sum_resultw  = %f MB "%(sum_resultw))
    if(sum_resultr > sys.float_info.epsilon and count_resultr > sys.float_info.epsilon):
        average_resultr = sum_resultr/count_resultr 
        print ("average_IOBW_resultr = %f MB/s"%(average_resultr))
    if(sum_resultw > sys.float_info.epsilon and count_resultw > sys.float_info.epsilon):
        average_resultw = sum_resultw/count_resultw
        print ("average_IOBW_resultw = %f MB/s"%(average_resultw))
    sum_resultrw = sum_resultr + sum_resultw 
    if(sum_resultrw > sys.float_info.epsilon and count_resultrw > sys.float_info.epsilon):
        average_resultrw = sum_resultrw/count_resultrw
        print ("average_IOBW_resultrw = %f MB/s"%(average_resultrw))

def show_IOPS(IOPS_r, IOPS_w, IOBW_r, IOBW_w, jobid):
#Calculate the sum IOPS .
    count_IOPS_r = 0
    count_IOPS_w = 0
    count_IOPS_rw = 0
    count_IOPS_rw_all = 0
    sum_IOPS_r = 0
    sum_IOPS_w = 0
    for i in range(len(IOPS_r)):
        if (abs(IOPS_r[i]) > sys.float_info.epsilon):
#        if (abs(IOPS_r[i]) > sys.float_info.epsilon \
#            and IOBW_r[i] > 1.0):
            count_IOPS_r += 1
            sum_IOPS_r += IOPS_r[i]
        if (abs(IOPS_w[i]) > sys.float_info.epsilon):
#        if (abs(IOPS_w[i]) > sys.float_info.epsilon \
#            and IOBW_w[i] > 1.0):
            count_IOPS_w += 1
            sum_IOPS_w += IOPS_w[i]
        if ((abs(IOPS_r[i]) > sys.float_info.epsilon and IOBW_r[i] > 1.0) or \
        (abs(IOPS_w[i]) > sys.float_info.epsilon and IOBW_w[i] > 1.0) ):
            count_IOPS_rw += 1
#        if (IOBW_r[i] > 1.0 or \
#        IOBW_w[i] > 1.0 or \
#        abs(IOPS_r[i]) > sys.float_info.epsilon or \
#        abs(IOPS_w[i]) > sys.float_info.epsilon):
#        if (abs(IOBW_r[i]) > sys.float_info.epsilon or \
#        abs(IOBW_w[i]) > sys.float_info.epsilon or \
#        abs(IOPS_r[i]) > sys.float_info.epsilon or \
#        abs(IOPS_w[i]) > sys.float_info.epsilon):
            count_IOPS_rw_all += 1
    print ("count_IOPS_r = %f "%(count_IOPS_r))
    print ("sum_IOPS_r = %f "%(sum_IOPS_r))
    print ("count_IOPS_w = %f "%(count_IOPS_w))
    print ("sum_IOPS_w = %f "%(sum_IOPS_w))
    average_IOPS_r = 0
    average_IOPS_w = 0
    if(sum_IOPS_r > sys.float_info.epsilon and count_IOPS_r > sys.float_info.epsilon):
        average_IOPS_r = sum_IOPS_r/count_IOPS_r
        print ("average_IOPS_r = %f "%(average_IOPS_r))
    if(sum_IOPS_w > sys.float_info.epsilon and count_IOPS_r > sys.float_info.epsilon):
        average_IOPS_w = sum_IOPS_w/count_IOPS_w
        print ("average_IOPS_w = %f "%(average_IOPS_w))


def show_MDS(MDS_o, MDS_c, jobid):
#Calculate the sum MDS .
    count_MDS_o = 0
    count_MDS_c = 0
    count_MDS_oc = 0
    sum_MDS_o = 0
    sum_MDS_c = 0
    for i in range(len(MDS_o)):
        if (abs(MDS_o[i]) > sys.float_info.epsilon):
            count_MDS_o += 1
            sum_MDS_o += MDS_o[i]
        if (abs(MDS_c[i]) > sys.float_info.epsilon):
            count_MDS_c += 1
            sum_MDS_c += MDS_c[i]
        if (abs(MDS_o[i]) > sys.float_info.epsilon or \
        abs(MDS_c[i]) > sys.float_info.epsilon ):
            count_MDS_oc += 1

    print ("count_MDS_o = %f "%(count_MDS_o))
    print ("sum_MDS_o = %f "%(sum_MDS_o))
    print ("count_MDS_c = %f "%(count_MDS_c))
    print ("sum_MDS_c = %f "%(sum_MDS_c))
    average_MDS_o = 0
    average_MDS_c = 0
    if(sum_MDS_o > sys.float_info.epsilon and count_MDS_o > sys.float_info.epsilon):
        average_MDS_o = sum_MDS_o/count_MDS_o
        print ("average_MDS_o = %f "%(average_MDS_o))
    if(sum_MDS_c > sys.float_info.epsilon and count_MDS_c > sys.float_info.epsilon):
        average_MDS_c = sum_MDS_c/count_MDS_c
        print ("average_MDS_c = %f "%(average_MDS_c))


def show_process(dic,pa,min_time,max_time,jobid):
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
    count = 0
#    for i in xrange(len(re)):
#        if(re[i] > 0):
#            count += 1
#            print pa, " PE: ", re[i]
    
    max_PE = max(re)
    ave_PE = 0.0
    if(count > 0):
        ave_PE = sum(re)/count
    print pa, "max_PE: ", max_PE
    print pa, "ave_PE: ", ave_PE

    return re

def max_PE(PE_r, PE_w, jobid):
    if(len(PE_r) >1 ): 
        PE_r_max = max(PE_r)
    else:
        PE_r_max = 0
    if(len(PE_w) >1 ): 
        PE_w_max = max(PE_w)
    else:
        PE_w_max = 0

#def test_IOmode(resultr_band,resultw_band,resultr_iops,resultw_iops,resultr_open,\
#resultw_close, resultr_size, resultw_size, dictr, dictw, min_time, max_time):
#    title_IOmode = "collect_data/test"
#
#    IOBW_file_IO = file(title_IO+'/IOBW.csv','wb')
#    writer_IOBW_IO = csv.writer(IOBW_file_IO)
#    writer_IOBW_IO.writerow(['IOBW_r', 'IOBW_w'])
#
#    PER_file_IO = file(title_IO+'/PER.csv','wb')
#    writer_PER_IO = csv.writer(PER_file_IO)
#    writer_PER_IO.writerow(['max_PE_r'])
#
#    PEW_file_IO = file(title_IO+'/PEW.csv','wb')
#    writer_PEW_IO = csv.writer(PEW_file_IO)
#    writer_PEW_IO.writerow(['max_PE_w'])
#
#    MDS_file_IO = file(title_IO+'/MDS.csv','wb')
#    writer_MDS_IO = csv.writer(MDS_file_IO)
#    writer_MDS_IO.writerow(['MDS_r', 'MDS_w'])
# 
#    saveIOBANDWIDTH(resultr_band, resultw_band, title_IOmode, jobid, \
#    program_name, corehour, min_time)
#    saveMDS(resultr_open,resultw_close,title_IOmode,jobid, \
#    program_name, corehour, min_time)
#    saveprocess(dictr,'r',title_IOmode,min_time,max_time,jobid, \
#    program_name, corehour)
#    saveprocess(dictw,'b',title_IOmode,min_time,max_time,jobid, \
#    program_name, corehour)


def test_save_main(starttime, endtime, jobid):
    
    title = 'test'
    title_IOmode = 'test_IO'
    title_IO = 'test_IO'
    
    IOBW_file = file('/home/export/mount_test/swstorage/results_job_data/collect_data/'+ title+'/IOBW.csv','wb')
    writer_IOBW = csv.writer(IOBW_file)
    writer_IOBW.writerow(['program_name', 'jobID', 'start_time', 'end_time', \
    'sum_READ', 'sum_WRITE', 'count_r', 'count_w', 'count_rw', \
    'average_r', 'average_w'])

    IOPS_file = file('/home/export/mount_test/swstorage/results_job_data/collect_data/'+ title+'/IOPS.csv','wb')
    writer_IOPS = csv.writer(IOPS_file)
    writer_IOPS.writerow(['program_name', 'jobID', 'start_time', 'end_time', \
    'sum_READ', 'sum_WRITE', 'count_r', 'count_w', 'count_rw', 'count_rw_all', \
    'average_r', 'average_w'])

    maxPE_file = file('/home/export/mount_test/swstorage/results_job_data/collect_data/'+ title+'/maxPE.csv','wb')
    writer_maxPE = csv.writer(maxPE_file)
    writer_maxPE.writerow(['program_name', 'jobID', 'start_time', 'end_time', \
    'max_PE_r', 'max_PE_w'])

    MDS_file = file('/home/export/mount_test/swstorage/results_job_data/collect_data/'+ title+'/MDS.csv','wb')
    writer_MDS = csv.writer(MDS_file)
    writer_MDS.writerow(['program_name', 'jobID', 'start_time', 'end_time', \
    'sum_OPEN', 'sum_CLOSE', 'count_o', 'count_c', 'count_oc', \
    'average_o', 'average_c'])

    size_r_file=file('/home/export/mount_test/swstorage/results_job_data/collect_data/'+ title +'/SIZE_r.csv','wb')
    writer_r=csv.writer(size_r_file)
    writer_r.writerow(['Read_size','count','total_size'])

    size_w_file=file('/home/export/mount_test/swstorage/results_job_data/collect_data/'+ title +'/SIZE_w.csv','wb')
    writer_w=csv.writer(size_w_file)
    writer_w.writerow(['Write_size','count','total_size'])

    IOBW_file_IO = file('/home/export/mount_test/swstorage/results_job_data/collect_data/'+ title_IO+'/IOBW.csv','wb')
    writer_IOBW_IO = csv.writer(IOBW_file_IO)
    writer_IOBW_IO.writerow(['IOBW_r', 'IOBW_w'])

    PER_file_IO = file('/home/export/mount_test/swstorage/results_job_data/collect_data/'+ title_IO+'/PER.csv','wb')
    writer_PER_IO = csv.writer(PER_file_IO)
    writer_PER_IO.writerow(['max_PE_r'])

    PEW_file_IO = file('/home/export/mount_test/swstorage/results_job_data/collect_data/'+ title_IO+'/PEW.csv','wb')
    writer_PEW_IO = csv.writer(PEW_file_IO)
    writer_PEW_IO.writerow(['max_PE_w'])

    MDS_file_IO = file('/home/export/mount_test/swstorage/results_job_data/collect_data/'+ title_IO+'/MDS.csv','wb')
    writer_MDS_IO = csv.writer(MDS_file_IO)
    writer_MDS_IO.writerow(['MDS_r', 'MDS_w'])
   
    file_name_file_IO = file('/home/export/mount_test/swstorage/results_job_data/collect_data/'+ title_IO+'/file_name.csv','wb')
    writer_file_name_IO = csv.writer(file_name_file_IO)
    writer_file_name_IO.writerow(['file_name_set', 'file_count'])
    
    jobid_abnormal_file = file('/home/export/mount_test/swstorage/results_job_data/collect_data/'+ title+'/jobid_abnormal.csv','wb')
    writer_jobid_abnormal = csv.writer(jobid_abnormal_file)
    try:
    	save_main(starttime, endtime, jobid, 'cc', 1111, \
    	title, title_IOmode, 1)
    except Exception as e:
	print e

    sys.exit()

def split_time(time1, time2, index):
    start_time = [(['00:00:00.000Z'] * 4) for i in range(len(index) - 2)] 
    end_time = [(['00:00:00.000Z'] * 4) for i in range(len(index) - 2)] 
    
    for i in xrange(1,len(index)-1):
	x = (index[i]).split('.')
	start_day = x[0] + '-'+ x[1] + '-' + x[2] + 'T'
	start_time[i-1][0] = start_day + '00:00:00.000Z'			
	end_time[i-1][0] = start_day + '05:59:59.000Z'			
	start_time[i-1][1] = start_day + '06:00:00.000Z'			
	end_time[i-1][1] = start_day + '11:59:59.000Z'			
	start_time[i-1][2] = start_day + '12:00:00.000Z'			
	end_time[i-1][2] = start_day + '17:59:59.000Z'			
	start_time[i-1][3] = start_day + '18:00:00.000Z'			
	end_time[i-1][3] = start_day + '23:59:59.000Z'			

    print start_time    
    print end_time    
    return start_time, end_time

def show_job_all(start_time, end_time, jobid):
    try:
        time1, time2, iplist, min_time, max_time \
        = compute_pre(start_time, end_time, jobid)
    except Exception as e:
        print e

    print time1    
    print time2    
    print min_time    
    print max_time

#    index = [time1[0:4] + "." + time1[5:7] + "." + time1[8:10]]
    
    if (time1[8:10] == time2[8:10]):
        index = [time1[0:4] + "." + time1[5:7] + "." + time1[8:10]]
    else:
        index = compute_index(time1, time2) 

    split_time_start, split_time_end = split_time(time1, time2, index)
    
    print "index: ", index
    count_ip = len(iplist)/size_ip
    remainder = len(iplist)%size_ip
    results_message = [] 
    results_host = []
    print "count_ip: ", count_ip
    print "remainder: ", remainder
#    print len(index)
    
    try:
        for lnd in xrange(len(index)):
	    print "Now search index: ", index[lnd]
            if (count_ip > 0):
                for c1 in xrange(count_ip):
	    	    print "Now search ip iteration: ", c1
		    try:
                    	results_message_tmp, results_host_tmp = es_search.search(time1, time2,\
                    	iplist[c1*size_ip:c1*size_ip+size_ip], index[lnd], 10)
                    	results_message += results_message_tmp
                    	results_host += results_host_tmp
		    except Exception as e:
			print e
                if(remainder > 0):    
                    results_message_tmp, results_host_tmp = es_search.search(time1, time2,\
                    iplist[count_ip*size_ip:count_ip*size_ip + remainder], index[lnd], 1)
            else:
                results_message_tmp, results_host_tmp = es_search.search(time1, time2,\
                iplist, index[lnd], 1)
    except Exception as e:
	print e
    
    results_message += results_message_tmp
    results_host += results_host_tmp
    print len(results_message)
    
    resultr_band,resultw_band,resultr_iops,resultw_iops,resultr_open,\
    resultw_close, resultr_size, resultw_size, dictr, dictw, file_count, file_open\
    = deal_all_message(results_message, results_host, min_time, max_time)

    print 'file_count: ', file_count
    show_IOBANDWIDTH(resultr_band,resultw_band,jobid)
    show_IOPS(resultr_iops,resultw_iops,resultr_band,resultw_band,jobid)
    show_MDS(resultr_open,resultw_close,jobid)
    pe_r = show_process(dictr,'r',min_time,max_time,jobid)
    pe_w = show_process(dictw,'b',min_time,max_time,jobid)

    tmp_file_name = '/home/export/mount_test/swstorage/results_job_data/tmp_csv/' + jobid + '.csv'
    print tmp_file_name 
    save_tmp(resultr_band, resultw_band, resultr_iops, resultw_iops, resultr_open, resultw_close, pe_r, pe_w, tmp_file_name)
    
    draw_2d(resultr_band, pe_r)
    draw_2d(resultw_band, pe_w)

def read_big_job():
    jobID = []
    starttime = []
    endtime = []

    f = open(big_job_file_name, 'r')
    for line in open(big_job_file_name):
        line = f.readline()
        array = line.split()
        x = str(array[0])
        y = str(array[1]) + ' ' + str(array[2])
        z = str(array[3]) + ' ' + str(array[4])
        jobID.append(x)
        starttime.append(y)
        endtime.append(z)

    return jobID, starttime, endtime

if __name__=="__main__":
    if (len(sys.argv)<3):
        print "please input time1 to time2 and jobid and select_tag \
        e.g:2016-07-19 12:30:00 2016-07-19 14:30:00 6100000"
        sys.exit()
    start_time0 = clock()
    time11=sys.argv[1]+" "+sys.argv[2]
    time12=sys.argv[3]+" "+sys.argv[4]
    jobid=sys.argv[5]
#    test_save_main(time11, time12, jobid)
    
    print time11, time12, jobid
    show_job_all(time11, time12, jobid)
