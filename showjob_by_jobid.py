#coding:utf-8
import json
import numpy as np
import sys
import datetime
import scipy.io as sio
from optparse import OptionParser
import csv
import os
import exceptions 
from time import clock 
import socket

sys.path.append("../query_script")
from job_ip_all import get_re_jobid as get_re_jobid_all
from time_to_sec import time_to_sec
import es_search
from savejob_jobid_modified import deal_csv, comput, compute_pre, compute_index, save_main
from deal_generator import deal_all_message
from es_search_ost import search_interval as search_interval_ost
from es_search_glb import search as search_interval_glb
from time_to_sec import time_to_sec_fast
from check_data import save_fd_info
from show_job_info import show_IOBANDWIDTH, show_IOPS, show_MDS, show_process, max_PE, show_ost_data
from save_job_info import save_trace, save_tmp, save_front_bw, save_back_bw, save_back_bw_agg, save_fwd_bw
from cal_ost_data import search_ost, get_ost_list, get_ost_data
from cal_fwd_data import get_fwd_data
from draw import draw_2d, draw_d

size_ip = 50

fwd_map_1 = [81, 82, 105, 85, 87, 88, 89, 90, 93, 136, 97, 99, 101, 103, 91, 92, 130, \
131, 140, 133, 134, 135, 96, 63, 100, 102, 128, 129, 104, 137, 56, 86, 83, 95, 30, \
127, 141, 142, 143, 144, 107, 108, 109, 110, 111, 112, 113, 55, 121, 116, 117, 118, \
119, 120, 122, 94, 124, 125, 138, 139, 23, 24, 25, 26, 27, 28, 29, 44, 31, 45, 33, 34, \
35, 36, 37, 38, 39, 40, 41, 42, 126, 20, 43]

fwd_map_2 = [81, 82, 105, 85, 87, 88, 89, 90, 93, 136, 97, 99, 101, 103, 91, 92, 130, \
131, 140, 133, 134, 135, 96, 63, 100, 102, 128, 129, 104, 137, 56, 86, 83, 95, 30, \
127, 141, 142, 143, 144, 107, 108, 109, 110, 111, 112, 113, 55, 121, 116, 117, 118, \
119, 120, 122, 94, 124, 125, 138, 139, 23, 24, 25, 26, 27, 28, 29, 44, 31, 45, 33, 34, \
35, 36, 37, 38, 39, 40, 41, 42, 126, 20, 43]

s_time = 'NULL'
e_time = 'NULL'


parser = OptionParser()

parser.add_option("-t", "--trace", default = False, action = "store_true", \
help = "Save the trace ", dest = "trace")

parser.add_option("-f", "--fd_info", default = False, action = "store_true", \
help = "Show the fd info", dest = "fd_info")

parser.add_option("-o", "--ost_info", default = False, action = "store_true", \
help = "Show the ost info", dest = "ost_info")

parser.add_option("-d", "--draw", default = False, action = "store_true",\
help = "Draw the figures", dest = "draw")

parser.add_option("-w", "--fwd_info", default = False, action = "store_true", \
help = "Show the forwarding info", dest = "fwd_info")

(options, args) = parser.parse_args()


def ost_bw_front(fd_info, file_ost_map, length):
    bandr = [([0]*(length+5)) for i in range(440)]
    bandw = [([0]*(length+5)) for i in range(440)]

    for fd in fd_info:
        try:
            if(len(fd_info[fd]['file_name']) > 2):
                file_name = fd_info[fd]['file_name']
#                print list(file_ost_map[file_name])[0]
                fd_info[fd]['ost'] = int(list(file_ost_map[file_name])[0])
            else:
                continue
        except Exception as e:
            continue

    for fd in fd_info:
        if fd_info[fd].has_key('ost'):
            ostid = fd_info[fd]['ost']
            for index in fd_info[fd]['time']:
                bandr[ostid][index] += fd_info[fd]['time'][index]['read']
                bandw[ostid][index] += fd_info[fd]['time'][index]['write']
        else:
            continue

    return bandr, bandw

def read_csv(read_file):
    reader = csv.reader(read_file)
    read_results = []
#    next(reader)
    for line in reader:
        read_results.append(line)
    read_file.close()
    return read_results

def compute_pre_with_jobid(jobid):
    UTC=datetime.timedelta(hours=8) 
    resu = []
    resu = get_re_jobid_all(jobid)
    print resu
    for val in resu:
        ti=val[0]+" "+val[1]+" "+val[2]
        time21=val[3]
        time22=val[4]

        if s_time != 'NULL':
            time21 = s_time
        if e_time != 'NULL':
            time22 = e_time

        node=val[8]
        iplist=[]
        fwd_list = dict()
        for no in node:
            a=no.split('-')
            try:
                int(a[0])
            except Exception:
                print a[0]
                print 'null node!!!'
                return
            if len(a)>1:
                print a[0],a[1]
                for x in range(int(a[0]),int(a[1])+1):
                    w2=x//1024
                    w3=(x-w2*1024)//8
                    w4=x-w2*1024-w3*8+1
                    ip="172."+str(w2)+"."+str(w3)+"."+str(w4)
                    iplist.append(ip)

                    fwd_no = x // 512
                    if(fwd_no in fwd_list):
                        fwd_list[fwd_no].add(ip)
                    else:
                        fwd_list[fwd_no] = set([ip])
            elif len(a)==1:
                    #print a[0]
                w2=int(a[0])//1024
                w3=(int(a[0])-w2*1024)//8
                w4=int(a[0])-w2*1024-w3*8+1
                ip="172."+str(w2)+"."+str(w3)+"."+str(w4)
                iplist.append(ip)

                fwd_no = int(a[0]) // 512
                if(fwd_no in fwd_list):
                    fwd_list[fwd_no].add(ip)
                else:
                    fwd_list[fwd_no] = set([ip])
        try:
            t21 = datetime.datetime.strptime(time21,'%Y-%m-%d %H:%M:%S') 
            t22 = datetime.datetime.strptime(time22,'%Y-%m-%d %H:%M:%S')
        except Exception as e:
            print e
            print 'Time format is incorrect. \
            Job information is not updated in job database.'
            sys.exit()

        min_time = time_to_sec_fast(time21)
        max_time = time_to_sec_fast(time22)
        tt1 = str(t21-UTC)
        tt2 = str(t22-UTC)
        time1 = tt1[:10]+"T"+tt1[11:]+".000Z"
        time2 = tt2[:10]+"T"+tt2[11:]+".000Z"

    return time1, time2, iplist, min_time, max_time, fwd_list

def show_job_all(jobid):
    try:
        time1, time2, iplist, min_time, max_time, fwd_list \
        = compute_pre_with_jobid(jobid)
    except Exception as e:
        print e
    try:
        print time1    
        print time2    
        print min_time    
        print max_time
    except Exception as e:
        print 'Start_time or end_time are not updated.Please wait for job complete or give explicit time format'
        sys.exit()

    if (time1[8:10] == time2[8:10]):
        index = [time1[0:4] + "." + time1[5:7] + "." + time1[8:10]]
    else:
        index = compute_index(time1, time2) 

    
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
        results_message_tmp = ""
        results_host_tmp = ""
        print "Warning, The ES index may be missing!!!!!"
    
    results_message += results_message_tmp
    results_host += results_host_tmp
    print 'message length: ', len(results_message)
    
    resultr_band,resultw_band,resultr_iops,resultw_iops,resultr_open,\
    resultw_close, resultr_size, resultw_size, dictr, dictw, file_count, \
    file_open, fd_info, fwd_file_map \
    = deal_all_message(results_message, results_host, min_time, max_time)

    print 'file_count: ', file_count

    show_IOBANDWIDTH(resultr_band,resultw_band,jobid)
    show_IOPS(resultr_iops,resultw_iops,resultr_band,resultw_band,jobid)
    show_MDS(resultr_open,resultw_close,jobid)
    pe_r = show_process(dictr,'r',min_time,max_time,jobid)
    pe_w = show_process(dictw,'b',min_time,max_time,jobid)
    file_open_count = [len(file_open[i]) for i in range(len(file_open))]

    if options.draw == True:
        if(not os.path.exists('../../results_job_data/job_trace/' + jobid)):
            os.mkdir('../../results_job_data/job_trace/' + jobid)
        front_file_name = '../../results_job_data/job_trace/'+ jobid + '/front_end_bw'+'.csv'
        tmp_file_name = '../../results_job_data/job_trace/'+ jobid + '/bw_iops_mds'+'.csv'
        save_tmp(resultr_band, resultw_band, resultr_iops, resultw_iops, \
        resultr_open, resultw_close, pe_r, pe_w, tmp_file_name)
        save_front_bw(resultr_band, resultw_band, front_file_name)
        draw_2d(resultr_band, resultw_band, 'IOBW')
        draw_2d(resultr_iops, resultw_iops, 'IOPS')
        draw_2d(pe_r, pe_w, 'PE')
        draw_d(file_open_count, 'Unique File Count')

    if options.trace == True:
        trace_file_name = '../../results_job_data/job_trace/' + jobid + '.csv'
        save_trace(results_message, results_host, trace_file_name)

    if options.fd_info == True:
        for fd in fd_info:
            print "[FD: %s] [filename: %s] [total_time: %d] \n\
            [sum_read_size: %f MB] [sum_write_size: %f MB] \n\
            [start_time: %s] [end_time: %s]"\
            %(fd, fd_info[fd]['file_name'], fd_info[fd]['total_time'], \
            fd_info[fd]['sum_read_size'], fd_info[fd]['sum_write_size'], \
            fd_info[fd]['start_time'], fd_info[fd]['end_time'])

    if options.ost_info == True:
        host = 90
        length = max_time - min_time

        file_open_set = set()
        for i in range(len(file_open)):
            file_open_set |= file_open[i]
        if('NULL' in file_open_set):
            file_open_set.remove('NULL')
        file_open_list = list(file_open_set)
        ost_list, target, file_ost_map = get_ost_list(file_open_list)
        front_ost_bw_r, front_ost_bw_w = ost_bw_front(fd_info, file_ost_map, length)
        save_fd_info(fd_info, 'test2_job.log')

#        results_message, results_host = search_interval_glb(time1, time2, index[0])
#        resultr_band,resultw_band,resultr_iops,resultw_iops,resultr_open,\
#        resultw_close, resultr_size, resultw_size, dictr, dictw, file_count, \
#        file_open_glb, fd_info_glb, fwd_file_map \
#        = deal_all_message(results_message, results_host, min_time, max_time)
#
#        save_fd_info(fd_info_glb, 'test2_glb.log')
#
#        print 'message length: ', len(results_message)
#        file_open_set_glb = set()
#        for i in range(len(file_open_glb)):
#            file_open_set_glb |= file_open_glb[i]
#        if('NULL' in file_open_set_glb):
#            file_open_set_glb.remove('NULL')
#        file_open_list_glb = list(file_open_set_glb)
#        ost_list_glb, target_glb, file_ost_map_glb = get_ost_list(file_open_list_glb)
#        glb_front_ost_bw_r, glb_front_ost_bw_w = ost_bw_front(fd_info_glb, file_ost_map_glb, length)
#        
#        for i in range(length):
#            bw_r = 0
#            bw_w = 0
#            glb_bw_r = 0
#            glb_bw_w = 0
#            for ost in ost_list:
#                ostid = int(ost)
#                bw_r += front_ost_bw_r[ostid][i]
#                bw_w += front_ost_bw_w[ostid][i]
#                glb_bw_r += glb_front_ost_bw_r[ostid][i]
#                glb_bw_w += glb_front_ost_bw_w[ostid][i]
#            print "bw_r: %f glb_bw_r: %f bw_w: %f glb_bw_w: %f"%(bw_r, glb_bw_r, bw_w, glb_bw_w)

        if(len(ost_list) == 0):
            print "Files have been deleted."
        else:
            bandr, bandw = get_ost_data(time1, time2, ost_list, index, host)

            if(not os.path.exists('../../results_job_data/job_trace/' + jobid)):
                os.mkdir('../../results_job_data/job_trace/' + jobid)
            ost_file_name = '../../results_job_data/job_trace/'+ jobid + '/back_end'+'.csv'
            ost_agg_file_name = '../../results_job_data/job_trace/'+ jobid + '/back_end_agg'+'.csv'

            save_back_bw(ost_list, bandr, bandw, ost_file_name)
            save_back_bw_agg(ost_list, bandr, bandw, ost_agg_file_name)

    if options.fwd_info == True:
        if(not os.path.exists('../../results_job_data/job_trace/' + jobid)):
            os.mkdir('../../results_job_data/job_trace/' + jobid)
        fwd_file_name = '../../results_job_data/job_trace/'+ jobid + '/fwd'+'.csv'
        for fwd in fwd_file_map:
            try:
                print fwd
                print fwd_file_map[fwd]
                file_list = ''
                if('NULL' in fwd_file_map[fwd]):
                    fwd_file_map[fwd].remove('NULL')
                file_list = list(fwd_file_map[fwd])
                ost_list, target, file_ost_map= get_ost_list(file_list)
    
                if(len(ost_list) == 0):
                    print "Files have been deleted."
                else:
                    if(target == 'online1'):
                        print "search online1.\n"
                        host = 87
                        bandr, bandw = get_fwd_data(time1, time2, fwd_map_1[fwd], \
                        ost_list, index, host)
                        save_fwd_bw(fwd, bandr, bandw, fwd_file_name)
                        print "fwd: %s info: \n"%fwd_map_1[fwd]
                        print bandr
                        print bandw
                    else:
                        print "search online2.\n"
                        host = 89
                        bandr, bandw = get_fwd_data(time1, time2, fwd_map_2[fwd], \
                        ost_list, index, host)
                        save_fwd_bw(fwd, bandr, bandw, fwd_file_name)
                        print "fwd: %s info: \n"%fwd_map_2[fwd]
                        print bandr
                        print bandw
            except Exception as e:
                print e

if __name__=="__main__":
    if (len(sys.argv) < 2):
        print "please input jobid e.g:6100000"
        sys.exit()
    start_time0 = clock()
    jobid=sys.argv[1]

    if (len(sys.argv) >= 6):
        s_time = sys.argv[2] + ' ' + sys.argv[3]
        e_time = sys.argv[4] + ' ' + sys.argv[5]

    print jobid
    show_job_all(jobid)
