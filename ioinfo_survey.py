# -*- coding: utf-8 -*-
import MySQLdb
import sys
import csv
import exceptions
import time
import multiprocessing
import gc
import heapq

k = 5
MAX_VAL = 0.0

def prt_msg(last_k_jobinfo):
        for jobname in last_k_jobinfo:
            msg = "jobname-cnc: %s job_num: %d iobw_r: %f iobw_w: %f \npe_r: %f pe_w: %f\n"\
            %(jobname, \
            last_k_jobinfo[jobname]['job_num'], \
            last_k_jobinfo[jobname]['iobw_r'], \
            last_k_jobinfo[jobname]['iobw_w'], \
            last_k_jobinfo[jobname]['pe_r']  , \
            last_k_jobinfo[jobname]['pe_w'])
            print msg

def get_job_info(jobname, CNC):
    conn=MySQLdb.connect(host='20.0.2.201',user='root',db='JOB_IO',passwd='',port=3306) 
    cursor=conn.cursor()
    sql = ''
    if(jobname == '' and CNC == 0):
        sql = "select JOB_NAME, CNC, JOBID, IOBW_READ_AVERAGE, \
        IOBW_WRITE_AVERAGE, IOPS_READ_AVERAGE, IOPS_WRITE_AVERAGE, \
        IOTIME_COUNT, MDS_OPEN_AVERAGE, PROCESS_READ_MAX, \
        PROCESS_WRITE_MAX, FILENAME_UNIQUE_COUNT \
        from JOB_IO_INFO"
    else: 
        sql = "select JOB_NAME, CNC, JOBID, IOBW_READ_AVERAGE, \
        IOBW_WRITE_AVERAGE, IOPS_READ_AVERAGE, IOPS_WRITE_AVERAGE, \
        IOTIME_COUNT, MDS_OPEN_AVERAGE, PROCESS_READ_MAX, \
        PROCESS_WRITE_MAX, FILENAME_UNIQUE_COUNT \
        from JOB_IO_INFO where JOB_NAME = '%s' and CNC = '%s'"%(jobname, CNC)

    cursor.execute(sql)
    result = cursor.fetchall()
    array = str(result[0]).split(',')
    jobname = array[0][2:-1]+'-'+array[1][1:-1]
    conn.commit()
    cursor.close()
    conn.close()

    return result
    
def deal_jobinfo(result):
    job_info = dict()
    for res in result:
        array = str(res).split(',')
        jobname = array[0][2:-1]+'-'+array[1][1:-1]
        jobid = int(array[2][2:-1])
        if(jobname not in job_info):
            job_info[jobname] = {}
        job_info[jobname][jobid] = {}
        job_info[jobname][jobid]['iobw_r'] = float(array[3])
        job_info[jobname][jobid]['iobw_w'] = float(array[4])
        job_info[jobname][jobid]['iops_r'] = float(array[5])
        job_info[jobname][jobid]['iops_w'] = float(array[6])
        job_info[jobname][jobid]['iotime'] = float(array[7][:-1])
        job_info[jobname][jobid]['mds'] = float(array[8])
        job_info[jobname][jobid]['pe_r'] = float(array[9][:-1])
        job_info[jobname][jobid]['pe_w'] = float(array[10][:-1])
        job_info[jobname][jobid]['file_count'] = float(array[11][:-2])
    return job_info

def get_last_k_jobinfo(k, job_info):
    last_k_jobinfo = dict()
    for jobname in job_info:
        jobid_list = sorted(job_info[jobname].keys(), reverse=True)
        if(len(jobid_list) >= k):
            length = k
        else:
            length = len(jobid_list)

        sum_iobw_r = 0
        sum_iobw_w = 0
        sum_pe_r = 0
        sum_pe_w = 0
        for i in range(0, length):
            print"%s %f %f %f %f"\
            %(jobid_list[i], job_info[jobname][jobid_list[i]]['iobw_r'], \
            job_info[jobname][jobid_list[i]]['iobw_w'], \
            job_info[jobname][jobid_list[i]]['pe_r'], \
            job_info[jobname][jobid_list[i]]['pe_w'])
            sum_iobw_r += job_info[jobname][jobid_list[i]]['iobw_r']
            sum_iobw_w += job_info[jobname][jobid_list[i]]['iobw_w']
            sum_pe_r   += job_info[jobname][jobid_list[i]]['pe_r']
            sum_pe_w   += job_info[jobname][jobid_list[i]]['pe_w']
        
        last_k_jobinfo[jobname] = {}
        last_k_jobinfo[jobname]['job_num'] = length
        last_k_jobinfo[jobname]['iobw_r']  = sum_iobw_r / length
        last_k_jobinfo[jobname]['iobw_w']  = sum_iobw_w / length
        last_k_jobinfo[jobname]['pe_r']    = sum_pe_r   / length
        last_k_jobinfo[jobname]['pe_w']    = sum_pe_w   / length

    return last_k_jobinfo 
    
def get_avg_last_k_jobinfo(k, job_info):
    last_k_jobinfo = dict()
    all_match = 0
    all_job = 0
    for jobname in job_info:
        jobid_list = sorted(job_info[jobname].keys())
        match = 0.0
        if(len(jobid_list) >= k + 1):
            length = float(k)
            all_avg_length = float(len(jobid_list) - k)
            for i in range(k, len(jobid_list)):
                sum_iobw_r = 0
                sum_iobw_w = 0
                sum_pe_r = 0
                sum_pe_w = 0
                for j in range(i - k, i):
                    sum_iobw_r += job_info[jobname][jobid_list[j]]['iobw_r']
                    sum_iobw_w += job_info[jobname][jobid_list[j]]['iobw_w']
                    sum_pe_r += job_info[jobname][jobid_list[j]]['pe_r']
                    sum_pe_w += job_info[jobname][jobid_list[j]]['pe_w']
                if(not last_k_jobinfo.has_key(jobname)): 
                    last_k_jobinfo[jobname] = {}
                last_k_jobinfo[jobname][i] = {}
                last_k_jobinfo[jobname][i]['job_num'] = len(jobid_list)
                if(sum_iobw_r > 0.0):
                    last_k_jobinfo[jobname][i]['iobw_r']     = \
                    abs(job_info[jobname][jobid_list[j]]['iobw_r'] \
                    /(sum_iobw_r / length) - 1)
                else:
                    last_k_jobinfo[jobname][i]['iobw_r'] = MAX_VAL
                if(sum_iobw_w > 0.0):
                    last_k_jobinfo[jobname][i]['iobw_w']     = \
                    abs(job_info[jobname][jobid_list[j]]['iobw_w'] \
                    /(sum_iobw_w / length) - 1)
                else:
                    last_k_jobinfo[jobname][i]['iobw_w'] = MAX_VAL
                if(sum_pe_r > 0.0):
                    last_k_jobinfo[jobname][i]['pe_r']       = \
                    abs(job_info[jobname][jobid_list[j]]['pe_r']/ \
                    (sum_pe_r / length) - 1)
                else:
                    last_k_jobinfo[jobname][i]['pe_r'] = 0.0
                if(sum_pe_w > 0.0):
                    last_k_jobinfo[jobname][i]['pe_w']       = \
                    abs(job_info[jobname][jobid_list[j]]['pe_w']/ \
                    (sum_pe_w / length) - 1)
                else:
                    last_k_jobinfo[jobname][i]['pe_w'] = 0.0
                if(last_k_jobinfo[jobname][i]['iobw_r'] < 0.2 and 
                last_k_jobinfo[jobname][i]['iobw_w'] < 0.2 and 
                last_k_jobinfo[jobname][i]['pe_r'] < 0.2 and 
                last_k_jobinfo[jobname][i]['pe_w'] < 0.2):
                    match += 1.0
            last_k_jobinfo[jobname]['match'] = \
            match / all_avg_length
            all_match += match
                    
        all_job += len(jobid_list)

    print "match: %f  all_jobid: %f"%(all_match, all_job) 
            
    return last_k_jobinfo 



def save_avg_last_k_jobinfo(topk_file, last_k_jobinfo, k):
    f = open(topk_file,'wb')
    jobname_list = last_k_jobinfo.keys()
    sorted(jobname_list)
    for jobname in jobname_list:
        if(last_k_jobinfo[jobname].has_key('avg_iobw_r')):
                writerow = "%s %d %f %f %f %f\n"\
                %(jobname, \
                last_k_jobinfo[jobname][k]['job_num'], \
                last_k_jobinfo[jobname]['avg_iobw_r'], \
                last_k_jobinfo[jobname]['avg_iobw_w'], \
                last_k_jobinfo[jobname]['avg_pe_r']   , \
                last_k_jobinfo[jobname]['avg_pe_w'])
                f.write(writerow)
   
def save_last_k_jobinfo(topk_file, last_k_jobinfo, k):
    f = open(topk_file,'wb')
    jobname_list = last_k_jobinfo.keys()
    sorted(jobname_list)
    for jobname in jobname_list:
        if(last_k_jobinfo[jobname].has_key(k)):
            for i in last_k_jobinfo[jobname]:
                writerow = "%s %d %d %f %f %f %f %f %f %f %f %f\n"\
                %(jobname, i, \
                last_k_jobinfo[jobname][i]['job_num'], \
                last_k_jobinfo[jobname][i]['iobw_r'], \
                last_k_jobinfo[jobname][i]['iobw_w'], \
                last_k_jobinfo[jobname][i]['iops_r'], \
                last_k_jobinfo[jobname][i]['iops_w'], \
                last_k_jobinfo[jobname][i]['iotime'], \
                last_k_jobinfo[jobname][i]['mds']   , \
                last_k_jobinfo[jobname][i]['pe_r']  , \
                last_k_jobinfo[jobname][i]['pe_w']  , \
                last_k_jobinfo[jobname][i]['file_count'])
                f.write(writerow)

def cal_all_job(k):
    topk_file = '/home/export/mount_test/swstorage/results_job_data/topk_jobinfo/top_%d_ioinfo.csv'%k
    avg_topk_file = '/home/export/mount_test/swstorage/results_job_data/topk_jobinfo/avg_top_%d_ioinfo.csv'%k
    row_jobinfo = get_job_info('', 0)
    job_info = deal_jobinfo(row_jobinfo)
    avg_last_k_jobinfo = get_avg_last_k_jobinfo(k, job_info)
    #save_avg_last_k_jobinfo(avg_topk_file, avg_last_k_jobinfo, k)
    #last_k_job = get_last_k_jobinfo(k, job_info)
    #save_last_k_jobinfo(topk_file, last_k_job, k)

def cal_sgl_job(jobname, CNC):
    row_jobinfo = get_job_info(jobname, CNC)
    job_info = deal_jobinfo(row_jobinfo)
    last_k_jobinfo = get_last_k_jobinfo(k, job_info)
    prt_msg(last_k_jobinfo)

if __name__ == "__main__":
    if(len(sys.argv) < 4):
        if(len(sys.argv) < 2):
            print "The default setting is get lastest 5 job info."
        else:
            k = int(sys.argv[1])
            print "Get lastest %d job info."%k

        cal_all_job(k)
    else:
        k = int(sys.argv[1])
        jobname = sys.argv[2]
        CNC = sys.argv[3]
        cal_sgl_job(jobname, CNC)
