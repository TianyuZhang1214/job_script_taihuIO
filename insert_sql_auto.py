# -*- coding: utf-8 -*-
import MySQLdb
import sys
import csv
import exceptions
import time
import multiprocessing
import gc

sys.path.append("../query_script")
from job_ip_all import get_re_jobid_CNC_runtime_corehour as get_re_jobid
from showjob_by_jobid import compute_pre_with_jobid
from savejob_jobid_modified import sum_process, sum_IOBANDWIDTH, sum_IOPS, sum_MDS, compute_index
import es_search
from deal_generator import deal_all_message
#Last JOBID queried before: 40645719
#40786081
#40880389
startjobid = 41240832
jobid_interval = 10000
thread_count=1
size_ip = 50

abnormal_job_file = "../../results_job_data/collect_data/insert_data/abnormal_insert_job.csv"
job_file = "../../results_job_data/collect_data/insert_data/insert_job_completed.csv"
job_file_all = "../../results_job_data/collect_data/insert_data/insert_job_all.csv"

def save_job(jobid, file_name):
    csvfile = file(file_name,'ab')
    writer=csv.writer(csvfile)
    writer.writerow([jobid])
    csvfile.close()

def compute_result(jobid):
    try:
        time1, time2, iplist, min_time, max_time \
        = compute_pre_with_jobid(jobid)
    except Exception as e:
        print e
    
    if (time1[8:10] == time2[8:10]):
        index = [time1[0:4] + "." + time1[5:7] + "." + time1[8:10]]
    else:
        index = compute_index(time1, time2) 
    
    count_ip = len(iplist)/size_ip
    remainder = len(iplist)%size_ip
    results_message = [] 
    results_host = []
    
    try:
        for lnd in xrange(len(index)):
            if (count_ip > 0):
                for c1 in xrange(count_ip):
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
    
    try:
        resultr_band,resultw_band,resultr_iops,resultw_iops,resultr_open,\
        resultw_close, resultr_size, resultw_size, dictr, dictw, file_count, file_open\
        = deal_all_message(results_message, results_host, min_time, max_time)
        del results_message
        del results_host
        gc.collect()
    except Exception as e:
        print e

    max_PE_r = sum_process(dictr,'r',min_time,max_time)
    max_PE_w = sum_process(dictw,'b',min_time,max_time)
    
    sum_resultr, sum_resultw, average_resultr, average_resultw, \
    count_resultr, count_resultw, count_resultrw, \
    average_resultr, average_resultw, \
    = sum_IOBANDWIDTH(resultr_band,resultw_band)
    
    sum_IOPS_r, sum_IOPS_w, count_IOPS_r, count_IOPS_w, \
    count_IOPS_rw, count_IOPS_rw_all, \
    average_IOPS_r, average_IOPS_w, \
    = sum_IOPS(resultr_iops,resultw_iops,resultr_band,resultw_band)
   
    sum_MDS_o, sum_MDS_c, \
    count_MDS_o, count_MDS_c, count_MDS_oc, \
    average_MDS_o, average_MDS_c, \
    = sum_MDS(resultr_open,resultw_close)
    
    return sum_resultr, sum_resultw,\
    count_resultr, count_resultw, count_resultrw, average_resultr, average_resultw, \
    sum_IOPS_r, sum_IOPS_w, count_IOPS_r, count_IOPS_w, \
    count_IOPS_rw, count_IOPS_rw_all, \
    average_IOPS_r, average_IOPS_w, \
    sum_MDS_o, sum_MDS_c, \
    count_MDS_o, count_MDS_c, count_MDS_oc, \
    average_MDS_o, average_MDS_c, \
    max_PE_r , max_PE_w, file_count      

def exist_job(jobid):
    conn=MySQLdb.connect(host='20.0.2.201',user='root',db='JOB_IO',passwd='',port=3306) 
    print "connect success----"
    cursor=conn.cursor()
#    sql = "select COLUMN_NAME from information_schema.COLUMNS where table_name = 'JOB_IO_INFO'"
    sql = "select 1 from JOB_IO_INFO where JOBID = '"+ jobid +"' limit 1"
    cursor.execute(sql)
    result=cursor.fetchall()
    tag = len(result)
    print tag
    conn.commit()
    cursor.close()
    conn.close()
    return tag 

def get_nonexistent_job(jobid_read):
    jobid = []
    for i in xrange(len(jobid_read)):
        tag = exist_job(jobid_read[i])
        if(tag == 1):
            continue
        else:
            jobid.append(jobid_read[i])
    return jobid

def insert(jobid, column):
    conn = MySQLdb.connect(host='20.0.2.201',user='root',db='JOB_IO',passwd='',port=3306) 
    print "connect success-----------------"
    try:
        resu=get_re_jobid(jobid)
        for val in resu:
            program_name = val[2]
            CNC = int(val[5])
        try:
            sum_IOBW_r, sum_IOBW_w, \
            count_IOBW_r, count_IOBW_w, count_IOBW_rw, \
            average_IOBW_r, average_IOBW_w, \
            sum_IOPS_r, sum_IOPS_w, count_IOPS_r, count_IOPS_w, \
            count_IOPS_rw, count_IOPS_rw_all, \
            average_IOPS_r, average_IOPS_w, \
            sum_MDS_o, sum_MDS_c, \
            count_MDS_o, count_MDS_c, count_MDS_oc, \
            average_MDS_o, average_MDS_c, \
            max_PE_r, max_PE_w, file_all_count= \
            compute_result(jobid)
            cursor=conn.cursor()
            sql="INSERT INTO JOB_IO_INFO( \
            "+column[0]+","+column[1]+","+ \
            column[2]+","+column[3]+","+ \
            column[4]+","+column[5]+","+ \
            column[6]+","+column[7]+","+ \
            column[8]+","+column[9]+","+ \
            column[10]+","+column[11]+","+ \
            column[12]+","+column[13]+","+ \
            column[14]+","+column[15]+","+ \
            column[16]+","+column[17]+","+ \
            column[18]+","+column[19]+","+ \
            column[20]+","+column[21]+","+ \
            column[22]+","+column[23]+","+ \
            column[24]+","+column[25]+","+ \
            column[26]+","+column[27] + ")"\
            +" values('%s','%s','%d','%f','%d','%f',\
            '%f','%d','%f','%d','%f','%d','%f','%f','%d','%f',\
            '%d','%d','%d','%d','%f','%d','%d','%f',\
            '%d','%d','%d','%d')" \
            %(jobid, program_name, CNC, sum_IOBW_r, count_IOBW_r, average_IOBW_r, \
            sum_IOBW_w, count_IOBW_w, average_IOBW_w, count_IOBW_rw, \
            sum_IOPS_r, count_IOPS_r, average_IOPS_r, \
            sum_IOPS_w, count_IOPS_w, average_IOPS_w, count_IOPS_rw, count_IOPS_rw_all, \
            sum_MDS_o, count_MDS_o, average_MDS_o, \
            sum_MDS_c, count_MDS_c, average_MDS_c, count_MDS_oc, \
            max_PE_r, max_PE_w, file_all_count)
            cursor.execute(sql)
            conn.commit()
            cursor.close()
            save_job(jobid, job_file)
        except Exception as e:
            save_job(jobid, abnormal_job_file )
            print e
            conn.rollback()
    except Exception as e:
        print e
    conn.close()

def get_column_name():
    conn=MySQLdb.connect(host='20.0.2.201',user='root',db='JOB_IO',passwd='',port=3306) 
    cursor=conn.cursor()
    sql = "select COLUMN_NAME from information_schema.COLUMNS where table_name = 'JOB_IO_INFO'"
    cursor.execute(sql)
    result=cursor.fetchall()
    column = []
    for re in result:
        column.append(str(re)[2:-3])
    conn.commit()
    cursor.close()
    conn.close()
    return column

def get_largest_jobid():
    conn=MySQLdb.connect(host='20.0.2.15',user='swqh',db='JOB',passwd='123456',port=3306) 
    cursor=conn.cursor()
    sql = "select max(JOBID) from JOB_log"
    cursor.execute(sql)
    result=cursor.fetchall()
    max_jobid = 0
    for re in result:
        max_jobid = int(str(re)[2:-3])
    cursor.close()
    conn.close()
    return max_jobid


def get_jobid_larger_32(startid, endid):
    conn=MySQLdb.connect(host='20.0.2.15',user='swqh',db='JOB',passwd='123456',port=3306) 
    cursor=conn.cursor()
    sql = "select JOBID from JOB_log where \
    state = 'Done' and \
    convert(JOBID,UNSIGNED) >= %d and \
    convert(JOBID,UNSIGNED) < %d and CNC >= 32"\
    %(startid, endid)
    cursor.execute(sql)
    result=cursor.fetchall()
    jobid = []
    for re in result:
        jobid.append(str(re)[2:-3])
    jobid.sort()
    conn.commit()
    cursor.close()
    conn.close()
    return jobid

def insert_main(column):
    tag = 1
    startid = startjobid 
    while(tag):
        max_jobid = get_largest_jobid()
        while(startid < max_jobid):
            jobid_read = get_jobid_larger_32(startid, startid+jobid_interval)
            jobid = get_nonexistent_job(jobid_read)
            for i in range(len(jobid)):
                save_job(jobid[i], job_file_all)
            p = multiprocessing.Pool(thread_count)
            for i in range(len(jobid)):
                 try:
                     p.apply_async(insert, args=(jobid[i], column))
                 except Exception as e:
                     print 'jobid: %s failed.'%(jobid[i])
                     print e
            print 'jobid in range(%s, %s) completed.'%(startid, startid+jobid_interval)
            p.close()
            p.join()
            startid += jobid_interval
        time.sleep(600)

def test_sql():
    conn=MySQLdb.connect(host='20.0.2.201',user='root',db='JOB_IO',passwd='',port=3306) 
    print "connect success----"
    cursor=conn.cursor()
#    sql = "select COLUMN_NAME from information_schema.COLUMNS where table_name = 'JOB_IO_INFO'"
    sql = "select 1 from JOB_IO_INFO where JOBID = '41027' limit 1"
    cursor.execute(sql)
    result=cursor.fetchall()
    print len(result)
    for re in result:
        tag = int(re[0])
    conn.commit()
    cursor.close()
    conn.close()
if __name__=="__main__":
    #test_sql()
    column = get_column_name()
    insert_main(column)
