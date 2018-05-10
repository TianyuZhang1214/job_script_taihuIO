# -*- coding: utf-8 -*-
import MySQLdb
import sys
sys.path.append("../query_script")
from job_ip_all import get_re_jobid_CNC_runtime_corehour as get_re_jobid
from showjob_by_jobid import compute_pre_with_jobid
from savejob_jobid_modified import sum_process, sum_IOBANDWIDTH, sum_IOPS, sum_MDS, compute_index
import exceptions
import es_search
from deal_generator import deal_all_message
import time
import csv

IOBW_file_name = '../../results_job_data/collect_data/all_data/IOBW.csv'
IOPS_file_name = '../../results_job_data/collect_data/all_data/IOPS.csv'
MDS_file_name = '../../results_job_data/collect_data/all_data/MDS.csv'
PE_file_name = '../../results_job_data/collect_data/all_data/maxPE.csv'

file_name = '../../source_job_data/JOB_log.csv'

IOPS_BW_MDS_file_name = '../../results_job_data/collect_data/format_data/IOPS_BW_MDS_file.csv'

thread_count=2
#def save_format_data():
#
#    ('%s','%s','%d','%f','%d','%f',\
#    '%f','%d','%f','%d','%f','%d','%f','%f','%d','%f',\
#    '%d','%d','%d','%d','%f','%d','%d','%f',\
#    '%d','%d','%d','%d')

def read_jobID_prog():

    jobID = []
    prog_name = []
    CNC = []

    reader = csv.reader(open(file_name, 'rU'), dialect=csv.excel_tab)
    for line in reader:
        if(len(line) >= 16):
            jobID.append(line[0].strip())
            prog_name.append(line[14].strip())
            CNC.append(int(line[4].strip()))

    return jobID, prog_name, CNC

def read_PE():
    reader = csv.reader(open(PE_file_name , 'rU'), dialect=csv.excel_tab)
    next(reader)

    pe_r = []
    pe_w = []

    for line in reader:
        x = str(line).split(',')
        if(len(x) == 6):
            try:
                pe_r.append(float(x[4].strip()))
                pe_w.append(float(x[5][:-2].strip()))
            except Exception as e:
                print x[4]
                print x[5][:-2]
                continue

    return pe_r, pe_w

def read_IOBW():
    reader = csv.reader(open(IOBW_file_name , 'rU'), dialect=csv.excel_tab)
    next(reader)

    jobID = []
    sum_READ = []
    sum_WRITE = []
    count_r = []
    count_w = []
    count_rw = []
    average_r = []
    average_w = []

    for line in reader:
        x = str(line).split(',')
        if(len(x) == 11):
            try:
                sum_READ.append(float(x[4].strip()))
                sum_WRITE.append(float(x[5].strip()))
                count_r.append(float(x[6].strip()))
                count_w.append(float(x[7].strip()))
                count_rw.append(float(x[8].strip()))
                average_r.append(float(x[9].strip()))
                average_w.append(float(x[10][:-2].strip()))
                jobID.append(x[1].strip())
            except Exception as e:
                print x
                print len(x)
                continue

    return jobID, sum_READ, sum_WRITE, count_r, count_w, count_rw, average_r, average_w

def read_IOPS():
    reader = csv.reader(open(IOPS_file_name , 'rU'), dialect=csv.excel_tab)
    next(reader)

    sum_READ = []
    sum_WRITE = []
    count_r = []
    count_w = []
    count_rw = []
    count_rw_all = []
    average_r = []
    average_w = []

    for line in reader:
        x = str(line).split(',')
        if(len(x) == 12):
            try:
                sum_READ.append(float(x[4].strip()))
                sum_WRITE.append(float(x[5].strip()))
                count_r.append(float(x[6].strip()))
                count_w.append(float(x[7].strip()))
                count_rw.append(float(x[8].strip()))
                count_rw_all.append(float(x[9].strip()))
                average_r.append(float(x[10].strip()))
                average_w.append(float(x[11][:-2].strip()))
            except Exception as e:
                print x
                continue

    return sum_READ, sum_WRITE, count_r, count_w, count_rw, count_rw_all, average_r, average_w

def read_MDS():
    reader = csv.reader(open(MDS_file_name , 'rU'), dialect=csv.excel_tab)
    next(reader)

    sum_OPEN = []
    sum_CLOSE = []
    count_o = []
    count_c = []
    count_oc = []
    average_o = []
    average_c = []
    file_count = []
    for line in reader:
        x = str(line).split(',')
        if(len(x) == 12):
            try:
                sum_OPEN.append(float(x[4].strip()))
                sum_CLOSE.append(float(x[5].strip()))
                count_o.append(float(x[6].strip()))
                count_c.append(float(x[7].strip()))
                count_oc.append(float(x[8].strip()))
                average_o.append(float(x[9].strip()))
                average_c.append(float(x[10].strip()))
                file_count.append(float(x[11][:-2].strip()))
            except Exception as e:
                continue

    return sum_OPEN, sum_CLOSE, count_o, count_c, count_oc, average_o, average_c, file_count

def search_for_info(jobID, jobID_read, compute_node_count_read, program_name_read):
    
    compute_node_count = []
    program_name = []
    for i in xrange(len(jobID)):
        for j in xrange(len(jobID_read)):
            if(jobID[i] == jobID_read[j]):
                compute_node_count.append(compute_node_count_read[j])
                program_name.append(program_name_read[j])
                break
#            print jobID[i]
    return program_name, compute_node_count

def get_all_data():

    jobID, sum_IOBW_r, sum_IOBW_w, count_IOBW_r, \
    count_IOBW_w, count_IOBW_rw, average_IOBW_r, average_IOBW_w = \
    read_IOBW()

    sum_IOPS_r, sum_IOPS_w, count_IOPS_r, \
    count_IOPS_w, count_IOPS_rw, count_IOPS_rw_all, \
    average_IOPS_r, average_IOPS_w = \
    read_IOPS()

    sum_MDS_o, sum_MDS_c, count_MDS_o, \
    count_MDS_c, count_MDS_oc, average_MDS_o, \
    average_MDS_c, file_all_count = \
    read_MDS()
          
    pe_r, pe_w = read_PE()
    
    print len(jobID)
    print len(sum_IOBW_r)
    print len(sum_IOBW_w)
    print len(count_IOBW_r)
    print len(count_IOBW_w)
    print len(count_IOBW_rw) 
    print len(average_IOBW_r) 
    print len(average_IOBW_w)
    
    print len(sum_IOPS_r)
    print len(sum_IOPS_w)
    print len(count_IOPS_r)
    print len(count_IOPS_w)
    print len(count_IOPS_rw)
    print len(count_IOPS_rw_all)
    print len(average_IOPS_r) 
    print len(average_IOPS_w)
    
    print len(sum_MDS_o)
    print len(sum_MDS_c)
    print len(count_MDS_o)
    print len(count_MDS_c)
    print len(count_MDS_oc)
    print len(average_MDS_o)
    print len(average_MDS_c)
    print len(file_all_count)
    
    print len(pe_r)
    print len(pe_w)
    
    jobID_read, program_name_read, CNC_read = read_jobID_prog()
    
    program_name, CNC = \
    search_for_info(jobID, jobID_read, CNC_read, program_name_read)

    print len(program_name)

    return jobID, program_name, CNC, \
    sum_IOBW_r, sum_IOBW_w, \
    count_IOBW_r, count_IOBW_w, count_IOBW_rw, \
    average_IOBW_r, average_IOBW_w, \
    sum_IOPS_r, sum_IOPS_w, count_IOPS_r, count_IOPS_w, \
    count_IOPS_rw, count_IOPS_rw_all, \
    average_IOPS_r, average_IOPS_w, \
    sum_MDS_o, sum_MDS_c, \
    count_MDS_o, count_MDS_c, count_MDS_oc, \
    average_MDS_o, average_MDS_c, \
    pe_r, pe_w, file_all_count

def insert(column):
    conn = MySQLdb.connect(host='20.0.2.201',user='root',db='JOB_IO',passwd='',port=3306) 
    print "connect success-----------------"
    try:
        jobID, program_name, CNC, \
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
        get_all_data()
        cursor=conn.cursor()
        for i in range(len(jobID)):
            try:
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
                %(jobID[i], program_name[i], CNC[i], \
                sum_IOBW_r[i], count_IOBW_r[i], \
                average_IOBW_r[i], sum_IOBW_w[i], \
                count_IOBW_w[i], average_IOBW_w[i], \
                count_IOBW_rw[i], sum_IOPS_r[i], \
                count_IOPS_r[i], average_IOPS_r[i], \
                sum_IOPS_w[i], count_IOPS_w[i], \
                average_IOPS_w[i], count_IOPS_rw[i], \
                count_IOPS_rw_all[i], sum_MDS_o[i], \
                count_MDS_o[i], average_MDS_o[i], \
                sum_MDS_c[i], count_MDS_c[i], \
                average_MDS_c[i], count_MDS_oc[i], \
                max_PE_r[i], max_PE_w[i], file_all_count[i])
                print sql
                cursor.execute(sql)
                conn.commit()
            except Exception as e:
                print e
                conn.rollback()
        cursor.close()
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
if __name__=="__main__":
    column = get_column_name()
    insert(column)
