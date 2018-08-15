# -*- coding: utf-8 -*-
import MySQLdb
import sys
import csv
import exceptions
import time
import multiprocessing
import gc

field = 'COREHOUR'
table_list = ['JOB_log_all', 'JOB_log_4050', 'JOB_log_4100', \
'JOB_log_4150', 'JOB_log_4200', 'JOB_log_4250', 'JOB_log']


def get_jobid(field):
    jobid = []
    conn=MySQLdb.connect(host='20.0.2.201',user='root',db='JOB_IO',passwd='',port=3306) 
    cursor=conn.cursor()
    sql = "select jobid from JOB_IO_INFO where %s is null"%(field)
    cursor.execute(sql)
    result=cursor.fetchall()
    for res in result:
        jobid.append(str(res)[2:-3])
    conn.commit()
    cursor.close()
    conn.close()

    return jobid

def get_job_info_all_table(field):
    job_info = dict()
    result = [] 
    conn=MySQLdb.connect(host='20.0.2.15',user='swqh',db='JOB',passwd='123456',port=3306)
    try:
        cursor=conn.cursor()
        for table in table_list:
            sql = "select jobid, %s from %s where CNC >= 32 \
            and QUEUE like '%%sw%%' and STATE like '%%Done%%'"\
            %(field, table)
            cursor.execute(sql)
            result += cursor.fetchall()
        conn.commit()
        cursor.close()
    except Exception as e:
        print e

    for res in result:
        job_info[res[0]] = res[1]

#    print len(job_info)
    return job_info

def get_job_info(jobid, field):
    conn=MySQLdb.connect(host='20.0.2.15',user='swqh',db='JOB',passwd='123456',port=3306)
    try:
        cursor=conn.cursor()
        if int(jobid)<=10000000:
            table="JOB_log_all"
        elif int(jobid)<=40500000:
            table="JOB_log_4050"
        elif int(jobid)<=41000000:
            table="JOB_log_4100"
        elif int(jobid)<=41500000:
            table="JOB_log_4150"
        elif int(jobid)<=42000000:
            table="JOB_log_4200"
        elif int(jobid)<=42500000:
            table="JOB_log_4250"
        else:
            table="JOB_log"
        sql1 = "select %s from %s where jobid = %s"%(field, table, jobid)
        cursor.execute(sql1)
        result = cursor.fetchall()
        conn.commit()
        cursor.close()
#        print result[0][0]
        return result[0][0]
    except Exception as e:
        print e

def update_jobname(jobid, jobname, field):
    conn = MySQLdb.connect(host='20.0.2.201',user='root',db='JOB_IO',passwd='',port=3306) 
    cursor = conn.cursor()
    sql = "UPDATE JOB_IO_INFO SET %s='%s' WHERE jobid=%s"%(field, jobname, jobid)
#    print sql
    cursor.execute(sql)
    result = cursor.fetchall()
#    print result
    conn.commit()
    cursor.close()
    conn.close()

def delete_job(jobid):

    conn = MySQLdb.connect(host='20.0.2.201',user='root',db='JOB_IO',passwd='',port=3306) 
    cursor = conn.cursor()
    sql = "DELETE from JOB_IO_INFO where jobid = %s"%(jobid)
#    print sql
    cursor.execute(sql)
    result = cursor.fetchall()
#    print result
    conn.commit()
    cursor.close()
    conn.close()
        
if __name__ == "__main__":
    
    jobid = get_jobid(field)
    job_info = get_job_info_all_table(field)

    for job in jobid:
        try:
#            if(not job_info.has_key(job)):
#                delete_job(job)
#                continue
#            else:
            update_jobname(job, job_info[job], field)
        except Exception as e:
            print e




