# -*- coding: utf-8 -*-
import MySQLdb
import sys
import csv
import exceptions
import time
import multiprocessing
import gc

def get_jobid():
    jobid = []
    conn=MySQLdb.connect(host='20.0.2.201',user='root',db='JOB_IO',passwd='',port=3306) 
    cursor=conn.cursor()
    sql = "select jobid from JOB_IO_INFO where JOB_NAME is null"
    cursor.execute(sql)
    result=cursor.fetchall()
    for res in result:
        jobid.append(str(res)[2:-3])
    conn.commit()
    cursor.close()
    conn.close()

    return jobid

    
def get_job_name(jobid):
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
        sql1="select JOB_NAME from %s where jobid=%s"%(table, jobid)
        cursor.execute(sql1)
        result = cursor.fetchall()
        conn.commit()
        cursor.close()
        return str(result[0])[2:-3]
    except Exception as e:
        print e

def update_jobname(jobid, jobname):
    conn=MySQLdb.connect(host='20.0.2.201',user='root',db='JOB_IO',passwd='',port=3306) 
    cursor=conn.cursor()
    sql = "UPDATE JOB_IO_INFO SET JOB_NAME='%s' WHERE jobid=%s"%(jobname, jobid)
    print sql
    cursor.execute(sql)
    result=cursor.fetchall()
    print result
    conn.commit()
    cursor.close()
    conn.close()

if __name__ == "__main__":
    
    jobid = get_jobid()
    for job in jobid:
        try:
            jobname = get_job_name(job)
            print job
            print jobname
#            update_jobname(job, jobname)
        except Exception as e:
            print e


