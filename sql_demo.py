# -*- coding: utf-8 -*-
import MySQLdb
import sys
import csv
import exceptions
import time
import multiprocessing
import gc
# Structure of Table JOB_IO_INFO 
'''
+-----------------------+--------------+------+-----+---------+-------+
| Field                 | Type         | Null | Key | Default | Extra |
+-----------------------+--------------+------+-----+---------+-------+
| JOBID                 | varchar(20)  | NO   | PRI | NULL    |       |
| PROGRAM_NAME          | varchar(500) | NO   |     | NULL    |       |
| CNC                   | int(11)      | NO   |     | NULL    |       |
| IOBW_READ_SUM         | double       | YES  |     | NULL    |       |
| IOBW_READ_COUNT       | int(11)      | YES  |     | NULL    |       |
| IOBW_READ_AVERAGE     | double       | YES  |     | NULL    |       |
| IOBW_WRITE_SUM        | double       | YES  |     | NULL    |       |
| IOBW_WRITE_COUNT      | int(11)      | YES  |     | NULL    |       |
| IOBW_WRITE_AVERAGE    | double       | YES  |     | NULL    |       |
| IOBW_ALL_COUNT        | int(11)      | YES  |     | NULL    |       |
| IOPS_READ_SUM         | double       | YES  |     | NULL    |       |
| IOPS_READ_COUNT       | int(11)      | YES  |     | NULL    |       |
| IOPS_READ_AVERAGE     | double       | YES  |     | NULL    |       |
| IOPS_WRITE_SUM        | double       | YES  |     | NULL    |       |
| IOPS_WRITE_COUNT      | int(11)      | YES  |     | NULL    |       |
| IOPS_WRITE_AVERAGE    | double       | YES  |     | NULL    |       |
| IOPS_ALL_COUNT        | int(11)      | YES  |     | NULL    |       |
| IOTIME_COUNT          | int(11)      | YES  |     | NULL    |       |
| MDS_OPEN_SUM          | int(11)      | YES  |     | NULL    |       |
| MDS_OPEN_COUNT        | int(11)      | YES  |     | NULL    |       |
| MDS_OPEN_AVERAGE      | double       | YES  |     | NULL    |       |
| MDS_CLOSE_SUM         | int(11)      | YES  |     | NULL    |       |
| MDS_CLOSE_COUNT       | int(11)      | YES  |     | NULL    |       |
| MDS_CLOSE_AVERAGE     | double       | YES  |     | NULL    |       |
| MDS_ALL_COUNT         | int(11)      | YES  |     | NULL    |       |
| PROCESS_READ_MAX      | int(11)      | YES  |     | NULL    |       |
| PROCESS_WRITE_MAX     | int(11)      | YES  |     | NULL    |       |
| FILENAME_UNIQUE_COUNT | int(11)      | YES  |     | NULL    |       |
| JOB_NAME              | varchar(500) | YES  |     | NULL    |       |
+-----------------------+--------------+------+-----+---------+-------+
'''

def get_jobid():
    jobid = []
    conn = MySQLdb.connect(host='20.0.2.201', user='root', db='JOB_IO', passwd='', port=3306) 
    cursor = conn.cursor()
    sql = "select * from JOB_IO_INFO where JOBID = 42606463;"
    cursor.execute(sql)
    result = cursor.fetchall()
    for res in result:
        print res
        #jobid.append(str(res)[2:-3])
    #for job in jobid:
        #print job
    conn.commit()
    cursor.close()
    conn.close()

    return jobid
    

    
    
    
if __name__ == "__main__":
    
    jobid = get_jobid()

