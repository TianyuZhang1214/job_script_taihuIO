#!/usr/bin/python
import csv 
import sys 
sys.path.append("../../Find_App") 
sys.path.append("../")
sys.path.append("../sql_script")
from savejob_jobid_sql import save_main
#import threading
import multiprocessing
import os
import re
import time
from classify_IOmode import search_corehour
from insert_sql_IOdata_auto import get_largest_jobid, get_jobid_larger_32
from insert_sql_IOmode_history import get_column_name

#startid = 42197397
#table = 'JOB_log'

def create_result_files():
    my_pid_all = multiprocessing.current_process()
    my_pid = re.sub("\D", "", str(my_pid_all))

    title_IO = time_stamp +"_IOmode/thread_"+str(my_pid)
    title = time_stamp +"/thread_"+str(my_pid)
#    print title_IO 
#    print title 
    os.mkdir("../../results_job_data/collect_data/" + title)

    all_file = file('../../results_job_data/collect_data/'+ title+'/all.csv','wb')
    writer_all = csv.writer(all_file)
    
    file_name_r = file('../../results_job_data/collect_data/'+ title+'/IO_phase_r.csv','wb')
    writer_file_name_r = csv.writer(file_name_r)

    file_name_w = file('../../results_job_data/collect_data/'+ title+'/IO_phase_w.csv','wb')
    writer_file_name_w  = csv.writer(file_name_w)
   
    jobid_abnormal_file = file('../../results_job_data/collect_data/'+ title+'/jobid_abnormal.csv','wb')
    writer_jobid_abnormal = csv.writer(jobid_abnormal_file)

def create_result_files_pid(my_pid):

    title_IO = time_stamp +"_IOmode/thread_"+str(my_pid)
    title = time_stamp +"/thread_"+str(my_pid)
    
    os.mkdir("../../results_job_data/collect_data/" + title)
    
    all_file = file('../../results_job_data/collect_data/'+ title+'/all.csv','wb')
    writer_all = csv.writer(all_file)
    
    file_name_r = file('../../results_job_data/collect_data/'+ title+'/IO_phase_r.csv','wb')
    writer_file_name_r = csv.writer(file_name_r)

    file_name_w = file('../../results_job_data/collect_data/'+ title+'/IO_phase_w.csv','wb')
    writer_file_name_w  = csv.writer(file_name_w)
   
    jobid_abnormal_file = file('../../results_job_data/collect_data/'+ title+'/jobid_abnormal.csv','wb')
    writer_jobid_abnormal = csv.writer(jobid_abnormal_file)

def compute_result(jobID, column):
    print jobID
    
    my_pid_all = multiprocessing.current_process()
    my_pid = re.sub("\D", "", str(my_pid_all))

    title_IO = time_stamp +"_IOmode/thread_"+str(my_pid)
    title = time_stamp +"/thread_"+str(my_pid)
    print title
    print title_IO
    try:
        if(not os.path.exists('../../results_job_data/collect_data/'+title)):
            create_result_files_pid(my_pid)
            print title
    except Exception as e:
        print e
   
    jobid_abnormal_file = file('../../results_job_data/collect_data/'+ title+'/jobid_abnormal.csv','ab')
    writer_jobid_abnormal = csv.writer(jobid_abnormal_file)

    try:
        save_main(jobID, title, column)
    except Exception as e:
        print e
        writer_jobid_abnormal.writerow([str(jobID)])

if __name__ == "__main__":
    if (len(sys.argv)<2):
        print "Please give enough parameters like (time_stamp thread_count)!!!!!!"
        sys.exit()
    else:
        time_stamp = sys.argv[1]  
        thread_count = int(sys.argv[2])
        startid = sys.argv[3]
        table = sys.argv[4]

    print thread_count
    
    p = multiprocessing.Pool(thread_count)
    for i in range(0,thread_count):
        try:
            p.apply_async(create_result_files, args=())
            time.sleep(2)
        except Exception as e:
            print e
    print 'All file created!'

    max_jobid = get_largest_jobid(table)
    jobid = get_jobid_larger_32(startid, max_jobid, table)
    column = column = get_column_name('JOB_IOMODE_WRITE')
    for i in range(len(jobid)):
         try:
             p.apply_async(compute_result, args=(jobid[i], column))
         except Exception as e:
             print e
    print "All threads have completed computing!"
#
    p.close()
    p.join()

    print "All threads are closed!"
