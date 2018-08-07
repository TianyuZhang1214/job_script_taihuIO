#!/usr/bin/python
import csv 
import sys 
sys.path.append("../../Find_App") 
sys.path.append("../")
from savejob_jobid_all_in import save_main
#import threading
import multiprocessing
import os
import re
import time
from classify_IOmode import search_corehour


start_file_name = '../../source_job_data/JOB_log_41027800.csv'
continue_file_name = '../../source_job_data/JOB_log_41027800.csv'

test_file_name = 'test_csv/wrf_8.exe.csv'
abnormal_file_name = '../../results_job_data/collect_data/20180116102430/jobid_abnormal.csv'

all_pid = []

def read_continue(res_pos):
    reader = csv.reader(open(continue_file_name, 'rU'), dialect=csv.excel_tab)
    for i in range(res_pos):
        next(reader)

    program_name = []
    jobID = []
    start_time = []
    end_time = []
    corehour = []

    for line in reader:
        if(len(line) >= 16):
            if(line[0].strip()):
                jobID.append(line[0].strip())
            if(line[6].strip()):
                corehour.append(line[6].strip())
            if(line[8].strip()):
                start_time.append(line[8].strip())
            if(line[9].strip()):
                end_time.append(line[9].strip())
            if(line[14].strip()):
                    program_name.append(line[14].strip())

    return start_time, end_time, jobID, program_name, corehour

def read_start():
    reader = csv.reader(open(start_file_name, 'rU'), dialect=csv.excel_tab)
    program_name = []
    jobID = []
    start_time = []
    end_time = []
    corehour = []

    for line in reader:
        if(len(line) >= 16):
            if(line[0].strip()):
                jobID.append(line[0].strip())
            if(line[6].strip()):
                corehour.append(line[6].strip())
            if(line[8].strip()):
                start_time.append(line[8].strip())
            if(line[9].strip()):
                end_time.append(line[9].strip())
            if(line[14].strip()):
                    program_name.append(line[14].strip())
    return start_time, end_time, jobID, program_name, corehour 

def read_abnormal(res_pos):
    program_name = [] 
    jobID = []
    start_time = []
    end_time = []
    reader = csv.reader(open(abnormal_file_name, 'rU'), dialect=csv.excel_tab)

    for i in range(res_pos):
        next(reader)
    for line in reader:
#        print (line[0].split(","))[0]
#        print (line[0].split(","))[1]
#        print (line[0].split(","))[2]
#        print (line[0].split(","))[3]
        if (len(line[0].split(",")) >= 4):
            if((line[0].split(","))[0].strip()):
                jobID.append((line[0].split(","))[0].strip())
    
    return program_name, start_time, end_time, jobID

def print_test(jobID):
    my_pid_all = multiprocessing.current_process()
    my_pid = re.sub("\D", "", str(my_pid_all))
    print 'my_pid : ', my_pid, jobID 

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

def compute_result(jobID):
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
        save_main(jobID, title)
    except Exception as e:
        print e
        writer_jobid_abnormal.writerow([str(jobID)])

if __name__ == "__main__":
    global time_stamp
    global thread_count 
    res_pos = 0

    if (len(sys.argv)<3):
        print "Please give enough parameters like (time_stamp , start_mode \
        and thread_count)!!!!!!"
        sys.exit()
    else:
        time_stamp = sys.argv[1]  
        start_mode = sys.argv[2]
        thread_count = int(sys.argv[3])
        if(start_mode == "continue" or start_mode == "abnormal"):
            if(len(sys.argv)<4):
                print "Please give enough parameters \
                like (time_stamp ,start_mode, thread_count and res_pos)!!!!!!"
                sys.exit()
            else:
                res_pos = int(sys.argv[4])
        else:
            res_pos = 0

    program_name = []
    start_time = []
    end_time = []
    jobID = []
    corehour = []

    global count_job_limit 
    count_job_limit = 3

    if(start_mode == 'start'):
        start_time, end_time, jobID, program_name, corehour = read_start()
    elif(start_mode == 'continue'):
        start_time, end_time, jobID, program_name, corehour= read_continue(res_pos)
    elif(start_mode == 'abnormal'):
        program_name, start_time, end_time, jobID = read_abnormal(res_pos)
        corehour = search_corehour(jobID)
        if(len(corehour) < len(program_name)):
            print "Error: source_file_name in classify_iomode.py is wrong!"
            sys.exit()
    elif(start_mode == 'test'):
        program_name_in = "wrf.exe"
        test_file = file(test_file_name,'r')
        res_pos = 0
        program_name, start_time, end_time, jobID = read_no_program(program_name_in, test_file, res_pos)
    else:
        print "Please input start mode (start or continue)!!!!!!!!!!!"
 

    print thread_count

    p = multiprocessing.Pool(thread_count)
    for i in range(0,thread_count):
        try:
#	    print '1111111111'
            p.apply_async(create_result_files, args=())
#	    print '2222222222'
            time.sleep(2)
        except Exception as e:
            print e
    print 'All file created!'
    
    for i in range(len(jobID)):
        try:
            p.apply_async(compute_result, args=(jobID[i], ))
        except Exception as e:
            print e
    print "All threads have completed computing!"
#
    p.close()
    p.join()

    print "All threads are closed!"
