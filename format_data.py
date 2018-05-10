#:!/usr/bin/python
import csv
import sys
from read_from_source import read_runtime, read_CNC, read_big_job, read_CNC_runtime_corehour
import scipy.io as sio  
import matplotlib.pyplot as plt

IOBW_file_name = '../../results_job_data/collect_data/all_data/IOBW.csv'
IOPS_file_name = '../../results_job_data/collect_data/all_data/IOPS.csv'
MDS_file_name = '../../results_job_data/collect_data/all_data/MDS.csv'

IOPS_BW_MDS_file_name = '../../results_job_data/collect_data/format_data/IOPS_BW_MDS_file.csv'

def draw_2d(x, y):

#	plt.xlim( 0, 150)
#	plt.ylim( 0, 60)
	
	plt.xticks(fontsize=20)
	plt.yticks(fontsize=20)
	plt.plot(x, y, '.')
	#plt.plot([0, 35], [average, average], 'r-')
	#plt.xlabel('Runtime (day)', fontsize = 20)
	#plt.ylabel('IO_Time/Runtime ', fontsize = 20)
	plt.xlabel('CNC', fontsize = 20)
	#plt.ylabel('IOPS ', fontsize = 20)
	plt.ylabel('file_count', fontsize = 20)
	label = ["Read", "WRITE"]
	plt.legend(label, loc = 1, ncol = 1)
	plt.show()

def read_jobID_IOBW_R_W():
    
    reader = csv.reader(open(IOBW_file_name , 'rU'), dialect=csv.excel_tab)
    next(reader)

    sum_READ = 0.0
    sum_WRITE = 0.0
    count_rw = 0.0
    

    jobID = []
    average_r = []
    average_w = []
    IO_time = []
    
    for line in reader:
        x = str(line).split(',')
        if(len(x) == 11):
            try:
                jobID_tmp = x[1].strip()
                sum_READ = float(x[4].strip())
                sum_WRITE = float(x[5].strip())
                count_r = float(x[6].strip())
                count_w = float(x[7].strip())
                count_rw = int(x[8].strip())
                average_r_tmp = 0.0
                average_w_tmp = 0.0
                if(abs(count_r) > sys.float_info.epsilon):
                    average_r_tmp = sum_READ/ count_r
                if(abs(count_w) > sys.float_info.epsilon):
                    average_w_tmp = sum_WRITE/ count_w
                jobID.append(jobID_tmp)
                average_r.append(average_r_tmp)
                average_w.append(average_w_tmp)
                IO_time.append(count_rw)
            except:
                continue

    return jobID, IO_time, average_r, average_w

def read_IOPS_R_W():
    
    reader = csv.reader(open(IOPS_file_name , 'rU'), dialect=csv.excel_tab)
    next(reader)

    sum_READ = 0.0
    sum_WRITE = 0.0
    count_rw = 0.0
    

    average_r = []
    average_w = []
    
    for line in reader:
        x = str(line).split(',')
        if(len(x) == 12):
            try:
                sum_READ = float(x[4].strip())
                sum_WRITE = float(x[5].strip())
                count_r = float(x[6].strip())
                count_w = float(x[7].strip())
                average_r_tmp = 0.0
                average_w_tmp = 0.0
                if(abs(count_r) > sys.float_info.epsilon):
                    average_r_tmp = sum_READ/ count_r
                if(abs(count_w) > sys.float_info.epsilon):
                    average_w_tmp = sum_WRITE/ count_w
                average_r.append(average_r_tmp)
                average_w.append(average_w_tmp)
            except:
                continue

    return average_r, average_w

def read_MDS_R_W():
    
    reader = csv.reader(open(MDS_file_name , 'rU'), dialect=csv.excel_tab)
    next(reader)

    sum_READ = 0.0
    sum_WRITE = 0.0

    average_r = []
    average_w = []
    file_count = []
    
    for line in reader:
        x = str(line).split(',')
        if(len(x) == 12):
            try:
                sum_READ = float(x[4].strip())
                sum_WRITE = float(x[5].strip())
                count_r = float(x[6].strip())
                count_w = float(x[7].strip())
                file_count_tmp = int(x[11][:-2].strip())
                average_r_tmp = 0.0
                average_w_tmp = 0.0
                if(abs(count_r) > sys.float_info.epsilon):
                    average_r_tmp = sum_READ/ count_r
                if(abs(count_w) > sys.float_info.epsilon):
                    average_w_tmp = sum_WRITE/ count_w
                average_r.append(average_r_tmp)
                average_w.append(average_w_tmp)
                file_count.append(file_count_tmp)
            except:
                continue

    return average_r, average_w, file_count

def search_for_info(jobID, jobID_read, compute_node_count_read, run_time_read, corehour_read):
    
    compute_node_count = []
    run_time = []
    corehour = []
    
    for i in xrange(len(jobID)):
	for j in xrange(len(jobID_read)):
	    if(jobID[i] == jobID_read[j]):
    		compute_node_count.append(compute_node_count_read[j])
    		run_time.append(run_time_read[j])
    		corehour.append(corehour_read[j])
		break
    return compute_node_count, run_time, corehour

def save_format_data(jobID, compute_node_count, run_time, corehour, IO_time, IOBW_r, IOBW_w, \
IOPS_r, IOPS_w, MDS_o, MDS_c, file_count):
    
    f = open(IOPS_BW_MDS_file_name, "wb")
    for i in range(len(jobID)):
        write_row = "%s %d %f %f %f %f %f %f %f %f %f %d\n"%(jobID[i], compute_node_count[i], \
	    run_time[i], corehour[i], IO_time[i], IOBW_r[i], IOBW_w[i], IOPS_r[i], IOPS_w[i], \
	    MDS_o[i], MDS_c[i], file_count[i])
        f.write(write_row)    

def format_data():

    jobID, IO_time, IOBW_r, IOBW_w = read_jobID_IOBW_R_W()
    IOPS_r, IOPS_w = read_IOPS_R_W()
    MDS_o, MDS_c, file_count = read_MDS_R_W()

    print len(MDS_o)
    print len(MDS_o)
    jobID_read, compute_node_count_read, run_time_read, corehour_read = read_CNC_runtime_corehour()
    compute_node_count, run_time, corehour = search_for_info\
    (jobID, jobID_read, compute_node_count_read, run_time_read, corehour_read)

    save_format_data(jobID, compute_node_count, run_time, corehour, IO_time, IOBW_r, IOBW_w, \
    IOPS_r, IOPS_w, MDS_o, MDS_c, file_count)

def read_format_data():
    CNC = []
    file_count = []
    reader = csv.reader(open(IOPS_BW_MDS_file_name, 'rU'), dialect=csv.excel_tab)
    for line in reader:
        x = str(line).split(' ')
        CNC.append(int(x[1]))
        file_count.append(float(x[11][:-2].strip()))
#        print x[11][:-2].strip()
    print len(CNC)
    return CNC, file_count

if __name__ == "__main__":
    CNC,file_count = read_format_data()
    draw_2d(CNC, file_count)
    #format_data()
