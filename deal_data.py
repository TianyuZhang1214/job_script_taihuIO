#:!/usr/bin/python
import csv
import sys
from read_from_source import read_runtime, read_CNC, read_big_job

IOBW_file_name = '/home/export/mount_test/swstorage/results_job_da/home/export/mount_test/swstoragellect_da/home/export/mount_test/swstoragel_da/home/export/mount_test/swstorageBW.csv'
IOPS_file_name = '/home/export/mount_test/swstorage/results_job_data/collect_data/all_data/IOPS.csv'
MDS_file_name = '/home/export/mount_test/swstorage/results_job_data/collect_data/all_data/MDS.csv'

results_Med_file_name = '/home/export/mount_test/swstorage/results_job_data/result_data/IO_mode/Med_result.csv'

draw_compute_node_count_file = '/home/export/mount_test/swstorage/results_job_data/draw_csv/runtime.csv'
draw_all_file_r = '/home/export/mount_test/swstorage/results_job_data/draw_csv/IOBW_IOPS_MDS_r.csv'
draw_all_file_w = '/home/export/mount_test/swstorage/results_job_data/draw_csv/IOBW_IOPS_MDS_w.csv'
draw_IOBW_IOPS_file_r = '/home/export/mount_test/swstorage/results_job_data/draw_csv/IOBW_IOPS_r.csv'
draw_IOBW_IOPS_file_w = '/home/export/mount_test/swstorage/results_job_data/draw_csv/IOBW_IOPS_w.csv'

draw_CNC_file = '/home/export/mount_test/swstorage/results_job_data/draw_csv/CNC.csv'
draw_CNC_IOBW_r_file = '/home/export/mount_test/swstorage/results_job_data/draw_csv/CNC_IOBW_r.csv'
draw_CNC_IOBW_w_file = '/home/export/mount_test/swstorage/results_job_data/draw_csv/CNC_IOBW_w.csv'
draw_CNC_IOtime_file = '/home/export/mount_test/swstorage/results_job_data/draw_csv/CNC_IOtime.csv'

draw_corehour_file = '/home/export/mount_test/swstorage/results_job_data/draw_csv/IOtime_corehour.csv'
draw_Med_corehour_file = '/home/export/mount_test/swstorage/results_job_data/draw_csv/Med_IOtime_corehour.csv'

draw_CNC_sum_IOBW_file = '/home/export/mount_test/swstorage/results_job_data/draw_csv/CNC_small_sum_IOBW.csv'
draw_CNC_IOBW_file = '/home/export/mount_test/swstorage/results_job_data/draw_csv/CNC_IOBW.csv'

draw_big_job_undone_file = '/home/export/mount_test/swstorage/results_job_data/draw_csv/big_job_undone.csv'


def read_Med_results():

    program_name = []
    jobID = []
    corehour = []
    IOBW_r = []
    IOBW_w = []
    PE_r = []
    PE_w = []
    file_count = []

    reader = csv.reader(open(results_Med_file_name, 'rU'), dialect=csv.excel_tab)
    next(reader)

    for line in reader:
        x = str(line).split(',')
#        for t in xrange(len(x)):
#            print x[t]
        if(len(x) == 8):
            program_name.append((x[0][2:]).strip())
            jobID.append(x[1].strip())
            corehour.append(float(x[2].strip()))
            IOBW_r.append(float(x[3].strip()))
            IOBW_w.append(float(x[4].strip()))
            PE_r.append(float(x[5].strip()))
            PE_w.append(float(x[6].strip()))
            file_count.append(float(x[7][:-2].strip()))

    return program_name, jobID, corehour, IOBW_r , IOBW_w , PE_r, PE_w, file_count


def search_big_job_from_result():

    jobID, CNC = read_2_value_data(draw_CNC_file)

    jobID_read, sum_w, average_w = read_sum_IOBW_R_W()
    
    print len(jobID) 
    print len(jobID_read) 
    
    jobID_big = [] 
    CNC_big = [] 
    sum_w_big = [] 
    average_w_big = [] 
    
    for i in xrange(len(jobID)):
        if(CNC[i] >= 0):
            for j in xrange(len(jobID_read)):
                if (jobID[i] == float(jobID_read[j])):
                    jobID_big.append(jobID[i])
                    CNC_big.append(CNC[i])
                    sum_w_big.append(sum_w[j])
                    average_w_big.append(average_w[j])
                    break

    print len(jobID_big) 
    
    f = open(draw_CNC_IOBW_file, "wb")
    for i in range(len(jobID_big)):
        write_row = "%s %d %f %f\n"%(jobID_big[i], CNC_big[i], \
        sum_w_big[i], average_w_big[i])
        f.write(write_row)

def read_big_job_from_source():
    jobID = []
    start_time = []
    end_time = []
    CNC = []
    run_time = []

    f = open(draw_big_job_undone_file, "r")
    for line in open(draw_big_job_undone_file):
	x = line.split()
	jobID.append(x[0])
        start_time.append(x[1])
        end_time.append(x[2])
        CNC.append(x[3])
        run_time.append(x[4])
	


def search_big_job_from_source():
    jobID, start_time, end_time, CNC, run_time = read_big_job()

    jobID_read, sum_w, average_w = read_sum_IOBW_R_W()

    jobID_done = set(jobID_read)
    
    print len(jobID) 
    f = open(draw_big_job_undone_file, "wb")
    for i in range(len(jobID)):
        if(jobID[i] not in jobID_done):
            write_row = "%s %s %s %d %d\n"%(jobID[i], start_time[i], end_time[i], \
            CNC[i], run_time[i])
            f.write(write_row)



def read_sum_IOBW_R_W():
    
    reader = csv.reader(open(IOBW_file_name , 'rU'), dialect=csv.excel_tab)
    next(reader)

    jobID = []
    sum_w = []
    average_w = []
    
    for line in reader:
        x = str(line).split(',')
        if(len(x) == 11):
            try:
                jobID_tmp = x[1].strip() 
                sum_w_tmp = float(x[5].strip())
                average_w_tmp = float(x[10][:-2].strip())
                jobID.append(jobID_tmp)
                sum_w.append(sum_w_tmp)
                average_w.append(average_w_tmp)
            except:
                continue
    
    return jobID, sum_w, average_w



def read_sum_IOBW_R_W():
    
    reader = csv.reader(open(IOBW_file_name , 'rU'), dialect=csv.excel_tab)
    next(reader)

    jobID = []
    sum_w = []
    average_w = []
    
    for line in reader:
        x = str(line).split(',')
        if(len(x) == 11):
            try:
                jobID_tmp = x[1].strip() 
                sum_w_tmp = float(x[5].strip())
                average_w_tmp = float(x[10][:-2].strip())
                jobID.append(jobID_tmp)
                sum_w.append(sum_w_tmp)
                average_w.append(average_w_tmp)
            except:
                continue
    
    return jobID, sum_w, average_w

def save_IOBW_IOPS(IOBW, IOPS):
    
    csvfile=file('../../results_job_data/collect_data/all_data/draw.csv','wb')
    writer=csv.writer(csvfile)
    for xu in xrange(len(IOBW)):
        writer.writerow([IOBW[xu], IOPS[xu]])
    csvfile.close()

def save_all(IOBW, IOPS, MDS):
    
    csvfile=file('collect_data/merge_data/draw_all.csv','wb')
    writer=csv.writer(csvfile)
    for xu in xrange(len(IOBW)):
        writer.writerow([IOBW[xu], IOPS[xu], MDS[xu]])
    csvfile.close()

def get_runtime(jobID_read, jobID, run_time_read):

    run_time = [0] * len(jobID)    
    
    for i in xrange(len(jobID)):
        for j in xrange(len(jobID_read)):
            if (jobID[i] == jobID_read[j]):
                run_time[i] = run_time_read[j]
                break

    return run_time

def get_CNC(jobID_read, jobID, compute_node_count_read):

    compute_node_count = [0] * len(jobID)    
    
    for i in xrange(len(jobID)):
        for j in xrange(len(jobID_read)):
            if (jobID[i] == jobID_read[j]):
                compute_node_count[i] = compute_node_count_read[j]
                break

    return compute_node_count

def read_2_value_data(file_name):

    data_1 = [] 
    data_2 = [] 
    
    f = open(file_name, 'r')
    for line in open(file_name):
        line = f.readline()
        array = line.split()
        x = float(array[0])
        y = float(array[1])
        data_1.append(x)
        data_2.append(y)

    return data_1, data_2

def read_3_value_data(file_name):

    data_1 = [] 
    data_2 = [] 
    data_3 = [] 
    
    f = open(file_name, 'r')
    for line in open(file_name):
        line = f.readline()
        array = line.split()
        x = float(array[0])
        y = float(array[1])
        z = float(array[2])
        data_1.append(x)
        data_2.append(y)
        data_3.append(z)

    return data_1, data_2, data_3

def save_2_value_data_int(data_1, data_2, file_name):

     f = open(file_name, "wb")
     print len(data_1)
     print len(data_2)
     for i in range(len(data_1)):
         write_row = "%s %d\n"%(data_1[i], data_2[i])
#         write_row = "%f %f"%(IOtime/runtime[i], runtime[i])
         f.write(write_row)


def save_2_value_data(data_1, data_2, file_name):

     f = open(file_name, "wb")
     print len(data_1)
     print len(data_2)
     for i in range(len(data_1)):
         write_row = "%f %f\n"%(data_1[i], data_2[i])
#         write_row = "%f %f"%(IOtime/runtime[i], runtime[i])
         f.write(write_row)

def save_2_value_data_1(data_1, data_2, file_name):

     f = open(file_name, "wb")
     print len(data_1)
     print len(data_2)
     for i in range(len(data_1)):
         write_row = "%f %f\n"%(data_1[i]/data_2[i], data_2[i])
#         write_row = "%f %f"%(IOtime/runtime[i], runtime[i])
         f.write(write_row)

def save_2_value_data_corehour(data_1, data_2, data_3, file_name):

     f = open(file_name, "wb")
     print len(data_1)
     print len(data_2)
     print len(data_3)
     for i in range(len(data_1)):
         write_row = "%f %f\n"%(data_1[i]/data_2[i], data_3[i])
         f.write(write_row)

def save_Med_IOtime_corehour(jobID, IO_time, run_time, corehour):
     f = open(draw_Med_corehour_file, "wb")
     print len(jobID)
     print len(IO_time)
     print len(run_time)
     print len(corehour)
     for i in range(len(jobID)):
         write_row = "%s %f %f %f\n"%(str(jobID[i]), IO_time[i], run_time[i], corehour[i])
         f.write(write_row)

def read_Med_IOtime_corehour():
    f = open(draw_Med_corehour_file, "r")
    jobID = []
    IO_time = []
    run_time = []
    corehour = []

    for line in open(draw_Med_corehour_file):
        line = f.readline()
        array = line.split()
        jobID.append(float(array[0]))
        IO_time.append(float(array[1]))
        run_time.append(float(array[2]))
        corehour.append(float(array[3]))
    return jobID, IO_time, run_time, corehour


def save_3_value_data(data_1, data_2, data_3, file_name):

     f = open(file_name, "wb")
     print len(data_1)
     print len(data_2)
     print len(data_3)
     for i in range(len(data_1)):
         write_row = "%f %f %f\n"%(data_1[i], data_2[i], data_3[i],)
         f.write(write_row)

def save_3_value_data_str(data_1, data_2, data_3, file_name):

     f = open(file_name, "wb")
     print len(data_1)
     print len(data_2)
     print len(data_3)
     for i in range(len(data_1)):
         write_row = "%s %s %s\n"%(data_1[i], data_2[i], data_3[i],)
         f.write(write_row)

def read_3_value_data_str(file_name):

    data_1 = [] 
    data_2 = [] 
    data_3 = [] 
    
    f = open(file_name, 'r')
    for line in open(file_name):
        line = f.readline()
        array = line.split()
        x = array[0]
        y = array[1]
        z = array[2]
        data_1.append(x)
        data_2.append(y)
        data_3.append(z)

    return data_1, data_2, data_3

def save_rumtime(run_time):
    csvfile=file('collect_data/merge_data/rum_time.csv','wb')
    writer=csv.writer(csvfile)
    for xu in xrange(len(run_time)):
        writer.writerow([run_time[xu]])
    csvfile.close()

def read_IO_time():
    
    reader = csv.reader(open(IOBW_file_name , 'rU'), dialect=csv.excel_tab)
    next(reader)
    
    IO_time = []
    jobID = []
    
    for line in reader:
        x = str(line).split(',')
        if(len(x) == 11):
            try:
                count_rw = float(x[8].strip())
                jobID_tmp = x[1].strip()
                IO_time.append(count_rw)
                jobID.append(jobID_tmp)
            except:
                continue

    return jobID, IO_time

def read_unique_file_count():
    
    reader = csv.reader(open(MDS_file_name , 'rU'), dialect=csv.excel_tab)
    
    jobID = []
    file_count = []
    
    for line in reader:
        x = str(line).split(',')
        if(len(x) == 12):
            try:
                file_count_tmp = int(x[11][:-2].strip())
                file_count.append(file_count_tmp)
                jobID_tmp = x[1].strip()
                jobID.append(jobID_tmp)
            except:
                continue

    return jobID, file_count

def read_jobID_IOBW_R_W():
    
    reader = csv.reader(open(IOBW_file_name , 'rU'), dialect=csv.excel_tab)
    next(reader)

    sum_READ = 0.0
    sum_WRITE = 0.0
    count_rw = 0.0
    

    jobID = []
    average_r = []
    average_w = []
    
    for line in reader:
        x = str(line).split(',')
        if(len(x) == 11):
            try:
                jobID_tmp = x[1].strip()
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
                jobID.append(jobID_tmp)
                average_r.append(average_r_tmp)
                average_w.append(average_w_tmp)
            except:
                continue

    return jobID, average_r, average_w


def read_IOBW_R_W():
    
    reader = csv.reader(open(IOBW_file_name , 'rU'), dialect=csv.excel_tab)
    next(reader)

    sum_READ = 0.0
    sum_WRITE = 0.0
    count_rw = 0.0
    
    average_r = []
    average_w = []
    
    for line in reader:
        x = str(line).split(',')
        if(len(x) == 11):
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
        if(len(x) >= 11):
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
    
    for line in reader:
        x = str(line).split(',')
        if(len(x) == 11):
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

def read_IOBW():
    
    reader = csv.reader(open(IOBW_file_name , 'rU'), dialect=csv.excel_tab)
    next(reader)

    sum_READ = 0.0
    sum_WRITE = 0.0
    count_rw = 0.0

    average_rw = []
    for line in reader:
        x = str(line).split(',')
        if(len(x) == 11):
            try:
                sum_READ = float(x[4].strip())
                sum_WRITE = float(x[5].strip())
                count_rw = float(x[8].strip())
                average_rw_tmp = 0.0
                if(abs(count_rw) > sys.float_info.epsilon):
                    average_rw_tmp = (sum_READ + sum_WRITE)/ count_rw
                average_rw.append(average_rw_tmp)
            except:
                continue

    return average_rw

def read_IOPS():
    
    reader = csv.reader(open(IOPS_file_name , 'rU'), dialect=csv.excel_tab)
    next(reader)

    sum_READ = 0.0
    sum_WRITE = 0.0
    count_rw = 0.0
    

    average_rw = []
    for line in reader:
#        print line
        x = str(line).split(',')
#        for t in xrange(len(x)):
#            print x[t]
        if(len(x) >= 11):
            try:
                sum_READ = float(x[4].strip())
                sum_WRITE = float(x[5].strip())
                count_rw = float(x[9].strip())
                average_rw_tmp = 0.0
                if(abs(count_rw) > sys.float_info.epsilon):
                    average_rw_tmp = (sum_READ + sum_WRITE)/ count_rw
                average_rw.append(average_rw_tmp)
            except:
                continue

    return average_rw

def read_MDS():
    
    reader = csv.reader(open(MDS_file_name , 'rU'), dialect=csv.excel_tab)
    next(reader)

    sum_READ = 0.0
    sum_WRITE = 0.0
    count_rw = 0.0
    

    average_rw = []
    for line in reader:
        x = str(line).split(',')
#        for t in xrange(len(x)):
#            print x[t]
        if(len(x) == 11):
            try:
                sum_READ = float(x[4].strip())
                sum_WRITE = float(x[5].strip())
                count_rw = float(x[8].strip())
                average_rw_tmp = 0.0
                if(abs(count_rw) > sys.float_info.epsilon):
                    average_rw_tmp = (sum_READ + sum_WRITE)/ count_rw
                average_rw.append(average_rw_tmp)
            except:
                continue

    return average_rw

def read_all():
    average_iobw = read_IOBW()
    average_iops = read_IOPS()
    average_mds = read_MDS()

    return average_iobw, average_iops, average_mds  

def read_all_R_W():
    average_iobw_r, average_iobw_w = read_IOBW_R_W()
    average_iops_r, average_iops_w = read_IOPS_R_W()
    average_mds_r, average_mds_w = read_MDS_R_W()

    return average_iobw_r, average_iobw_w, average_iops_r, \
    average_iops_w,  average_mds_r, average_mds_w

def read_all_1024_R_W():
    average_iobw_r, average_iobw_w, average_iops_r, \
    average_iops_w,  average_mds_r, average_mds_w = read_all_R_W()

    for i in xrange(len(average_iops)):
        average_iobw_r[i] /= 1024
        average_iobw_w[i] /= 1024
        average_iops_r[i] /= 1000
        average_iops_w[i] /= 1000
        average_mds_r[i] /= 1000
        average_mds_w[i] /= 1000

    return average_iobw_r, average_iobw_w, average_iops_r, \
    average_iops_w,  average_mds_r, average_mds_w

def read_all_1024():
    average_iobw, average_iops, average_mds =  read_all()

    for i in xrange(len(average_iops)):
        average_iobw[i] /= 1024
        average_iops[i] /= 1000
        average_mds[i] /= 1000
    return average_iobw, average_iops, average_mds  

def delete_abnormal_IOPS():

    average_iobw, average_iops, average_mds =  read_all_1024()

    for i in xrange(len(average_iops)):
        if(average_iops[i] > 600.0):
            average_iops[i] = 0.0
            average_iobw[i] = 0.0
            average_mds[i] = 0.0
        elif(average_mds[i] > 60.0):
            average_iops[i] = 0.0
            average_iobw[i] = 0.0
            average_mds[i] = 0.0

    return average_iobw, average_iops, average_mds  

def get_request_size():

    average_iobw, average_iops, average_mds =  read_all_1024()

    for i in xrange(len(average_iops)):
        if(average_iops[i] > 600.0):
            average_iops[i] = 0.0
            average_iobw[i] = 0.0
            average_mds[i] = 0.0
        elif(average_mds[i] > 60.0):
            average_iops[i] = 0.0
            average_iobw[i] = 0.0
            average_mds[i] = 0.0

    return average_iobw, average_iops, average_mds  

def compute_IO_time_runtime():

    jobID_read, run_time_read = read_runtime()
    print 'read_runtime completed!'
    jobID, IO_time = read_IO_time()
    print 'read_IO_time completed!'
    run_time = get_runtime(jobID_read, jobID, run_time_read)
    print 'get_runtime completed!'
    save_2_value_data_1(IO_time, run_time, draw_run_time_file)
    print 'save_runtime completed!'


def compute_IO_time_corehour():

    program_name, jobID_corehour, corehour_read, IOBW_r , IOBW_w , PE_r, PE_w, file_count \
    = read_Med_results()
    print 'read_Med_results completed!'
    
    

    jobID_read, run_time_read = read_runtime()
    print 'read_runtime completed!'
    
    jobID, IO_time = read_IO_time()
    print 'read_IO_time completed!'
    
#    save_Med_IOtime_corehour(jobID, IO_time, run_time_read, corehour_read)
    run_time = get_runtime(jobID_read, jobID, run_time_read)
    print 'get_runtime completed!'
#    
    corehour = get_runtime(jobID_corehour, jobID, corehour_read)
    print 'get corehour completed!'
    
    save_Med_IOtime_corehour(jobID, IO_time, run_time, corehour)
#    save_2_value_data_corehour(IO_time, run_time, corehour, draw_corehour_file)
    print 'save_corehour completed!'

def save_R_W():
    IOBW_r, IOBW_w = read_IOBW_R_W()
    print "read_IOBW_R_W completed"
    IOPS_r, IOPS_w = read_IOPS_R_W()
    print "read_IOPS_R_W completed"
    MDS_r, MDS_w = read_MDS_R_W()
    print "read_MDS_R_W completed"

    save_2_value_data(IOBW_r, IOPS_r, draw_IOBW_IOPS_file_r)
    print "save_2_value_data_r completed!"
    save_2_value_data(IOBW_w, IOPS_w, draw_IOBW_IOPS_file_w)
    print "save_2_value_data_w completed!"
    save_3_value_data(IOBW_r, IOPS_r, MDS_r, draw_all_file_r)
    print "save_3_value_data_r completed!"
    save_3_value_data(IOBW_w, IOPS_w, MDS_w, draw_all_file_w)
    print "save_3_value_data_w completed!"

def deal_abnormal_MDS():

    jobID, IO_time = read_IO_time()

    MDS_r, MDS_w = read_MDS_R_W()
    print "read_MDS_R_W completed"

    for i in xrange(len(MDS_r)):
        if (MDS_r[i] > 50000):
            print i
            print MDS_r[i]
            print jobID[i]



def search_by_runtime():
    
    jobId, IO_time, run_time, corehour = read_Med_IOtime_corehour()
    IO_time.sort()
    run_time.sort()
    corehour.sort()

    for i in xrange(len(IO_time) - 5, len(IO_time)):
	print 'IO_time[' + str(i) + ']: ' ,  IO_time[i]
	print 'run_time[' + str(i) + ']: ' , run_time[i]
	print 'corehour[' + str(i) + ']: ' , corehour[i]

#    print 'max(IO_time): ', max(IO_time)
#    print 'max(run_time): ', max(run_time)
#    print 'max(corehour): ', max(corehour)

def read_IOBW_CNC():

    jobID_IOtime, IO_time = read_IO_time()
    jobID, average_r, average_w = read_jobID_IOBW_R_W()
    jobID_read, compute_node_count_read = read_CNC()
    compute_node_count = get_CNC(jobID_read, jobID, compute_node_count_read)
    
    save_2_value_data_int(jobID, compute_node_count, draw_CNC_file)
    save_2_value_data(average_r, compute_node_count, draw_CNC_IOBW_r_file)
    save_2_value_data(average_w, compute_node_count, draw_CNC_IOBW_w_file)
    save_2_value_data(IO_time, compute_node_count, draw_CNC_IOtime_file)

if __name__ == "__main__":

    search_big_job_from_result()
#    search_by_runtime()
#    compute_IO_time_corehour()
#    search_big_job_from_source()

#    search_big_job()

#    read_IOBW_CNC()
#    compute_IO_time()

#    save_R_W()

#   deal_abnormal_MDS()

#    average_IOBW, average_IOPS, average_MDS = delete_abnormal_IOPS()
#    save_IOBW_IOPS(average_IOBW, average_IOPS)
#    save_all(average_IOBW, average_IOPS, average_MDS)
#    data = read_information()

#    for i in xrange(len(average_IOBW)):
#        if(average_IOBW[i] > 30 and average_IOPS[i] > 300):
#            print "average_request_size med: B/t", \
#            average_IOBW[i] *1024*1024*1024/ (average_IOPS[i] *1000)
#        if(average_IOBW[i] < 5 and average_IOPS[i] > 50):
#            print "average_request_size high: B/t", \
#            average_IOBW[i] *1024*1024*1024/ (average_IOPS[i] *1000)
#        if(average_IOBW[i] > 5 and average_IOPS[i] < 50):
#            print "average_request_size small: B/t", \
#            average_IOBW[i] *1024*1024*1024/ (average_IOPS[i] *1000)

#    print len(average_IOBW)
#    print len(average_IOPS)
#    print len(average_MDS)

#    print max(average_IOBW)
#    print max(average_IOPS)
#    print max(average_MDS)

