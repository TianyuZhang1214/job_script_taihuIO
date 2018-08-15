#!/usr/bin/python
import csv
import sys
from deal_data import read_unique_file_count

IOmode_N_N = 800.0 
IOmode_N_1 = 400.0 
IOmode_1_1 = 100.0

IOBW_file_name = '/home/export/mount_test/swstorage/results_job_data/collect_data/all_IOmode/IOBW.csv'
MDS_file_name = '/home/export/mount_test/swstorage/results_job_data/collect_data/all_IOmode/MDS.csv'
PE_r_file_name = '/home/export/mount_test/swstorage/results_job_data/collect_data/all_IOmode/PER.csv'
PE_w_file_name = '/home/export/mount_test/swstorage/results_job_data/collect_data/all_IOmode/PEW.csv'

PE_file_name = '/home/export/mount_test/swstorage/results_job_data/collect_data/all_data/maxPE.csv'

results_file_name = '/home/export/mount_test/swstorage/results_job_data/result_data/result.csv'
results_Med_file_name = '/home/export/mount_test/swstorage/results_job_data/result_data/Med_result.csv'

source_file_name = '/home/export/mount_test/swstorage/source_data/JOB_log.csv'

#IOBW_file_name = '/home/export/mount_test/swstorage/results_job_data/collect_data/all_IOmode/stage_all_IOBW.csv'
#MDS_file_name = '/home/export/mount_test/swstorage/results_job_data/collect_data/all_IOmode/stage_all_MDS.csv'
#PE_r_file_name = '/home/export/mount_test/swstorage/results_job_data/collect_data/all_IOmode/stage_all_PER.csv'
#PE_w_file_name = '/home/export/mount_test/swstorage/results_job_data/collect_data/all_IOmode/stage_all_PEW.csv'
#
#results_file_name = 'result_data/stage_all_IOmode_result.csv'
#results_Med_file_name = 'result_data/stage_all_IOmode_Med_result.csv'

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

def search_corehour(jobID):
    reader = csv.reader(open(source_file_name, 'rU'), dialect=csv.excel_tab)
    jobID_read = []
    corehour_read = []
    corehour = []

    for line in reader:
        if(len(line) >= 16):
            if(line[0].strip()):
                jobID_read.append(line[0].strip())
            if(line[6].strip()):
                corehour_read.append(float(line[6].strip()))

    
    for i in xrange(len(jobID)):
        for j in xrange(len(jobID_read)):
            if(jobID[i] == jobID_read[j]):
                corehour.append(corehour_read[j])
                break
    return corehour

def read_IOBW_corehour(IOBW_file):
    program_name = []
    jobID = []
    corehour = []
    IOBW_r = []
    IOBW_w = []

    IOBW_r_tmp = []
    IOBW_w_tmp = []

    reader = csv.reader(IOBW_file)
    next(reader)
#    print reader
    count_i = 1 
    for line in reader:
        if(count_i == 1):
#            print line
            program_name.append(line[1].strip())
            jobID.append(line[2].strip())
            corehour.append(line[3].strip())
            count_i = 2
            continue
        if(len(line) > 1):
            try:
                IOBW_r1 = float(line[1].strip())
                IOBW_w1 = float(line[2].strip())
                if(IOBW_r1 < 1000.0 and IOBW_w1 < 1000.0):
                    IOBW_r_tmp.append(IOBW_r1)
                    IOBW_w_tmp.append(IOBW_w1)
                else:
                    continue
            except:
                if("IOBW" not in line[0]):
                    program_name.append(line[1].strip())
                    jobID.append(line[2].strip())
                    corehour.append(line[3].strip())
                    if(len(IOBW_r_tmp) > 0):
                        IOBW_r_max = max(IOBW_r_tmp)
                        IOBW_w_max = max(IOBW_w_tmp)
                        IOBW_r.append(IOBW_r_max)
                        IOBW_w.append(IOBW_w_max)
                        del IOBW_r_tmp[:]
                        del IOBW_w_tmp[:]
                    else:
                        IOBW_r.append(0.0)
                        IOBW_w.append(0.0)
                else:
                    continue
    IOBW_file.close()
    if(len(IOBW_r_tmp) > 0):
        IOBW_r_max = max(IOBW_r_tmp)
        IOBW_w_max = max(IOBW_w_tmp)
        IOBW_r.append(IOBW_r_max)
        IOBW_w.append(IOBW_w_max)
        del IOBW_r_tmp[:]
        del IOBW_w_tmp[:]
    else:
        IOBW_r.append(0.0)
        IOBW_w.append(0.0)

    return program_name, jobID, IOBW_r, IOBW_w, corehour


def read_IOBW(IOBW_file):
    program_name = []
    jobID = []
    corehour = []
    IOBW_r = []
    IOBW_w = []

    IOBW_r_tmp = []
    IOBW_w_tmp = []

    reader = csv.reader(IOBW_file)
    next(reader)
#    print reader
    count_i = 1 
    for line in reader:
        if(count_i == 1):
#            print line
            program_name.append(line[1].strip())
            jobID.append(line[2].strip())
#            corehour.append(line[3].strip())
            count_i = 2
            continue
        if(len(line) > 1):
            try:
                IOBW_r1 = float(line[0].strip())
                IOBW_w1 = float(line[1].strip())
                if(IOBW_r1 < 1000.0 and IOBW_w1 < 1000.0):
                    IOBW_r_tmp.append(IOBW_r1)
                    IOBW_w_tmp.append(IOBW_w1)
                else:
                    continue
            except:
                if("IOBW" not in line[0]):
                    program_name.append(line[1].strip())
                    jobID.append(line[2].strip())
#                    corehour.append(line[3].strip())
                    if(len(IOBW_r_tmp) > 0):
                        IOBW_r_max = max(IOBW_r_tmp)
                        IOBW_w_max = max(IOBW_w_tmp)
                        IOBW_r.append(IOBW_r_max)
                        IOBW_w.append(IOBW_w_max)
                        del IOBW_r_tmp[:]
                        del IOBW_w_tmp[:]
                    else:
                        IOBW_r.append(0.0)
                        IOBW_w.append(0.0)
                else:
                    continue
    IOBW_file.close()
    if(len(IOBW_r_tmp) > 0):
        IOBW_r_max = max(IOBW_r_tmp)
        IOBW_w_max = max(IOBW_w_tmp)
        IOBW_r.append(IOBW_r_max)
        IOBW_w.append(IOBW_w_max)
        del IOBW_r_tmp[:]
        del IOBW_w_tmp[:]
    else:
        IOBW_r.append(0.0)
        IOBW_w.append(0.0)

    return program_name, jobID, IOBW_r, IOBW_w

def read_MDS(MDS_file):
    jobID = []
    MDS_o = []
    MDS_c = []

    MDS_o_tmp = []
    MDS_c_tmp = []
    reader = csv.reader(MDS_file)
    next(reader)
    count_i = 1
    for line in reader:
#        print line
        if(count_i == 1):
#            print line
#            program_name.append(line[1].strip())
#            jobID.append(line[2].strip())
            count_i = 2
            continue
        if(len(line) > 1):
            try:
                MDS_o1 = float(line[1].strip())
                MDS_c1 = float(line[2].strip())
                MDS_o_tmp.append(MDS_o1)
                MDS_c_tmp.append(MDS_c1)
            except:
                if("MDS" not in line[0]):
                    jobID.append(line[1].strip())
                    if(len(MDS_o_tmp) > 0):
                        MDS_o_max = max(MDS_o_tmp)
                        MDS_c_max = max(MDS_c_tmp)
                        MDS_o.append(MDS_o_max)
                        MDS_c.append(MDS_c_max)
                        del MDS_o_tmp[:]
                        del MDS_c_tmp[:]
                    else:
                        MDS_o.append(0.0)
                        MDS_c.append(0.0)
                else:
                    continue
    MDS_file.close()

    if(len(MDS_o_tmp) > 0):
        MDS_o_max = max(MDS_o_tmp)
        MDS_c_max = max(MDS_c_tmp)
        MDS_o.append(MDS_o_max)
        MDS_c.append(MDS_c_max)
        del MDS_o_tmp[:]
        del MDS_c_tmp[:]
    else:
        MDS_o.append(0.0)
        MDS_c.append(0.0)

    return MDS_o, MDS_c 

def read_PE_r(PE_r_file):
    jobID = []
    PE_r = []

    PE_r_tmp = []
    reader = csv.reader(PE_r_file)
    next(reader)
    count_i = 1
    for line in reader:
        if(count_i == 1):
#            print line
#            program_name.append(line[1].strip())
#            jobID.append(line[2].strip())
            count_i = 2
            continue
        if(len(line) >= 1):
            try:
                PE_r1 = float(line[1].strip())
                PE_r_tmp.append(PE_r1)
            except:
                if("PE" not in line[0]):
                    jobID.append(line[1].strip())
                    if(len(PE_r_tmp) > 0):
                        PE_r_max = max(PE_r_tmp)
                        PE_r.append(PE_r_max)
                        del PE_r_tmp[:]
                    else:
                        PE_r.append(0.0)
                else:
                    continue
    PE_r_file.close()
    if(len(PE_r_tmp) > 0):
        PE_r_max = max(PE_r_tmp)
        PE_r.append(PE_r_max)
        del PE_r_tmp[:]
    else:
        PE_r.append(0.0)
 
    return PE_r
def read_maxPE():
    
    reader = csv.reader(open(PE_file_name , 'rU'), dialect=csv.excel_tab)
    next(reader)

    jobID = []
    PE_r = []
    PE_w = []
    
    for line in reader:
        x = str(line).split(',')
        if(len(x) == 6):
            try:
                jobID_tmp = x[1].strip() 
                PE_r_tmp = float(x[4].strip())
                PE_w_tmp = float(x[5][:-2].strip())
                jobID.append(jobID_tmp)
                PE_r.append(PE_r_tmp)
                PE_w.append(PE_w_tmp)
            except:
                continue
    
    return jobID, PE_r, PE_w

def read_PE_w(PE_w_file):
    jobID = []
    PE_w = []

    PE_w_tmp = []
    reader = csv.reader(PE_w_file)
    next(reader)
    count_i = 1
    for line in reader:
        if(count_i == 1):
#            print line
#            program_name.append(line[1].strip())
#            jobID.append(line[2].strip())
            count_i = 2
            continue
        if(len(line) >= 1):
            try:
                PE_w1 = float(line[1].strip())
                PE_w_tmp.append(PE_w1)
            except:
                if("PE" not in line[0]):
                    jobID.append(line[1].strip())
                    if(len(PE_w_tmp) > 0):
                        PE_w_max = max(PE_w_tmp)
                        PE_w.append(PE_w_max)
                        del PE_w_tmp[:]
                    else:
                        PE_w.append(0.0)
                else:
                    continue
    PE_w_file.close()
    if(len(PE_w_tmp) > 0):
        PE_w_max = max(PE_w_tmp)
        PE_w.append(PE_w_max)
        del PE_w_tmp[:]
    else:
        PE_w.append(0.0)
 
    return PE_w


def classify_single(IOBW_r, PE_r, file_count):
    
    IOmode_r = "NULL"

    if(abs(IOBW_r) > sys.float_info.epsilon \
    and abs(PE_r) > sys.float_info.epsilon):
        if(PE_r == 1):
            IOmode_r = "1to1" 
        else:
            if(IOBW_r >= IOmode_N_N ):
                IOmode_r = "NtoN"
            elif(IOBW_r <= IOmode_N_N and IOBW_r >= IOmode_N_1 \
            and file_count/PE_r > 0.25):
                IOmode_r = "NtoN" 
            elif(IOBW_r <= IOmode_N_N and IOBW_r >= IOmode_N_1 \
            and file_count/PE_r <= 0.25):
                IOmode_r = "Nto1" 
            elif(IOBW_r <= IOmode_N_1 and file_count/PE_r <= 0.5):
                IOmode_r = "Nto1"
            elif(IOBW_r <= IOmode_N_1 and file_count/PE_r > 0.5):
                IOmode_r = "NtoN"
    else:
        print 'IOBW: ', IOBW_r
        print 'PE_r: ', PE_r
        print 'file_count: ', PE_r
        IOmode_r = "UNKNOW"
    if(IOmode_r == 'NULL'):
        print 'NULL IOBW: ', IOBW_r
        print 'NULL PE_r: ', PE_r
        print 'NULL file_count: ', PE_r
    
    return IOmode_r


def classify_all(program_name, jobID, IOBW_r, IOBW_w, PE_r, PE_w, file_count):
     
    jobID_r_N_N = []
    jobID_w_N_N = []
    jobID_r_N_1 = []
    jobID_w_N_1 = []
    jobID_r_1_1 = []
    jobID_w_1_1 = []
    jobID_r_null = [] 
    jobID_w_null = [] 

    corehour_r_N_N = 0.0 
    corehour_w_N_N = 0.0
    corehour_r_N_1 = 0.0
    corehour_w_N_1 = 0.0
    corehour_r_1_1 = 0.0
    corehour_w_1_1 = 0.0
    corehour_r_null = 0.0 
    corehour_w_null = 0.0 

    index_r_N_N = []
    index_w_N_N = []
    index_r_N_1 = []
    index_w_N_1 = []
    index_r_1_1 = []
    index_w_1_1 = []
    index_r_null = [] 
    index_w_null = [] 

    IOmode_r = []
    IOmode_w = []
 
#Classify the Read IOmode.
    for i in xrange(len(jobID)):
        if(abs(IOBW_r[i]) > sys.float_info.epsilon \
        and abs(PE_r[i]) > sys.float_info.epsilon):
            if(PE_r[i] == 1):
                jobID_r_1_1.append(jobID[i])
                index_r_1_1.append(i)
#                corehour_r_1_1 += corehour[i] 
                IOmode_r.append("1to1") 
            else:
                if(IOBW_r[i] >= IOmode_N_N ):
                    jobID_r_N_N.append(jobID[i]) 
                    index_r_N_N.append(i) 
#                    corehour_r_N_N += corehour[i] 
                    IOmode_r.append("NtoN") 
                elif(IOBW_r[i] <= IOmode_N_N and IOBW_r[i] >= IOmode_N_1 \
                and file_count[i]/PE_r[i] > 0.25):
                    jobID_r_N_N.append(jobID[i])
                    index_r_N_N.append(i)
#                    corehour_r_N_N += corehour[i] 
                    IOmode_r.append("NtoN") 
                elif(IOBW_r[i] <= IOmode_N_N and IOBW_r[i] >= IOmode_N_1 \
                and file_count[i]/PE_r[i] <= 0.25):
                    jobID_r_N_1.append(jobID[i])
                    index_r_N_1.append(i)
#                    corehour_r_N_1 += corehour[i] 
                    IOmode_r.append("Nto1") 
                elif(IOBW_r[i] <= IOmode_N_1 and file_count[i]/PE_r[i] <= 0.5):
                    jobID_r_N_1.append(jobID[i])
                    index_r_N_1.append(i)
#                    corehour_r_N_1 += corehour[i] 
                    IOmode_r.append("Nto1") 
                elif(IOBW_r[i] <= IOmode_N_1 and file_count[i]/PE_r[i] > 0.5):
                    jobID_r_N_N.append(jobID[i])
                    index_r_N_N.append(i)
#                    corehour_r_N_N += corehour[i] 
                    IOmode_r.append("NtoN") 
        else:
            jobID_r_null.append(jobID[i]) 
            index_r_null.append(i) 
#            corehour_r_null += corehour[i] 
            IOmode_r.append("NULL")

        if(len(IOmode_r) != i+1 ): 
            print i 
            print IOBW_r[i] 
            print PE_r[i]
            print file_count[i]
            sys.exit()
 
    print "IOmode_r: ", len(IOmode_r)
#Classify the Write IOmode.
    for i in xrange(len(jobID)):
        if(abs(IOBW_w[i]) > sys.float_info.epsilon \
        and abs(PE_w[i]) > sys.float_info.epsilon):
            if(PE_w[i] == 1):
                jobID_w_1_1.append(jobID[i])
                index_w_1_1.append(i)
#                corehour_w_1_1 += corehour[i] 
                IOmode_w.append("1to1") 
            else:
                if(IOBW_w[i] >=IOmode_N_N ):
                    jobID_w_N_N.append(jobID[i])
                    index_w_N_N.append(i)
#                    corehour_w_N_N += corehour[i] 
                    IOmode_w.append("NtoN") 
                elif(IOBW_w[i] <= IOmode_N_N and IOBW_w[i] >= IOmode_N_1 \
                and file_count[i]/PE_w[i] > 0.25):
                    jobID_w_N_N.append(jobID[i])
                    index_w_N_N.append(i)
#                    corehour_w_N_N += corehour[i] 
                    IOmode_w.append("NtoN") 
                elif(IOBW_w[i] <= IOmode_N_N and IOBW_w[i] >= IOmode_N_1 \
                and file_count[i]/PE_w[i] <= 0.25):
                    jobID_w_N_1.append(jobID[i])
                    index_w_N_1.append(i)
#                    corehour_w_N_1 += corehour[i] 
                    IOmode_w.append("Nto1") 
                elif(IOBW_w[i] <= IOmode_N_1 and file_count[i]/PE_w[i] <= 0.5):
                    jobID_w_N_1.append(jobID[i])
                    index_w_N_1.append(i)
                    #corehour_w_N_1 += corehour[i] 
                    IOmode_w.append("Nto1") 
                elif(IOBW_w[i] <= IOmode_N_1 and file_count[i]/PE_w[i] > 0.5):
                    jobID_w_N_N.append(jobID[i])
                    index_w_N_N.append(i)
#                    corehour_w_N_N += corehour[i] 
                    IOmode_w.append("NtoN") 
        else:
            jobID_w_null.append(jobID[i]) 
            index_w_null.append(i) 
#            corehour_w_null += corehour[i] 
            IOmode_w.append("NULL") 
    print "IOmode_w: ", len(IOmode_w)

    return jobID_r_N_N, jobID_w_N_N, jobID_r_N_1, jobID_w_N_1,\
    jobID_r_1_1, jobID_w_1_1, jobID_r_null, jobID_w_null, \
    IOmode_r, IOmode_w, \
    index_r_N_N, index_w_N_N, index_r_N_1, index_w_N_1, \
    index_r_1_1, index_w_1_1, index_r_null, index_w_null
  
#    corehour_r_N_N, corehour_w_N_N, corehour_r_N_1, corehour_w_N_1, \
#    corehour_r_1_1, corehour_w_1_1, corehour_r_null, corehour_w_null, \

#def sum_corehour(corehour):
#    sum_corehour = 0.0
#    for i in xrange(len(corehour)):
#        sum_corehour += corehour[i]
#    return sum_corehour

def calculate_corehour(corehour, index_r_N_N, index_w_N_N, index_r_N_1, index_w_N_1, \
index_r_1_1, index_w_1_1, index_r_null, index_w_null):
 
    sum_corehour = sum(corehour)
    
    corehour_r_N_N = 0.0 
    corehour_w_N_N = 0.0
    corehour_r_N_1 = 0.0
    corehour_w_N_1 = 0.0
    corehour_r_1_1 = 0.0
    corehour_w_1_1 = 0.0
    corehour_r_null = 0.0 
    corehour_w_null = 0.0 

    for r_N_N in index_r_N_N:
        corehour_r_N_N += corehour[r_N_N]
    for r_N_1 in index_r_N_1:
        corehour_r_N_1 += corehour[r_N_1]
    for r_1_1 in index_r_1_1:
        corehour_r_1_1 += corehour[r_1_1]
    for r_null in index_r_null:
        corehour_r_null += corehour[r_null]

    for w_N_N in index_w_N_N:
        corehour_w_N_N += corehour[w_N_N]
    for w_N_1 in index_w_N_1:
        corehour_w_N_1 += corehour[w_N_1]
    for w_1_1 in index_w_1_1:
        corehour_w_1_1 += corehour[w_1_1]
    for w_null in index_w_null:
        corehour_w_null += corehour[w_null]

    corehour_r_N_N_percent = corehour_r_N_N/ sum_corehour
    corehour_w_N_N_percent = corehour_w_N_N/ sum_corehour
    corehour_r_N_1_percent = corehour_r_N_1/ sum_corehour
    corehour_w_N_1_percent = corehour_w_N_1/ sum_corehour
    corehour_r_1_1_percent = corehour_r_1_1/ sum_corehour
    corehour_w_1_1_percent = corehour_w_1_1/ sum_corehour
    corehour_r_null_percent = corehour_r_null/ sum_corehour
    corehour_w_null_percent = corehour_w_null/ sum_corehour

    return corehour_r_N_N, corehour_w_N_N, corehour_r_N_1, corehour_w_N_1, \
    corehour_r_1_1, corehour_w_1_1, corehour_r_null, corehour_w_null, \
    corehour_r_N_N_percent, corehour_w_N_N_percent, corehour_r_N_1_percent, \
    corehour_w_N_1_percent, corehour_r_1_1_percent, corehour_w_1_1_percent, \
    corehour_r_null_percent, corehour_w_null_percent 



#def calculate_corehour(corehour, index_r_N_N, index_w_N_N, index_r_N_1, index_w_N_1, \
#index_r_1_1, index_w_1_1, index_r_null, index_w_null):
# 
#    sum_corehour = sum(corehour)
#    
#    corehour_r_N_N = 0.0 
#    corehour_w_N_N = 0.0
#    corehour_r_N_1 = 0.0
#    corehour_w_N_1 = 0.0
#    corehour_r_1_1 = 0.0
#    corehour_w_1_1 = 0.0
#    corehour_r_null = 0.0 
#    corehour_w_null = 0.0 
#
#    for r_N_N in index_r_N_N:
#        corehour_r_N_N += corehour[r_N_N]
#    for r_N_1 in index_r_N_1:
#        corehour_r_N_1 += corehour[r_N_1]
#    for r_1_1 in index_r_1_1:
#        corehour_r_1_1 += corehour[r_1_1]
#    for r_null in index_r_null:
#        corehour_r_null += corehour[r_null]
#
#    for w_N_N in index_w_N_N:
#        corehour_w_N_N += corehour[w_N_N]
#    for w_N_1 in index_w_N_1:
#        corehour_w_N_1 += corehour[w_N_1]
#    for w_1_1 in index_w_1_1:
#        corehour_w_1_1 += corehour[w_1_1]
#    for w_null in index_w_null:
#        corehour_w_null += corehour[w_null]


def search_program_name(jobID):
    program_name = []
    program_name_read = []
    jobID_read = []

    search_file = file('collect_data/jobid.csv','r')
    reader = csv.reader(search_file)
    next(reader)

    for line in reader:
#        print line
        if(line[0].strip()):
            program_name_read.append(line[0].strip())
        if(line[3].strip()):
            jobID_read.append(line[3].strip())
    search_file.close()

    for i in xrange(len(jobID)):
        for j in xrange(len(jobID_read)):
            if(jobID[i] == jobID_read[j]):
                program_name.append(program_name_read[j])
                break

    return program_name

def save_med_results(program_name, jobID, corehour, IOBW_r, IOBW_w, PE_r, PE_w, file_count):
    data = []

    for i in xrange(len(program_name)):
        data.append([program_name[i], jobID[i], corehour[i], \
        float("{0:.2f}".format(IOBW_r[i])), float("{0:.2f}".format(IOBW_w[i])), \
        PE_r[i], PE_w[i], file_count[i]])

    results_file = file(results_Med_file_name, 'wb')
    writer = csv.writer(results_file)
    writer.writerow(['Program_name', 'jobID', 'IOBW_w', 'PE_r', 'PE_w', 'file_count'])
    writer.writerows(data) 
    results_file.close() 



def show_classify_results(program_name, jobID, corehour, IOmode_r, IOmode_w):
    data = []

    for i in xrange(len(program_name)):
        data.append([program_name[i], jobID[i], corehour[i], IOmode_r[i], IOmode_w[i]])

    return data

def save_classify_results(data):

    results_file = file(results_file_name, 'wb')
    writer = csv.writer(results_file)
    writer.writerow(['Program_name', 'jobID', 'IOmode_read', 'IOmode_write'])
    writer.writerows(data) 
    results_file.close() 


def compute_from_Med():
    
    program_name, jobID, corehour, IOBW_r , IOBW_w , PE_r, PE_w, file_count \
    = read_Med_results()
    
    print len(program_name)
    print len(jobID)
    print len(corehour)

    print len(IOBW_r)
    print len(IOBW_w )
    print len(file_count)
    print len(PE_r)
    print len(PE_w)
    
    
    jobID_r_N_N, jobID_w_N_N, jobID_r_N_1, jobID_w_N_1,\
    jobID_r_1_1, jobID_w_1_1, jobID_r_null, jobID_w_null, IOmode_r, IOmode_w, \
    index_r_N_N, index_w_N_N, index_r_N_1, index_w_N_1, \
    index_r_1_1, index_w_1_1, index_r_null, index_w_null \
    = classify_all(program_name, jobID, IOBW_r, IOBW_w, PE_r, PE_w, file_count)

    print "classify_all complete!" 
    
    data = show_classify_results(program_name, jobID, corehour, IOmode_r, IOmode_w)
    save_classify_results(data)
    
    print "save_classify_results complete!" 

    print "Number of jobID_r_N_N: %d."%len(jobID_r_N_N)
    print "Number of jobID_w_N_N: %d."%len(jobID_w_N_N)
    print "Number of jobID_r_N_1: %d."%len(jobID_r_N_1)
    print "Number of jobID_w_N_1: %d."%len(jobID_w_N_1)
    print "Number of jobID_r_1_1: %d."%len(jobID_r_1_1)
    print "Number of jobID_w_1_1: %d."%len(jobID_w_1_1)
    print "Number of jobID_r_null: %d."%len(jobID_r_null)
    print "Number of jobID_w_null: %d."%len(jobID_w_null) 
    
    print corehour[0]

    corehour_r_N_N, corehour_w_N_N, corehour_r_N_1, corehour_w_N_1, \
    corehour_r_1_1, corehour_w_1_1, corehour_r_null, corehour_w_null, \
    corehour_r_N_N_percent, corehour_w_N_N_percent, corehour_r_N_1_percent, \
    corehour_w_N_1_percent, corehour_r_1_1_percent, corehour_w_1_1_percent, \
    corehour_r_null_percent, corehour_w_null_percent \
    = calculate_corehour(corehour, index_r_N_N, index_w_N_N, index_r_N_1, index_w_N_1, \
    index_r_1_1, index_w_1_1, index_r_null, index_w_null)
    
    print "calculate_corehour complete!" 

    print "corehour_r_N_N: ", corehour_r_N_N
    print "corehour_w_N_N: ", corehour_w_N_N
    print "corehour_r_N_1: ", corehour_r_N_1
    print "corehour_w_N_1: ", corehour_w_N_1
    print "corehour_r_1_1: ", corehour_r_1_1
    print "corehour_w_1_1: ", corehour_w_1_1
    print "corehour_r_null: ", corehour_r_null
    print "corehour_w_null: ", corehour_w_null

    print "corehour_r_N_N_percent: ", corehour_r_N_N_percent
    print "corehour_w_N_N_percent: ", corehour_w_N_N_percent
    print "corehour_r_N_1_percent: ", corehour_r_N_1_percent
    print "corehour_w_N_1_percent: ", corehour_w_N_1_percent
    print "corehour_r_1_1_percent: ", corehour_r_1_1_percent
    print "corehour_w_1_1_percent: ", corehour_w_1_1_percent
    print "corehour_r_null_percent: ", corehour_r_null_percent
    print "corehour_w_null_percent: ", corehour_w_null_percent

def match_data(program_read, jobID_read, corehour_read, jobID_file, IOBW_r_read, IOBW_w_read, jobID_PE, PE_r_read, PE_w_read):
    program_name = []
    corehour = []
    
    IOBW_r = []
    IOBW_w = []

    PE_r = []
    PE_w = []
    for i in xrange(len(jobID_file)):
	for j in xrange(len(jobID_read)):
	    if(jobID_read[j] == jobID_file[i]):
    		program_name.append(program_read[j])
    		corehour.append(corehour_read[j])
    		IOBW_r.append(IOBW_r_read[j])
    		IOBW_w.append(IOBW_w_read[j])
		break
	for k in xrange(len(jobID_PE)):
	    if(jobID_PE[k] == jobID_file[i]):
    		PE_r.append(PE_r_read[k])
    		PE_w.append(PE_w_read[k])
		break
    return program_name, corehour, IOBW_r, IOBW_w, PE_r, PE_w

def compute_from_start():
    IOBW_file = file(IOBW_file_name ,'r')
    MDS_file = file(MDS_file_name ,'r')
    PE_r_file = file(PE_r_file_name ,'r')
    PE_w_file = file(PE_w_file_name ,'r')

    jobID_PE, PE_r_read, PE_w_read = read_maxPE()
    
    print "Read PE file complete!" 
    
    jobID_file, file_count = read_unique_file_count()
    jobID_file_set = set(jobID_file)

    print "Read file_count file complete!" 
 
    program_name_IOBW, jobID_IOBW, IOBW_r_IOBW, IOBW_w_IOBW, corehour_IOBW = read_IOBW_corehour(IOBW_file)
    
    print "Read IOBW file complete!" 
    
    
    print "Length of jobID_IOBW: ", len(jobID_IOBW)
    print "Length of jobID_file: ", len(jobID_file)
    print "Length of IOBW_r_IOBW: ", len(IOBW_r_IOBW)
    print "Length of PE_r_read: ", len(PE_r_read)

    program_name, corehour, IOBW_r, IOBW_w, PE_r, PE_w = match_data(program_name_IOBW, jobID_IOBW, corehour_IOBW, jobID_file, \
    IOBW_r_IOBW, IOBW_w_IOBW, jobID_PE, PE_r_read, PE_w_read)

    save_med_results(program_name, jobID_file, corehour, IOBW_r, IOBW_w, PE_r, PE_w, file_count)

    print "Save_med_results complete!" 
    
    jobID_r_N_N, jobID_w_N_N, jobID_r_N_1, jobID_w_N_1,\
    jobID_r_1_1, jobID_w_1_1, jobID_r_null, jobID_w_null, IOmode_r, IOmode_w, \
    index_r_N_N, index_w_N_N, index_r_N_1, index_w_N_1, \
    index_r_1_1, index_w_1_1, index_r_null, index_w_null \
    = classify_all(program_name, jobID_file, IOBW_r, IOBW_w, PE_r, PE_w, file_count)
    
    data = show_classify_results(program_name, jobID_file, corehour, IOmode_r, IOmode_w)
    save_classify_results(data)
    
    print "save_classify_results complete!" 

    print "Number of jobID_r_N_N: %d."%len(jobID_r_N_N)
    print "Number of jobID_w_N_N: %d."%len(jobID_w_N_N)
    print "Number of jobID_r_N_1: %d."%len(jobID_r_N_1)
    print "Number of jobID_w_N_1: %d."%len(jobID_w_N_1)
    print "Number of jobID_r_1_1: %d."%len(jobID_r_1_1)
    print "Number of jobID_w_1_1: %d."%len(jobID_w_1_1)
    print "Number of jobID_r_null: %d."%len(jobID_r_null)
    print "Number of jobID_w_null: %d."%len(jobID_w_null) 
    
    print corehour[0]

    corehour_r_N_N, corehour_w_N_N, corehour_r_N_1, corehour_w_N_1, \
    corehour_r_1_1, corehour_w_1_1, corehour_r_null, corehour_w_null, \
    corehour_r_N_N_percent, corehour_w_N_N_percent, corehour_r_N_1_percent, \
    corehour_w_N_1_percent, corehour_r_1_1_percent, corehour_w_1_1_percent, \
    corehour_r_null_percent, corehour_w_null_percent \
    = calculate_corehour(corehour, index_r_N_N, index_w_N_N, index_r_N_1, index_w_N_1, \
    index_r_1_1, index_w_1_1, index_r_null, index_w_null)
    
    print "calculate_corehour complete!" 

    print "corehour_r_N_N: ", corehour_r_N_N
    print "corehour_w_N_N: ", corehour_w_N_N
    print "corehour_r_N_1: ", corehour_r_N_1
    print "corehour_w_N_1: ", corehour_w_N_1
    print "corehour_r_1_1: ", corehour_r_1_1
    print "corehour_w_1_1: ", corehour_w_1_1
    print "corehour_r_null: ", corehour_r_null
    print "corehour_w_null: ", corehour_w_null

    print "corehour_r_N_N_percent: ", corehour_r_N_N_percent
    print "corehour_w_N_N_percent: ", corehour_w_N_N_percent
    print "corehour_r_N_1_percent: ", corehour_r_N_1_percent
    print "corehour_w_N_1_percent: ", corehour_w_N_1_percent
    print "corehour_r_1_1_percent: ", corehour_r_1_1_percent
    print "corehour_w_1_1_percent: ", corehour_w_1_1_percent
    print "corehour_r_null_percent: ", corehour_r_null_percent
    print "corehour_w_null_percent: ", corehour_w_null_percent

if __name__ == "__main__":

#    compute_from_start()
    compute_from_Med()
