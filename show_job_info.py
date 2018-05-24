
import sys
IOBW_affective = 0.1

def show_IOBANDWIDTH(resultr,resultw,jobid):
    count_resultr = 0 
    count_resultw = 0
    count_resultrw = 0
    sum_resultr = 0
    sum_resultw = 0
    max_resultr = max(resultr)
    max_resultw = max(resultw)
    for i in range(len(resultr)):
#        if (abs(resultr[i]) > sys.float_info.epsilon):
        if (resultr[i] > IOBW_affective):
#            if (resultr[i] < 1.0):
#                print resultr[i]
            count_resultr += 1
            sum_resultr += resultr[i]
#        if (abs(resultw[i]) > sys.float_info.epsilon):
        if (resultw[i] > IOBW_affective):
            count_resultw += 1
            sum_resultw += resultw[i]
#        if(abs(resultr[i]) > sys.float_info.epsilon or \
#        abs(resultw[i]) > sys.float_info.epsilon): 
        if(abs(resultr[i]) > IOBW_affective or \
        abs(resultw[i]) > IOBW_affective ):
#            print i
            count_resultrw += 1
    average_resultr = 0.0
    average_resultw = 0.0
    print ("count_IOBW_read  = %f "%(count_resultr))
    print ("count_IOBW_write  = %f "%(count_resultw))
    print ("count_IOBW_read_write = %f "%(count_resultrw))
    print ("sum_IOBW_read  = %f MB "%(sum_resultr))
    print ("sum_IOBW_write  = %f MB "%(sum_resultw))
    print ("max_IOBW_read  = %f MB/s "%(max_resultr))
    print ("max_IOBW_write  = %f MB/s "%(max_resultw))
    if(sum_resultr > sys.float_info.epsilon and count_resultr > sys.float_info.epsilon):
        average_resultr = sum_resultr/count_resultr 
        print ("average_IOBW_resultr = %f MB/s"%(average_resultr))
    if(sum_resultw > sys.float_info.epsilon and count_resultw > sys.float_info.epsilon):
        average_resultw = sum_resultw/count_resultw
        print ("average_IOBW_resultw = %f MB/s"%(average_resultw))
    sum_resultrw = sum_resultr + sum_resultw 
    if(sum_resultrw > sys.float_info.epsilon and count_resultrw > sys.float_info.epsilon):
        average_resultrw = sum_resultrw/count_resultrw
        print ("average_IOBW_resultrw = %f MB/s"%(average_resultrw))

def show_IOPS(IOPS_r, IOPS_w, IOBW_r, IOBW_w, jobid):
#Calculate the sum IOPS .
    count_IOPS_r = 0
    count_IOPS_w = 0
    count_IOPS_rw = 0
    count_IOPS_rw_all = 0
    sum_IOPS_r = 0
    sum_IOPS_w = 0
    for i in range(len(IOPS_r)):
#        if (abs(IOPS_r[i]) > sys.float_info.epsilon):
        if (abs(IOPS_r[i]) > sys.float_info.epsilon \
            and IOBW_r[i] > IOBW_affective):
            count_IOPS_r += 1
            sum_IOPS_r += IOPS_r[i]
#        if (abs(IOPS_w[i]) > sys.float_info.epsilon):
        if (abs(IOPS_w[i]) > sys.float_info.epsilon \
            and IOBW_w[i] > IOBW_affective):
            count_IOPS_w += 1
            sum_IOPS_w += IOPS_w[i]
        if ((abs(IOPS_r[i]) > sys.float_info.epsilon and IOBW_r[i] > IOBW_affective) or \
        (abs(IOPS_w[i]) > sys.float_info.epsilon and IOBW_w[i] > IOBW_affective) ):
            count_IOPS_rw += 1
#        if (IOBW_r[i] > 1.0 or \
#        IOBW_w[i] > 1.0 or \
#        abs(IOPS_r[i]) > sys.float_info.epsilon or \
#        abs(IOPS_w[i]) > sys.float_info.epsilon):
#        if (abs(IOBW_r[i]) > sys.float_info.epsilon or \
#        abs(IOBW_w[i]) > sys.float_info.epsilon or \
#        abs(IOPS_r[i]) > sys.float_info.epsilon or \
#        abs(IOPS_w[i]) > sys.float_info.epsilon):
            count_IOPS_rw_all += 1
    print ("count_IOPS_r = %f "%(count_IOPS_r))
    print ("sum_IOPS_r = %f "%(sum_IOPS_r))
    print ("count_IOPS_w = %f "%(count_IOPS_w))
    print ("sum_IOPS_w = %f "%(sum_IOPS_w))
    average_IOPS_r = 0
    average_IOPS_w = 0
    if(sum_IOPS_r > sys.float_info.epsilon and count_IOPS_r > sys.float_info.epsilon):
        average_IOPS_r = sum_IOPS_r/count_IOPS_r
        print ("average_IOPS_r = %f "%(average_IOPS_r))
    if(sum_IOPS_w > sys.float_info.epsilon and count_IOPS_r > sys.float_info.epsilon):
        average_IOPS_w = sum_IOPS_w/count_IOPS_w
        print ("average_IOPS_w = %f "%(average_IOPS_w))

def show_MDS(MDS_o, MDS_c, jobid):
#Calculate the sum MDS .
    count_MDS_o = 0
    count_MDS_c = 0
    count_MDS_oc = 0
    sum_MDS_o = 0
    sum_MDS_c = 0
    for i in range(len(MDS_o)):
        if (abs(MDS_o[i]) > sys.float_info.epsilon):
            count_MDS_o += 1
            sum_MDS_o += MDS_o[i]
        if (abs(MDS_c[i]) > sys.float_info.epsilon):
            count_MDS_c += 1
            sum_MDS_c += MDS_c[i]
        if (abs(MDS_o[i]) > sys.float_info.epsilon or \
        abs(MDS_c[i]) > sys.float_info.epsilon ):
            count_MDS_oc += 1

    print ("count_MDS_o = %f "%(count_MDS_o))
    print ("sum_MDS_o = %f "%(sum_MDS_o))
    print ("count_MDS_c = %f "%(count_MDS_c))
    print ("sum_MDS_c = %f "%(sum_MDS_c))
    average_MDS_o = 0
    average_MDS_c = 0
    if(sum_MDS_o > sys.float_info.epsilon and count_MDS_o > sys.float_info.epsilon):
        average_MDS_o = sum_MDS_o/count_MDS_o
        print ("average_MDS_o = %f "%(average_MDS_o))
    if(sum_MDS_c > sys.float_info.epsilon and count_MDS_c > sys.float_info.epsilon):
        average_MDS_c = sum_MDS_c/count_MDS_c
        print ("average_MDS_c = %f "%(average_MDS_c))

def show_process(dic,pa,min_time,max_time,jobid):
    host=dict()
    con=dict()
    count=0
    for i in range(len(dic)):
        if not  (host.has_key((dic[i][0],dic[i][1]))):
            host[(dic[i][0],dic[i][1])]=dic[i][2]/1024.0
        else:
            host[(dic[i][0],dic[i][1])]=host[(dic[i][0],dic[i][1])]+dic[i][2]/1024.0
    re=[0.0 for i in range(max_time-min_time+5)]
    for key,value in host.items():
        if value >50:
            re[key[1]]+=1
    count = 0
#    for i in xrange(len(re)):
#        if(re[i] > 0):
#            count += 1
#            print pa, " PE: ", re[i]
    
    max_PE = max(re)
    ave_PE = 0.0
    if(count > 0):
        ave_PE = sum(re)/count
    print pa, "max_PE: ", max_PE
    print pa, "ave_PE: ", ave_PE

    return re

def max_PE(PE_r, PE_w, jobid):
    if(len(PE_r) >1 ): 
        PE_r_max = max(PE_r)
    else:
        PE_r_max = 0
    if(len(PE_w) >1 ): 
        PE_w_max = max(PE_w)
    else:
        PE_w_max = 0

def show_ost_data(ost_list, bandr, bandw):
    for ost in ost_list:
        print "Ost: %s \n"%ost
        print bandr[int(ost)]
        print bandw[int(ost)]
