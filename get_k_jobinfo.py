#--------------README----------------#
#Function : cal_sgl_job(jobname, CNC, k)
#Return : average_bandwidth_read, average_bandwidth_write,    
#         process_read_max, process_write_max
# test case: python get_k_jobinfo.py 3 mdrun_mpi 150

import MySQLdb
import sys

def prt_msg(last_k_jobinfo):
        for jobname in last_k_jobinfo:
            msg = "jobname-cnc: %s job_num: %d iobw_r: %f iobw_w: %f \npe_r: %f pe_w: %f\n"\
            %(jobname, \
            last_k_jobinfo[jobname]['job_num'], \
            last_k_jobinfo[jobname]['iobw_r'], \
            last_k_jobinfo[jobname]['iobw_w'], \
            last_k_jobinfo[jobname]['pe_r']  , \
            last_k_jobinfo[jobname]['pe_w'])
            print msg

def get_job_info(jobname, CNC):
    conn=MySQLdb.connect(host='20.0.2.201',user='root',db='JOB_IO',passwd='',port=3306) 
    cursor=conn.cursor()
    sql = "select JOB_NAME, CNC, JOBID, IOBW_READ_AVERAGE, \
    IOBW_WRITE_AVERAGE, PROCESS_READ_MAX, \
    PROCESS_WRITE_MAX \
    from JOB_IO_INFO where JOB_NAME = '%s' and CNC = '%s'"\
    %(jobname, CNC)

    cursor.execute(sql)
    result = cursor.fetchall()
    conn.commit()
    cursor.close()
    conn.close()

    return result

def deal_jobinfo(result):
    job_info = dict()
    for res in result:
        array = str(res).split(',')
        jobname = array[0][2:-1]+'-'+array[1][1:-1]
        jobid = int(array[2][2:-1])
        if(jobname not in job_info):
            job_info[jobname] = {}
        job_info[jobname][jobid] = {}
        job_info[jobname][jobid]['iobw_r'] = float(array[3])
        job_info[jobname][jobid]['iobw_w'] = float(array[4])
        job_info[jobname][jobid]['pe_r'] = float(array[5][:-1])
        job_info[jobname][jobid]['pe_w'] = float(array[6][:-2])
    return job_info

def get_last_k_jobinfo(k, job_info):
    last_k_jobinfo = dict()
    for jobname in job_info:
        jobid_list = sorted(job_info[jobname].keys(), reverse=True)
        if(len(jobid_list) >= k):
            length = k
        else:
            length = len(jobid_list)

        sum_iobw_r = 0
        sum_iobw_w = 0
        sum_pe_r = 0
        sum_pe_w = 0
        for i in range(0, length):
            #print"%s %f %f %f %f"\
            #%(jobid_list[i], job_info[jobname][jobid_list[i]]['iobw_r'], \
            #job_info[jobname][jobid_list[i]]['iobw_w'], \
            #job_info[jobname][jobid_list[i]]['pe_r'], \
            #job_info[jobname][jobid_list[i]]['pe_w'])
            sum_iobw_r += job_info[jobname][jobid_list[i]]['iobw_r']
            sum_iobw_w += job_info[jobname][jobid_list[i]]['iobw_w']
            sum_pe_r   += job_info[jobname][jobid_list[i]]['pe_r']
            sum_pe_w   += job_info[jobname][jobid_list[i]]['pe_w']
        
        last_k_jobinfo[jobname] = {}
        last_k_jobinfo[jobname]['job_num'] = length
        last_k_jobinfo[jobname]['iobw_r']  = sum_iobw_r / length
        last_k_jobinfo[jobname]['iobw_w']  = sum_iobw_w / length
        last_k_jobinfo[jobname]['pe_r']    = sum_pe_r   / length
        last_k_jobinfo[jobname]['pe_w']    = sum_pe_w   / length

    return last_k_jobinfo 

def cal_sgl_job(jobname, CNC, k):
    row_jobinfo = get_job_info(jobname, CNC)
    job_info = deal_jobinfo(row_jobinfo)
    last_k_jobinfo = get_last_k_jobinfo(k, job_info)
    jobname_key = jobname + '-' + CNC
    prt_msg(last_k_jobinfo)
    return last_k_jobinfo[jobname_key]['iobw_r'], \
    last_k_jobinfo[jobname_key]['iobw_w'], \
    last_k_jobinfo[jobname_key]['pe_r'], \
    last_k_jobinfo[jobname_key]['pe_w']


if __name__ == "__main__":
    if(len(sys.argv) < 4):
        print "Please input: k jobname CNC cal_sql_job"
    else:
        k = int(sys.argv[1])
        jobname = sys.argv[2]
        CNC = sys.argv[3]
        cal_sgl_job(jobname, CNC, k)
