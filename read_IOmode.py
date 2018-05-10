file_name_r = '../../results_job_data/collect_data/all_IOphase/IO_phase_r.csv'
file_name_w = '../../results_job_data/collect_data/all_IOphase/IO_phase_w.csv'

result_file_name_r = '../../results_job_data/result_data/IOmode_by_volumn/IO_mode_r_count.csv'
result_file_name_w = '../../results_job_data/result_data/IOmode_by_volumn/IO_mode_w_count.csv'

classify_volumn_percent = 0.5
classify_volumn_Nto1_percent = 0.3

def read_all_results(file_name):

    jobID = []
    CNC = []
    runtime = []
    corehour = []
    IO_mode = []
    IO_volumn = []
    time_start = []
    time_end = []
    time = []
    
    f = open(file_name, 'r')
    for line in open(file_name):
        line = f.readline()
        array = line.split()
        jobID.append(array[0])
        CNC.append(int(array[1]))
        runtime.append(int(array[2]))
        corehour.append(float(array[3]))
        IO_mode.append(str(array[4]))
        IO_volumn.append(float(array[5]))
        time_start.append(int(array[6]))
        time_end.append(int(array[7]))
        time.append(int(array[8]))
    return jobID, CNC, runtime, corehour, IO_mode, IO_volumn, time_start, time_end, time

def group_by_jobid(jobID, CNC, runtime, corehour, IO_mode, IO_volumn, tag):
    IOmode = dict()
#    IOmode[0] = {'CNC':1, 'runtime':2, 'corehour':1, 'IO_mode':{'NtoN':0, 'Nto1':1, '1to1':2}}
    for i in xrange(len(jobID)):
        if(jobID[i] not in IOmode):
            IOmode[jobID[i]] = {'CNC':CNC[i], 'runtime':runtime[i], 'corehour':corehour[i], 'IO_mode':{'volumn': {}, 'count': {}}}
            if( tag == 'r'): 
                continue
            elif( tag == 'w'): 
                if(IO_mode[i] in IOmode[jobID[i]]['IO_mode']['volumn']):
                    IOmode[jobID[i]]['IO_mode']['volumn'][IO_mode[i]] += IO_volumn[i]
                    IOmode[jobID[i]]['IO_mode']['count'][IO_mode[i]] += 1
                else:
                    IOmode[jobID[i]]['IO_mode']['volumn'][IO_mode[i]] = IO_volumn[i]
                    IOmode[jobID[i]]['IO_mode']['count'][IO_mode[i]] = 1
            else:
                print 'Tag is needed. r for read;w for write.'
        else:
            if(IO_mode[i] in IOmode[jobID[i]]['IO_mode']['volumn']):
                IOmode[jobID[i]]['IO_mode']['volumn'][IO_mode[i]] += IO_volumn[i]
                IOmode[jobID[i]]['IO_mode']['count'][IO_mode[i]] += 1
            else:
                IOmode[jobID[i]]['IO_mode']['volumn'][IO_mode[i]] = IO_volumn[i]
                IOmode[jobID[i]]['IO_mode']['count'][IO_mode[i]] = 1
    return IOmode
    
def save_IOmode(file_name, IOmode):
    f = open(file_name, "wb")

    for key in IOmode:
        if(len(IOmode[key]['IO_mode']['count']) > 0):
            write_row = "%s %d %d %f "%(key, IOmode[key]['CNC'],\
            IOmode[key]['runtime'], IOmode[key]['corehour'])
            for key_1 in IOmode[key]['IO_mode']['count']:
                write_row += "%s %f %d "%(key_1, IOmode[key]['IO_mode']['volumn'][key_1], IOmode[key]['IO_mode']['count'][key_1])
            write_row += " \n"
            f.write(write_row)
        else:
            continue

def read_IOmode():

    jobID, CNC, runtime, corehour, IO_mode, IO_volumn, \
    time_start, time_end, time = read_all_results(file_name_r)
    IOmode_r = group_by_jobid(jobID, CNC, runtime, corehour, IO_mode, IO_volumn, 'r')
    
    del jobID
    del CNC
    del runtime
    del corehour
    del IO_mode
    del IO_volumn
    del time_start
    del time_end
    del time
    
    jobID, CNC, runtime, corehour, IO_mode, IO_volumn, time_start, time_end, time = read_all_results(file_name_w)
    IOmode_w = group_by_jobid(jobID, CNC, runtime, corehour, IO_mode, IO_volumn, 'w')

    return IOmode_r, IOmode_w
    
def deal_IOmode(IOmode):
    IOmode_result = dict()
    for key in IOmode:
        if(len(IOmode[key]['IO_mode']['volumn']) > 0):
            IOmode_result[key] = {}
            IOmode_result[key]['CNC'] = IOmode[key]['CNC']
            IOmode_result[key]['runtime'] = IOmode[key]['runtime']
            IOmode_result[key]['corehour'] = IOmode[key]['corehour']
            total_values = sum(list(IOmode[key]['IO_mode']['volumn'].values()))
            IOmode_result[key]['IO_mode'] = ['Mix']
            if(IOmode[key]['IO_mode']['volumn'].has_key('Nto1')):
                if(IOmode[key]['IO_mode']['volumn']['Nto1']/total_values >= \
                classify_volumn_Nto1_percent):
                    IOmode_result[key]['IO_mode'] = ['Nto1']
                else:
                    for key_1 in IOmode[key]['IO_mode']['volumn']:
                        if(IOmode[key]['IO_mode']['volumn'][key_1]/total_values >= \
                        classify_volumn_percent):
                            IOmode_result[key]['IO_mode'] = [key_1]
                            break
                        else:
                            continue
            else:
                for key_1 in IOmode[key]['IO_mode']['volumn']:
                    if(IOmode[key]['IO_mode']['volumn'][key_1]/total_values >= \
                    classify_volumn_percent):
                        IOmode_result[key]['IO_mode'] = [key_1]
                        break
                    else:
                        continue
            if (IOmode_result[key]['IO_mode'] == ['Mix']):
                print key
        else:
            continue

    return IOmode_result

def get_IOmode_count_result(IOmode_result):
    IOmode_count = {'1to1':0, 'Nto1':0, 'NtoN':0, 'UNKNOW':0, 'Mix':0}
    IOmode_corehour = {'1to1':0, 'Nto1':0, 'NtoN':0, 'UNKNOW':0, 'Mix':0}
    for key in IOmode_result:
        for key_1 in IOmode_result[key]['IO_mode']:
            IOmode_count[key_1] += 1 
            IOmode_corehour[key_1] += IOmode_result[key]['corehour'] 
    return IOmode_count, IOmode_corehour
        
def calculate_NtoN(IOmode_result_r, IOmode_result_w):
    job_count = 0
    job_set = set()
    corehour_count = 0
    tag = 'NtoN';
    for key in IOmode_result_r:
        for key_1 in IOmode_result_r[key]['IO_mode']:
            if(key_1 == tag):
                job_set.add(key)
    
    for key in IOmode_result_w:
        for key_1 in IOmode_result_w[key]['IO_mode']:
            if(key_1 == tag):
                job_set.add(key)
    
    for job in job_set:
        if(job in IOmode_result_r):
            corehour_count += IOmode_result_r[job]['corehour']
        else:
            corehour_count += IOmode_result_w[job]['corehour']
        
    print len(job_set) 
    print corehour_count 
    
    
def get_IOmode_volumn_result(IOmode):
    IOmode_volumn = {'1to1':0, 'Nto1':0, 'NtoN':0, 'UNKNOW':0}
    for key in IOmode:
        for key_1 in IOmode[key]['IO_mode']['volumn']:
            IOmode_volumn[key_1] += IOmode[key]['IO_mode']['volumn'][key_1]
    
    return IOmode_volumn
    
if __name__ == "__main__":

    IOmode_r, IOmode_w = read_IOmode()
    IOmode_r_set = set(IOmode_r)
    IOmode_w_set = set(IOmode_w)

    IOmode_set = IOmode_r_set | IOmode_w_set 
    print len(IOmode_set)
#    save_IOmode(result_file_name_r, IOmode_r)
#    save_IOmode(result_file_name_w, IOmode_w)
    IOmode_volumn_r = get_IOmode_volumn_result(IOmode_r)
    IOmode_volumn_w = get_IOmode_volumn_result(IOmode_w)

    IOmode_result_r = deal_IOmode(IOmode_r)
    IOmode_result_w = deal_IOmode(IOmode_w)
    
    calculate_NtoN(IOmode_result_r, IOmode_result_w)
#    IOmode_count_r, IOmode_corehour_r = get_IOmode_count_result(IOmode_result_r)
#    IOmode_count_w, IOmode_corehour_w = get_IOmode_count_result(IOmode_result_w)
#
#    print IOmode_volumn_r
#    print IOmode_volumn_w
#
#    print IOmode_count_r
#    print IOmode_count_w
#
#    print IOmode_corehour_r
#    print IOmode_corehour_w

