jobid_file = '../../source_job_data/jobid_corehour_100k_1000k.csv'
volume_file = '../../results_job_data/collect_data/all_data/IOBW.csv'
save_volume_file = '../../results_job_data/volume/jobid_corehour_100k_1000k.csv'

def read_jobid():
    f = open(jobid_file, 'r')
    jobid = []
    for line in open(jobid_file):
        line = f.readline()
        array = line.split()
        jobid.append(array[0])
    return jobid

def read_volume():
    f = open(volume_file, 'r')
    volume_r = dict()
    volume_w = dict()
    for line in open(volume_file):
        try:
            line = f.readline()
            array = line.split(',')
            volume_r_tmp = float(array[4])
            volume_w_tmp = float(array[5])
            volume_r[array[1]] = volume_r_tmp
            volume_w[array[1]] = volume_w_tmp
        except Exception as e:
            continue

    return volume_r, volume_w

def save_volume(jobid, volume_r, volume_w):
    f = open(save_volume_file, 'wb')
    for job in jobid:
        try:
            writerow = "%s %f %f\n"%(job, volume_r[job], volume_w[job])
            f.write(writerow)
        except Exception as e:
            print e
if __name__ == "__main__":
    jobid = read_jobid()
    volume_r, volume_w = read_volume()
    save_volume(jobid, volume_r, volume_w)




