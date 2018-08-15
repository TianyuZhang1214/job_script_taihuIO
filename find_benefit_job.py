from read_from_source import read_corehour

jobid_file = '/home/export/mount_test/swstorage/results_job_data/benefit_job/Nto1.csv'
#jobid_file = '/home/export/mount_test/swstorage/results_job_data/collect_data/all_data/jobid.csv'
def read_jobid():
    f = open(jobid_file, 'r')
    jobid = []
    for line in open(jobid_file):
        line = f.readline()
        array = line.split()
        jobid.append(array[0])
    return jobid

def get_sum_benefit_corehour(jobid, corehour):
    sum_benefit_corehour = 0
    sum_corehour = 0
    for job in jobid:
        sum_benefit_corehour += corehour[job]

    for job in corehour:
        sum_corehour += corehour[job]

    print sum_corehour
    print sum_benefit_corehour

if __name__ == "__main__":
    jobid = read_jobid()
    corehour = read_corehour()
    get_sum_benefit_corehour(jobid, corehour)

