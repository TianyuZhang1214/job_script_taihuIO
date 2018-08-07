job_file = '/home/export/mount_test/swstorage/source_job_data/JOB_log.csv'
benefit_job_file = 'benefitjob.csv'

def read(file_name):
    jobid = []
    f = open(file_name, 'r')
    for line in open(file_name):
        jobid.append(line.strip())

    return jobid
    
def get_job_name(jobid):
    job_name = dict()
    job_name_set = set()
    f = open(job_file, 'r')
    
    for line in open(job_file):
        array = line.split()
        job_name[array[0]] = {}
        job_name[array[0]]['usr'] = array[1]
        job_name[array[0]]['project'] = array[14]
        job_name[array[0]]['jobname'] = array[15]
        job_name[array[0]]['prgname'] = array[16]

    for job in jobid:
        print "%s %s %s %s"%(job_name[job]['usr'],\
        job_name[job]['project'], job_name[job]['jobname'], \
        job_name[job]['prgname'],) 

if __name__ == "__main__":
    jobid = read(benefit_job_file)
    get_job_name(jobid)
