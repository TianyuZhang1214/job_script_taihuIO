import MySQLdb
import MySQLdb.cursors
import re

iomode_file_read = '/home/export/mount_test/swstorage/results_job_data/app_IO_pattern/iomode_read.csv'
iomode_file_write = '/home/export/mount_test/swstorage/results_job_data/app_IO_pattern/iomode_write.csv'

iopt_file_read = '/home/export/mount_test/swstorage/results_job_data/app_IO_pattern/iopt_read_sort.csv'
iopt_file_write = '/home/export/mount_test/swstorage/results_job_data/app_IO_pattern/iopt_write_sort.csv'

jobname_file = '/home/export/mount_test/swstorage/results_job_data/app_IO_pattern/jobname_unq.csv'

def read_jobname():
    app_info = dict()
    f = open(jobname_file, 'r')
    for line in open(jobname_file):
        array = line.split()
        if(array[1] in app_info):
            app_info[array[1]][array[0]] = {}
            app_info[array[1]][array[0]]['core'] = int(array[2])
        else:
            app_info[array[1]] = {}
            app_info[array[1]][array[0]] = {}
            app_info[array[1]][array[0]]['core'] = int(array[2])

    return app_info

def merge_iomode(file_name):
    f = open(file_name, 'r')
    app_io_pt= dict()

    for line in open(file_name):
        array = line.split()
        if(not app_io_pt.has_key(array[0])):
            app_io_pt[array[0]] = {}
            app_io_pt[array[0]]['iomode'] = {}
            app_io_pt[array[0]]['iomode'][array[3]] = int(array[4])
        else:
            if(not app_io_pt[array[0]].has_key(array[3])):
                app_io_pt[array[0]]['iomode'][array[3]] = int(array[4])
            else:
                app_io_pt[array[0]]['iomode'][array[3]] += int(array[4])

    return app_io_pt

def merge_job_info(app_info, app_io_pt):
    for app in app_info:
        for job in app_info[app]:
            if(job in app_io_pt):
                app_info[app][job]['iopt'] = app_io_pt[job]

def diff_iopt(file_name):
    iopt = dict()
    diff_cnt = set()
    f = open(file_name, 'r')
    for line in open(file_name):
        array = line.split()
        if(array[0] in iopt):
            if(array[3] == iopt[array[0]]['iomode']):
                continue
            else:
                diff_cnt.add(array[0])
        else:
            iopt[array[0]] = {}
            iopt[array[0]]['iomode'] = array[3]

    print len(diff_cnt)
                
def save_iomode(app_info, file_name):
    f = open(file_name,'wb')
    for app in app_info:
        for job in app_info[app]:
            if(app_info[app][job].has_key('iopt')):
                writerow = '%s %d %s'%(app, app_info[app][job]['core'], job)
                for iomode in app_info[app][job]['iopt']['iomode']:
                    writerow += ' %s %d'%(iomode, app_info[app][job]['iopt']['iomode'][iomode])
                writerow += '\n'
                f.write(writerow)
            else:
                continue

def get_app_iopt():
    app_io_pt_write = merge_iomode(iomode_file_write)
    print 'Read iomode completed!'
    app_info = read_jobname()
    print 'Read app name completed!'
    merge_job_info(app_info, app_io_pt_write)
    print 'Merge jobinfo completed!'
    save_iomode(app_info, iopt_file_write)
    print 'Save appinfo completed!'

    
if __name__ == '__main__':

    diff_iopt(iopt_file_read)
    diff_iopt(iopt_file_write)
    
