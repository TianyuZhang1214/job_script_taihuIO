



def save_fd_info(fd_info, file_name):
    f = open(file_name, 'wb')
    for fd in fd_info:
        for time in fd_info[fd]['time']:
            writerow = "%s %d %f %f\n"\
            %(fd, time, fd_info[fd]['time'][time]['read'], \
            fd_info[fd]['time'][time]['write'])
            f.write(writerow)
            
    


