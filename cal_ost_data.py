import socket
from es_search_ost import search_interval as search_interval_ost
from deal_generator import ost_deal_message

def search_ost(file_list):
    try:
        s=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        s.connect(('20.0.2.17',5000))
    except Exception as e:
        print e
#    message = "/home/export/online1/swstorage/test.c,/home/export/online1/swstorage/taihu-io/*"

    s.send(str(file_list)+"\n")
    str1=s.recv(1024)
    s.close()
    array = str1.split(',')
    array_set = set(array)
    return array_set

def get_ost_list(file_open_list):
    ite = len(file_open_list) / 5
    remainder = len(file_open_list) % 5
    ost_list = set()
    file_list_group = []
    target = 'online2'
    
    file_ost_map = dict()
    for file_tmp in file_open_list:
        file_ost_map[file_tmp] = search_ost('/home/export/online2' + file_tmp)
        ost_list |= file_ost_map[file_tmp]

    if(len(ost_list) == 0):
        target = 'online2'
        for file_list_tmp in file_list_group:
            file_list_tmp.replace('online2', 'online1')

    for file_list_tmp in file_list_group:
        ost_list_tmp = search_ost(file_list_tmp)
        ost_list |= ost_list_tmp

    if('' in ost_list):
        ost_list.remove('')
    ost_list = list(ost_list)
    return ost_list, target, file_ost_map

def get_ost_data(start_time, end_time, ost_list, index, host):
    print ost_list 
    ost_host = []
    ost_message = []
    ost_time = []
    ost_str = []
    for i in range(len(ost_list)):
        tmp = str(hex(int(ost_list[i])))[2:]
        if(len(tmp) == 1):
            ost_str.append('000'+tmp)
        elif(len(tmp) == 2):
            ost_str.append('00'+tmp)
        elif(len(tmp) == 3):
            ost_str.append('0'+tmp)
    print ost_str
    for index_tmp in index:
#        try:
        ost_host_tmp, ost_message_tmp, ost_time_tmp \
        = search_interval_ost(start_time, end_time, ost_str, index_tmp, host)
#        except Exception as e:
#            print e
        ost_host += ost_host_tmp
        ost_message += ost_message_tmp
        ost_time += ost_time_tmp
#    print len(ost_message)

#    for i in range(len(ost_message)):
#        print ost_host[i] + ost_message[i] + ost_time[i]

    time1 = start_time[:10] + ' ' + start_time[11:-5]
    time2 = end_time[:10] + ' ' + end_time[11:-5]
    bandr, bandw = ost_deal_message(ost_message, ost_time, time1, time2)
    return bandr, bandw


