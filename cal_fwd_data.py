from es_search_fwd import search_interval as search_interval_fwd
from deal_generator import fwd_deal_message

def get_fwd_data(start_time, end_time, fwd, ost_list, index, host):
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
        ost_host_tmp, ost_message_tmp, ost_time_tmp = search_interval_fwd(start_time, end_time, fwd, ost_str, index_tmp, host)
        ost_host += ost_host_tmp
        ost_message += ost_message_tmp
        ost_time += ost_time_tmp
    print "fwd: %d message_length: %d"%(fwd, len(ost_message))
    res = []
    for i in range(len(ost_message)):
        res.append(ost_host[i]+' '+ ost_message[i]+' '+ ost_time[i])
    res.sort()

    time1 = start_time[:10] + ' ' + start_time[11:-5]
    time2 = end_time[:10] + ' ' + end_time[11:-5]

    bandr, bandw = fwd_deal_message(ost_message, ost_time, time1, time2)
    return bandr, bandw


