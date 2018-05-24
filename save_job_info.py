import sys

def save_trace(result_message, result_host, trace_file_name):
    f = open(trace_file_name, "wb")
    for i in range(len(result_message)):
        write_row = "[Message]: %s [host]: %s\n"%(result_message[i], result_host[i])
        f.write(write_row)

def save_tmp(resultr_band, resultw_band, resultr_iops, resultw_iops, resultr_open, resultw_close, pe_r, pe_w, tmp_file_name):
    f = open(tmp_file_name, "wb")
    for i in range(len(resultr_band)):
        if(abs(resultr_band[i]) > sys.float_info.epsilon or \
        abs(resultr_band[i]) > sys.float_info.epsilon or \
        abs(resultr_iops[i]) > sys.float_info.epsilon or \
        abs(resultw_iops[i]) > sys.float_info.epsilon or \
        abs(resultr_open[i]) > sys.float_info.epsilon or \
        abs(resultw_close[i]) > sys.float_info.epsilon or \
        abs(pe_r[i]) > sys.float_info.epsilon or \
        abs(pe_w[i]) > sys.float_info.epsilon):
            write_row = "%d %f %f %f %f %f %f %f %f \n"\
            %(i, resultr_band[i], resultw_band[i], resultr_iops[i], \
            resultw_iops[i], resultr_open[i], resultw_close[i], pe_r[i], pe_w[i])
            f.write(write_row)
        else:
            continue

def save_front_bw(bandr, bandw, file_name):
    f = open(file_name, "wb")
    for i in range(len(bandr)):
        write_row = "%d %f %f \n"%(i, bandr[i], bandw[i])
        f.write(write_row)

def save_back_bw(ost_list, bandr, bandw, file_name):
    f = open(file_name, "wb")
    for ost in ost_list:
        ost = int(ost)
        for i in range(len(bandr[ost])):
            write_row = "%d %d %f %f \n"%(ost, i, bandr[ost][i], bandw[ost][i])
            f.write(write_row)

def save_back_bw_agg(ost_list, bandr, bandw, file_name):
    f = open(file_name, "wb")
    for i in range(len(bandr[0])):
        bw_r = 0
        bw_w = 0
        for ost in ost_list:
            ost = int(ost)
            bw_r += bandr[ost][i]
            bw_w += bandw[ost][i]
        write_row = "%d %d %f %f \n"%(ost, i, bw_r, bw_w)
        f.write(write_row)


def save_fwd_bw(fwd, bandr, bandw, file_name):
    f = open(file_name, "ab")
    for i in range(len(bandr)):
        write_row = "%d %d %f %f \n"%(fwd, i, bandr[i], bandw[i])
        f.write(write_row)

