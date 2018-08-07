fwd_file = '../../results/fn_result_all/fn-usage-high.csv'

def read_fwd_usage():
    f = open(fwd_file, "r")
    cnt = [0 for i in range(5)]
    cnt_tmp = 1
    date = ''
    fwd = 0
    sec = 0
    for line in open(fwd_file):
        array = line.split()
        if(date == array[0] and fwd == int(array[1]) and sec + 1 == int(array[2])):
            cnt_tmp += 1
            date = array[0]
            fwd = int(array[1])
            sec = int(array[2])
        else:
            date = array[0]
            fwd = int(array[1])
            sec = int(array[2])
            if(cnt_tmp <= 5):
                cnt[0] += 1
            elif(cnt_tmp > 5 and cnt_tmp <= 10):
                cnt[1] += 1
            elif(cnt_tmp > 10 and cnt_tmp <= 30):
                cnt[2] += 1
            elif(cnt_tmp > 30 and cnt_tmp <= 60):
                cnt[3] += 1
            elif(cnt_tmp > 60):
                cnt[4] += 1
            cnt_tmp = 1

    sum_val = sum(cnt)
    print "Burst interval (0, 5] count: %d percent: %0.2f."%(cnt[0], 100*cnt[0]*1.0/sum_val)
    print "Busrs interval (5, 10] count: %d percent: %0.2f."%(cnt[1], 100*cnt[1]*1.0/sum_val)
    print "Busrs interval (10, 30] count: %d percent: %0.2f."%(cnt[2], 100*cnt[2]*1.0/sum_val)
    print "Busrs interval (30, 60] count: %d percent: %0.2f."%(cnt[3], 100*cnt[3]*1.0/sum_val)
    print "Busrs interval (60,inf] count: %d percent: %0.2f."%(cnt[4], 100*cnt[4]*1.0/sum_val)




if __name__ == "__main__":

    read_fwd_usage()





