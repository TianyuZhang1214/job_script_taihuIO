import matplotlib.pyplot as plt
fwd_file = "../../results_job_data/job_trace/42340081/fwd.csv"
front_file = "../../results_job_data/job_trace/42340081/front_end_bw.csv"
back_file = "../../results_job_data/job_trace/42340081/back_end_agg.csv"

def read_value(file_name):
    f = open(file_name, "r")
    bw_r = []
    bw_w = []
    for line in open(file_name):
        array = line.split()
        bw_r.append(float(array[0]))
        bw_w.append(float(array[1]))

    return bw_r, bw_w

def draw_2d(x, y, tag):

    plt.xticks(fontsize=20)
    plt.yticks(fontsize=20)
    plt.plot(x, 'r-')
    plt.plot(y, 'g-')
    #plt.plot([0, 35], [average, average], 'r-')
    #plt.xlabel('Runtime (day)', fontsize = 20)
    #plt.ylabel('IO_Time/Runtime ', fontsize = 20)
    plt.xlabel('time', fontsize = 20)
    plt.ylabel(tag, fontsize = 20)
    label = ["read", "write"]
    plt.legend(label, loc = 1, ncol = 1)
    plt.show()

if __name__ == "__main__":

    fwd_bw_r, fwd_bw_w = read_value(fwd_file)
    front_bw_r, front_bw_w = read_value(front_file)
    back_bw_r, back_bw_w = read_value(back_file)
    print fwd_bw_r
    print front_bw_r
    print back_bw_r
    draw_2d(fwd_bw_r, fwd_bw_w, 'fwd')
    draw_2d(front_bw_r, front_bw_w, 'front-end')
    draw_2d(back_bw_r, back_bw_w, 'back-end')



