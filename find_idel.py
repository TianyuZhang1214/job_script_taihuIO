file1 = "/home/export/mount_test/swstorage/results/fn_result_all/fn-usage-high-2017-11-25.csv"
file2 = "/home/export/mount_test/swstorage/results/fn_result_all/fn-usage-idel-2017-11-25.csv"

def read_mnt():
    mnt = []
    f = open(file1,"r") 
    for line in open(file1):
        array = line.split()
        mnt.append(array[0])
    return mnt
def read_idle():

    idle = dict()
    f = open(file2,"r") 
    for line in open(file2):
        array = line.split()
        idle[array[1]] = int(array[0])
    return idle

def get_sum(mnt, idle):
    sum_val = 0
    for mnt_c in mnt:
        sum_val += idle[mnt_c]

    print sum_val

if __name__ == "__main__":
    mnt = read_mnt()
    idle = read_idle()
    get_sum(mnt, idle)

