pe_file = "../../results_job_data/collect_data/all_data/maxPE1.csv"
max_pe_file = "../../results_job_data/collect_data/all_data/maxPE2.csv"


def read_PE():
    pe_all = dict()
    f = open(pe_file, "r")
    for line in open(pe_file):
        array = line.split()
        if(array[0] in pe_all):
            if(pe_all[array[0]]['read'] < int(array[1])):
                pe_all[array[0]]['read'] = int(array[1])
            if(pe_all[array[0]]['write'] < int(array[2])):
                pe_all[array[0]]['write'] = int(array[2])
        else:
            pe_all[array[0]] = {}
            pe_all[array[0]]['read'] = int(array[1])
            pe_all[array[0]]['write'] = int(array[2])
    return pe_all

def save(pe_all):
    f = open(max_pe_file,'wb')
    for pe in pe_all:
        writerow = "%s %d %d\n"%(pe, pe_all[pe]['read'], pe_all[pe]['write'])
        f.write(writerow)

if __name__ == "__main__":
    pe_all = read_PE()
    save(pe_all)
