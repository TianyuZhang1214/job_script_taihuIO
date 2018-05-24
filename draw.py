import matplotlib.pyplot as plt

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

def draw_d(x,tag):

    plt.xticks(fontsize=20)
    plt.plot(x, 'r-')
    #plt.plot([0, 35], [average, average], 'r-')
    #plt.xlabel('Runtime (day)', fontsize = 20)
    #plt.ylabel('IO_Time/Runtime ', fontsize = 20)
    plt.xlabel('time', fontsize = 20)
#    label = ["read"]
#    plt.legend(label, loc = 1, ncol = 1)
    plt.show()

