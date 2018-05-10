#coding:utf-8
import os
import sys 
sys.path.append("../../")
#from mail import send_mail
import time

cmd = 'ps -aux|grep save_singlejob_of_program_file.py'
cmd_kill = 'kill -9 '

def exec_cmd():
    result = os.popen(cmd)
    res = result.read()
    
    return res

def deal_result(res):
    for line in res.splitlines():
        if ('save_singlejob_of_program_file.py' in line):
#            print (line.split())[1]
            thread_ID = (line.split())[1]
            cmd_kill_thread = cmd_kill + thread_ID
            print cmd_kill_thread
            result_kill = os.popen(cmd_kill_thread)
            print result_kill.read()

if __name__ == "__main__":
    cmd_res = exec_cmd()
    deal_result(cmd_res)



