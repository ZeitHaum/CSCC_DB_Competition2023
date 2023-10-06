import multiprocessing as mp
import subprocess
import time

MAX_ITER_NUM = 1
PROCESS_NUM = 32
RECOVER_ITER_NUM = 1
is_exit = False
is_auto_start_server = False

def write_assert():
    with open('assert.txt', "a+") as f:
        f.write("Assert Occurred.\n")
        
def server_run():
    try:
        subprocess.check_output(["sh", "server.sh"])
    except subprocess.CalledProcessError as e:
        exit_status = e.returncode
        if exit_status == 1:
            pass
        else:
            write_assert()
    except AssertionError:
        write_assert()
        pass
            

def client_run(is_recovery, is_first, sql_file_name, now_iter):
    #waiting for server opening
    sleep_time = 2 + now_iter #more time for recovery.
    time.sleep(sleep_time)
    is_first_arg = list(["0", "1"])[int(is_first)]
    is_recovery_arg = list(["0", "1"])[int(is_recovery)]
    if(is_first == True or is_recovery == True):
        subprocess.run(["sh", "random_test.sh", is_first_arg, sql_file_name, is_recovery_arg])
    else:
        subprocess.run(["sh", "random_test.sh", is_first_arg, sql_file_name, is_recovery_arg])
    
def main_test():
    process_file_name = [f"test_checker_{i}.sql" for i in range(PROCESS_NUM)]
    if(is_auto_start_server):
        subprocess.run(["rm", "-rf", "testdb"], shell=False)
    is_recovery = False
    for i in range(RECOVER_ITER_NUM):#恢复
        process_list = []
        if(is_auto_start_server):
            server_process = mp.Process(target=server_run, args=())
        client_process = mp.Process(target=client_run, args=(is_recovery, True, process_file_name[0],i))
        process_list.append(client_process)
        for j in range(1, PROCESS_NUM):
            process_list.append(mp.Process(target=client_run, args=(False, False, process_file_name[j],i)))
        if(is_auto_start_server):
            server_process.start()
        process_list[0].start()
        time.sleep(240)
        for i in range(1, PROCESS_NUM):
            process_list[i].start()
        for i in range(PROCESS_NUM):
            process_list[i].join() 
        if(is_auto_start_server):
            server_process.join()
        if(is_recovery == False):
            is_recovery = True


if __name__ == "__main__":
    for i in range(MAX_ITER_NUM):
        main_test()