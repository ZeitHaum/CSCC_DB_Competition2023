import subprocess
import multiprocessing
import os

def run_server():
    # 创建命名管道
    pipe_name = 'my_pipe'
    os.mkfifo(pipe_name)
    
    # 执行服务器端脚本，从命名管道中读取数据
    subprocess.run(["rm", "-rf" ,"testdb"])
    subprocess.run(["./build/bin/rmdb", "testdb", pipe_name], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    
    # 删除命名管道
    os.remove(pipe_name)

def run_client():
    # ... 执行客户端脚本，将数据写入命名管道
    subprocess.run(["cd", "./SampleGen/"])
    subprocess.run(["python3" ,"sqlGenerator.py"])
    subprocess.run(["cd", ".."])
    subprocess.run(["cd", ".."])
    subprocess.run(["cd", "rmdb_client/build"])
    subprocess.run(["./rmdb_client", "<", "/mnt/c/0xSelf/DBContest/db2023_updatemaster_commit/SampleGen/test_insert.sql"],  stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)

# 创建进程
server_process = multiprocessing.Process(target=run_server)
client_process = multiprocessing.Process(target=run_client)

# 启动进程
server_process.start()
client_process.start()

# 等待进程结束
server_process.join()
client_process.join()
