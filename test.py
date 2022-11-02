import multiprocessing
from multiprocessing import Manager

def worker(procnum, returns):
    '''worker function'''
    print(str(procnum) + ' represent!')
    returns.append(procnum)
    return returns

if __name__ == '__main__':
    manager = Manager()
    return_list = manager.list() #也可以使用列表dict
    # returns = []
    jobs = []
    for i in range(5):
        p = multiprocessing.Process(target=worker, args=(i, return_list))
        jobs.append(p)
        p.start()

    for proc in jobs:
        proc.join()
    # 也可以单独等待每个线程介绍，这样就能知道每个进程对应的返回值的，使用上面的方法只是得到所有进程的返回值
    print(return_list)