import multiprocessing


class Process:
    def __init__(self):
        pass
    
    def run_in_parallel(self, functions):
        processes = []
        for func in functions:
            process = multiprocessing.Process(target=func)
            processes.append(process)
            process.start()
        for process in processes:
            process.join()