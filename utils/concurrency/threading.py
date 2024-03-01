import threading

class threder:
    def __init__(self):
        pass
    
    def run_in_parallel(self, functions):
        threads = []
        for func in functions:
            thread = threading.Thread(target=func)
            threads.append(thread)
            thread.start()
        for thread in threads:
            thread.join()