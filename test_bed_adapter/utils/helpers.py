from threading import Event, Thread


class Helpers:
    def __init__(self):
        self.thread_set_interval = None
        pass

    # This functions executes the function function_handler periodically each number of seconds given by sec
    # imitates the functionallity of the nodejs funciton setInterval
    def set_interval(self, function_handler, sec, *args):
        stopped = Event()

        def loop():
            while not stopped.wait(sec):  # the first call is in `interval` secs
                function_handler(*args)
        Thread(target=loop).start()
        return stopped.set
