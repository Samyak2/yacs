import threading


class Worker:
    """Stores worker details as well as a semaphore
    counting the number of slots left

    :param id: worker id
    :param host: hostname or IP of worker
    :param port: port of worker
    :num_slots: total number of slots
    """

    def __init__(self, id_, host, port, num_slots):
        self.id = id_
        self.host = host
        self.port = port
        self.totalSlots = num_slots
        self.slots = threading.BoundedSemaphore(num_slots)

    def delegateTask(self):
        """Decrements the number of available slots by one
        (blocks until a slot is available)
        """
        return self.slots.acquire()

    def finishTask(self):
        """Increments the number of available slots"""
        return self.slots.release()

    def __repr__(self):
        return (
            f"<Worker id={self.id}"
            f" {self.host}:{self.port}"
            f" slots={self.totalSlots}>"
        )
