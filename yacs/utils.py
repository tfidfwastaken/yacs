from threading import Lock
from enum import IntEnum


class Status(IntEnum):
    SUCCESS = 1
    UNSCHEDULED = 2
    SCHEDULED = 3
    FAILED = 4


class TaskPool():
    def __init__(self, tasks):
        self.tasks = tasks
        self.lock = Lock()

    def get(self, func):
        """Returns first task that matches condition in func"""
        with self.lock:
            for task in self.tasks:
                if func(task):
                    return task
        
    # TODO: this is not a clean way to do it, breaks separation
    # of concerns. Works for now though.
    def take(self, n):
        if n < len(self.tasks):
            with self.lock:
                taskpool = TaskPool(self.tasks[:n])
                for task in taskpool.tasks:
                    task['status'] = Status.SCHEDULED
                return taskpool
        else:
            with self.lock:
                for task in self.tasks:
                    task['status'] = Status.SCHEDULED
                return self

    def filter(self, func):
        with self.lock:
            filtered = [task for task in self.tasks if func(task)]
            return TaskPool(filtered)

    def remove(self, func):
        with self.lock:
            self.tasks = [item for item in self.tasks if not func(item)]

    def update(self, cond_func, transform):
        """Updates items in the task pool
        
        For all items in the task pool satisfying `cond_func`,
        update using the transforming function `transform`
        """
        with self.lock:
            for task in self.tasks:
                if cond_func(task):
                    # this mutation needs fixing, inconsistent with the API
                    transform(task)
    
    def is_empty(self):
        with self.lock:
            if self.tasks == []:
                return True
            else:
                return False
