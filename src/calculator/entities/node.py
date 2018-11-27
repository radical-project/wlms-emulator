import radical.utils as ru
from .task import Task
from ..exceptions import *

class Node(object):

    def __init__(self, perf=0):

        self._uid = ru.generate_id('node')
        self._perf = perf
        self._util = list()
        self._task_history = list()

    @property
    def uid(self):
        return self._uid

    @property
    def perf(self):
        return self._perf

    @property
    def util(self):
        return self._util

    @property
    def task_history(self):
        return self._task_history

    @perf.setter
    def perf(self, val):

        if not (isinstance(val, float) or isinstance(val, int)):
            raise CalcTypeError(expected_type=[float,int], actual_type=type(val))

        self._perf = val

    def execute(self, task):

        if not isinstance(task, Task):
            raise CalcTypeError(expected_type=Task, actual_type=type(task))

        dur = task.ops / self._perf

        if not self._util:
            task.start_time = 0
        else:
            task.start_time = self._util[-1][1]

        task.end_time = task.start_time + dur
        self._util.append([task.start_time, task.end_time])
        self._task_history.append(task.uid)
        task.exec_node = self._uid
