import radical.utils as ru
from threading import Thread
from time import sleep, time
from .task import Task
from ..exceptions import *

class Node(object):

    def __init__(self, perf=0):

        self._uid = ru.generate_id('node')
        self._perf = perf
        self._util = list()
        self._task = None
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
    def task(self):
        return self._task

    @property
    def task_history(self):
        return self._task_history

    @perf.setter
    def perf(self, val):

        if not (isinstance(val, float) or isinstance(val, int)):
            raise CalcTypeError(expected_type=[float,int], actual_type=type(val))

        self._perf = val

    @util.setter
    def util(self, val):

        if not isinstance(val, list):
            raise CalcTypeError(expected_type=list, actual_type=type(val))

        self._util = val

    @task.setter
    def task(self, val):

        self._task = val

    def execute(self):

        if not self._task:
            raise CalcMissingError(obj=self._uid, missing_attribute='task')

        dur = self._task.ops / self._perf
        self._task.start_time = time()
        self._task.end_time = self._task.start_time + dur
        self._util.append([self._task.start_time, self._task.end_time])
        self._task_history.append(self._task.uid)
        self._task.exec_node = self._uid
