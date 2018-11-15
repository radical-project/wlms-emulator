import radical.utils as ru

class Task(object):

    def __init__(self, ops=0):

        self._uid = ru.generate_id('task')
        self._ops = ops
        self._start_time = None
        self._end_time = None
        self._exec_node = None

    @property
    def uid(self):
        return self._uid

    @property
    def ops(self):
        return self._ops

    @property
    def start_time(self):
        return self._start_time

    @property
    def end_time(self):
        return self._end_time

    @property
    def exec_node(self):
        return self._exec_node

    @ops.setter
    def ops(self, val):
        self._ops = val

    @start_time.setter
    def start_time(self, val):
        self._start_time = val

    @end_time.setter
    def end_time(self, val):
        self._end_time = val

    @exec_node.setter
    def exec_node(self, val):
        self._exec_node = val
