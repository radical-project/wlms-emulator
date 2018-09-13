import radical.utils as ru

class Task(object):

    def __init__(self, ops=0):

        self._uid = ru.generate_id('task.%(item_counter)04d', ru.ID_CUSTOM)
        self._ops = ops

    @property
    def uid(self):
        return self._uid

    @property
    def ops(self):
        return self._ops

    @ops.setter
    def ops(self, val):
        self._ops = val
