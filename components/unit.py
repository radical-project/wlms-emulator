import radical.utils as ru

class ExecUnit(object):

    def __init__(self, perf=0):

        self._uid = ru.generate_id('unit.%(item_counter)04d', ru.ID_CUSTOM)
        self._perf = perf

    @property
    def uid(self):
        return self._uid

    @property
    def perf(self):
        return self._perf

    @perf.setter
    def perf(self, val):
        self._perf = val
