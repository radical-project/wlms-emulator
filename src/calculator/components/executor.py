import radical.utils as ru
from ..exceptions import *

class Executor(object):

    def __init__(self, schedule):

        self._uid = ru.generate_id('executor')

        if not isinstance(schedule, list):
            raise CalcTypeError(expected_type=list, actual_type=type(schedule))

        self._schedule = schedule
        self._tte = 0
        self._util = 0


    @property
    def schedule(self):
        return self._schedule

    def execute(self):

        for s in self._schedule:
            task = s.keys()[0]
            node = s.values()[0]
            node.execute(task)