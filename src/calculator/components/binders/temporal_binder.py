import radical.utils as ru
from ..algorithms.temporal_binding_algos import fastest_first, slowest_first, random_order
from ...exceptions import CalcValueError


class Temporal_Binder(object):

    def __init__(self):

        self._uid = ru.generate_id('binder')
        self._criteria_options = ['ff', 'sf', 'random']
        self._criteria = None
        self._schedule = None

    @property
    def criteria(self):
        return self._criteria

    @criteria.setter
    def criteria(self, val):
        if val not in self._criteria_options:
            raise CalcValueError(obj=self._uid, attribute='criteria',
                                 expected_value=self._criteria_options,
                                 actual_value=val)

        self._criteria = val

    def bind(self, workload):

        if not self._criteria:
            raise CalcValueError(obj=self._uid, attribute='criteria',
                                 expected_value=self._criteria_options,
                                 actual_value=None)

        if self._criteria == 'ff':
            self._schedule = fastest_first(workload)

        elif self._criteria == 'sf':
            self._schedule = slowest_first(workload)

        elif self._criteria == 'random':
            self._schedule = random_order(workload)

        return self._schedule
