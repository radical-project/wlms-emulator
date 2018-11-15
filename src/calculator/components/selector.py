import radical.utils as ru
from .algorithms.selection_algos import *
from ..exceptions import *

class Selector(object):

    def __init__(self, metric='all'):

        self._uid = ru.generate_id('selector.%(item_counter)04d', ru.ID_CUSTOM)
        self._metric_candidates = ['all']

        if metric not in self._metric_candidates:
            raise CalcMissingError(obj=self._uid, missing_attribute='metric')
        self._metric = metric

    def select(self, collection, count):

        if not self._metric:
            return select_all(collection, count)