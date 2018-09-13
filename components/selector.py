import radical.utils as ru
from selection_algos import *

class Selector(object):

    def __init__(self, algorithm=None):

        self._uid = ru.generate_id('selector.%(item_counter)04d', ru.ID_CUSTOM)
        self._algo_candidates = [None,'all']

        self._algo = algorithm

    def select(self, group, n):

        if self._algo == 'all':
            return select_all(group, n)