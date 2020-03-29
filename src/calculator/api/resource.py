import radical.utils as ru
import numpy as np
from ..entities.core import Core
from ..exceptions import CalcTypeError


class Resource(object):

    def __init__(self, num_cores=1, perf_dist='uniform',
                 dist_mean=10, temporal_var=0, spatial_var=0,
                 no_uid=False, data_rate=1, num_edge=1, num_fog=1, num_cloud=1, dist_mean_edge=3, dist_mean_fog=5):

        # Initialize
        self._uid = None
        self._dist_options = ['uniform', 'normal']
        self._core_list = list()

        if not isinstance(num_cores, int):
            raise CalcTypeError(expected_type=int,
                                actual_type=type(num_cores),
                                entity='num_cores'
                                )
        
        if not (isinstance(data_rate, int) or isinstance(data_rate, float)):
            raise CalcTypeError(expected_type='int or float',
                                actual_type=type(data_rate),
                                entity='data_rate'
                                )

        if not isinstance(num_edge, int):
           raise CalcTypeError(expected_type=int,
                               actual_type=type(num_edge),
                               entity='num_edge'
                               )

        if not isinstance(num_fog, int):
           raise CalcTypeError(expected_type=int,
                               actual_type=type(num_fog),
                               entity='num_fog'
                               )

        if not isinstance(num_cloud, int):
           raise CalcTypeError(expected_type=int,
                               actual_type=type(num_cloud),
                               entity='num_cloud'
                               )

        if not isinstance(perf_dist, str):
            raise CalcTypeError(expected_type=str,
                                actual_type=type(num_cores),
                                entity='perf_dist'
                                )

        if perf_dist not in self._dist_options:
            raise ValueError("possible distributions are %s" %
                             (','.join(self._dist_options)))

        if not (isinstance(dist_mean, int) or isinstance(dist_mean, float)):
            raise CalcTypeError(expected_type='int or float',
                                actual_type=type(dist_mean),
                                entity='dist_mean'
                                )

        if not (isinstance(temporal_var, int) or isinstance(temporal_var, float)):
            raise CalcTypeError(expected_type='int or float',
                                actual_type=type(temporal_var),
                                entity='temporal_var'
                                )

        if not (isinstance(spatial_var, int) or isinstance(spatial_var, float)):
            raise CalcTypeError(expected_type='int or float',
                                actual_type=type(spatial_var),
                                entity='spatial_var'
                                )

        self._num_cores = num_cores
        self._perf_dist = perf_dist
        self._dist_mean = dist_mean
        self._temp_var = temporal_var
        self._spat_var = spatial_var
        self._data_rate = data_rate
        self._num_edge = num_edge
        self._num_fog = num_fog
        self._num_cloud = num_cloud
        self._dist_mean_fog = dist_mean_fog
        self._dist_mean_edge = dist_mean_edge

        if not no_uid:
            self._uid = ru.generate_id('resource')

    @property
    def uid(self):
        return self._uid
    
    @property
    def num_cores(self):
        return self._num_cores
    
    @property
    def data_rate(self):
        return self._data_rate

    @property
    def num_edge(self):
        return self._num_edge

    @property
    def num_fog(self):
        return self._num_fog

    @property
    def num_cloud(self):
        return self._num_cloud

    @property
    def core_list(self):
        return self._core_list
    
    def create_core_list(self):

        # Select N samples from the selected distribution
        if self._perf_dist == 'uniform':
            spatial_mean = np.random.uniform(low=self._dist_mean - self._temp_var,
                                             high=self._dist_mean + self._temp_var,
                                             size=1)[0]

            spatial_mean_fog = np.random.uniform(low=self._dist_mean_fog - self._temp_var,
                                             high=self._dist_mean_fog + self._temp_var,
                                             size=1)[0]

            spatial_mean_edge = np.random.uniform(low=self._dist_mean_edge - self._temp_var,
                                             high=self._dist_mean_edge + self._temp_var,
                                             size=1)[0]

            samples_cloud = list(np.random.uniform(low=spatial_mean - self._spat_var,
                                        high=spatial_mean + self._spat_var,
                                        size=self._num_cloud))
            samples_fog = list(np.random.uniform(low=spatial_mean_fog - self._spat_var,
                                        high=spatial_mean_fog + self._spat_var,
                                        size=self._num_fog))
            samples_edge = list(np.random.uniform(low=spatial_mean_edge - self._spat_var,
                                        high=spatial_mean_edge + self._spat_var,
                                        size=self._num_edge))

            samples = samples_cloud + samples_fog+ samples_edge

        elif self._perf_dist == 'normal':
            if self._temp_var:
                spatial_mean = np.random.normal(self._dist_mean,self._temp_var,1)[0]
            else:
                spatial_mean = self._dist_mean

            if self._spat_var:
                samples = list(np.random.normal(spatial_mean, self._spat_var, self._num_cores))
            else:
                samples = [spatial_mean for _ in range(self._num_cores)]
            

        # Create N execution units with the selected samples
        if not self._core_list:
            self._core_list = [Core(abs(samples[i]),self._data_rate)
                               for i in range(self._num_cores)]
        elif self._temp_var:
            for ind, core in enumerate(self._core_list):
                core.perf = abs(samples[ind])
                core.data_rate = self._data_rate

    def to_dict(self):

        core_list_as_dict = list()
        for core in self._core_list:
            core_list_as_dict.append(core.to_dict())

        return {
            'uid': self._uid,
            'num_cores': self._num_cores,
            'perf_dist': self._perf_dist,
            'dist_mean': self._dist_mean,
            'temp_var': self._temp_var,
            'spat_var': self._spat_var,
            'core_list': core_list_as_dict,
            'data_rate': self._data_rate,
            'num_edge' : self._num_edge,
            'num_fog' : self._num_fog,
            'num_cloud' : self._num_cloud,
            'dist_mean_fog' : self._dist_mean_fog,
            'dist_mean_edge' : self._dist_mean_edge 
        }

    def from_dict(self, entry):

        self._uid = entry['uid']
        self._num_cores = entry['num_cores']
        self._perf_dist = entry['perf_dist']
        self._dist_mean = entry['dist_mean']
        self._temp_var = entry['temp_var']
        self._spat_var = entry['spat_var']
        self._data_rate = entry['data_rate']
        self._num_edge = entry['num_edge']
        self._num_fog = entry['num_fog']
        self._num_cloud = entry['num_cloud']
        self._dist_mean_fog = entry['dist_mean_fog']
        self._dist_mean_edge = entry['dist_mean_edge']

        for core in entry['core_list']:
            c = Core(no_uid=True)
            c.from_dict(core)
            self._core_list.append(c)

    def reset(self):

        for n in self._core_list:
            n.task = None
            n.util = list()
