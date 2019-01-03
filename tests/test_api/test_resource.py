from calculator.api.resource import Resource
from calculator.exceptions import *
from calculator.entities.core import Core
from calculator.entities.task import Task


def test_resource_init():

    res = Resource()

    assert res._uid.split('.')[0] == 'resource'
    assert res._dist_options == ['uniform', 'normal']
    assert isinstance(res.core_list, list) and len(res.core_list) == 0
    assert res.num_cores == 1
    assert res._perf_dist == 'uniform'
    assert res._dist_mean == 10
    assert res._temp_var == 0
    assert res._spat_var == 0

    res = Resource(no_uid=True)
    assert res._uid == None

def test_resource_create():

    res = Resource( num_cores=10, dist_mean=100, 
                    temporal_var=10, spatial_var=5)
    res.create_core_list()
    l1 = res._core_list
    assert len(l1) == 10
    for n in l1:
        assert isinstance(n, Core)

def test_resource_reset():
    res = Resource(num_cores=1, dist_mean=10, temporal_var=2, spatial_var=1)
    res.create_core_list()

    t = Task()
    res._core_list[0].task = t
    res._core_list[0].util = [100, 120]

    assert res._core_list[0].task == t
    assert res._core_list[0].util == [100, 120]

    res.reset()

    assert res._core_list[0].task == None
    assert len(res._core_list[0].util) == 0


def test_resource_to_dict():

    res = Resource()
    tmp = res.to_dict()

    assert tmp['uid'] == res._uid
    assert tmp['num_cores'] == res.num_cores
    assert tmp['perf_dist'] == res._perf_dist
    assert tmp['dist_mean'] == res._dist_mean
    assert tmp['temp_var'] == res._temp_var
    assert tmp['spat_var'] == res._spat_var

    tmp_list = list()
    for core in res.core_list:
        tmp_list.append(core.to_dict())

    assert tmp_list == tmp['core_list']


    res = Resource(num_cores=10, dist_mean=100, temporal_var=10, spatial_var=5)
    res.create_core_list()
    tmp = res.to_dict()

    assert tmp['uid'] == res._uid
    assert tmp['num_cores'] == res.num_cores
    assert tmp['perf_dist'] == res._perf_dist
    assert tmp['dist_mean'] == res._dist_mean
    assert tmp['temp_var'] == res._temp_var
    assert tmp['spat_var'] == res._spat_var

    tmp_list = list()
    for core in res.core_list:
        tmp_list.append(core.to_dict())

    assert tmp_list == tmp['core_list']

def test_resource_from_dict():

    tmp = {
            'uid': 'resource.1234',
            'num_cores': 10,
            'perf_dist': 'uniform',
            'dist_mean': 20,
            'temp_var': 5,
            'spat_var': 2,
            'core_list': list()
        }

    for x in range(tmp['num_cores']):
        tmp['core_list'].append(Core(perf=20).to_dict())

    res = Resource(no_uid=True)
    res.from_dict(tmp)

    assert tmp['uid'] == res._uid
    assert tmp['num_cores'] == res.num_cores
    assert tmp['perf_dist'] == res._perf_dist
    assert tmp['dist_mean'] == res._dist_mean
    assert tmp['temp_var'] == res._temp_var
    assert tmp['spat_var'] == res._spat_var

    tmp_list = list()
    for core in res.core_list:
        tmp_list.append(core.to_dict())

    assert tmp_list == tmp['core_list']


def test_resource_exceptions():
    pass