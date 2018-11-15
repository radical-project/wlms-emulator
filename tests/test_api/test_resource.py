from calculator.api.resource import Resource
from calculator.exceptions import *
from calculator.entities.node import Node
from calculator.entities.task import Task

def test_resource_init():

    res = Resource(num_nodes=1, dist_value=10, dist_var=2)

    assert res._uid.split('.')[0] == 'resource'
    assert res._dist_candidates == ['uniform', 'normal']
    assert isinstance(res.node_list,list) and len(res.node_list)==1
    assert res.num_nodes == 1
    assert res._perf_dist == 'uniform'
    assert res._dist_value == 10
    assert res._dist_var == 2

def test_resource_create():

    res = Resource(num_nodes=10, dist_value=100, dist_var=10)
    assert len(res._node_list) == 10
    for n in res._node_list:
        assert isinstance(n, Node)

def test_resource_reset():
    res = Resource(num_nodes=1, dist_value=10, dist_var=2)
    t = Task()
    res._node_list[0].task = t
    res._node_list[0].util = [100,120]

    assert res._node_list[0].task == t
    assert res._node_list[0].util == [100,120]

    res.reset()

    assert res._node_list[0].task == None
    assert len(res._node_list[0].util)==0