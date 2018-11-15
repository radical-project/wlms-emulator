from calculator.entities.node import Node
from calculator.entities.task import Task
from calculator.exceptions import *
import pytest
from hypothesis import given, strategies as st

def test_node_init():

    n = Node()
    assert n.uid.split('.')[0] == 'node'
    assert n.perf == 0
    assert isinstance(n.util, list)
    assert n.task == None
    assert isinstance(n.task_history, list) and len(n.task_history)==0

    n = Node(perf=10)
    assert n.perf == 10
    n.perf = 20
    assert n.perf == 20

def test_node_execute():

    n = Node(perf=10)
    t = Task(ops=100)
    n.task = t
    n.execute()

    assert n._util == [[t.start_time, t.end_time]]
    assert n._task_history == [t.uid]

@given(s=st.text(), l=st.lists(st.integers()), i=st.integers().filter(lambda x: type(x) == int), f=st.floats())
def test_node_exceptions(s,l,i,f):

    data_type = [s,i,l,f]
    for data in data_type:

        if not isinstance(data, float) and not isinstance(data, int):

            with pytest.raises(CalcTypeError):
                n = Node()
                n.perf = data

        if not isinstance(data, list):

            with pytest.raises(CalcTypeError):
                n = Node()
                n.util = data


    with pytest.raises(CalcMissingError):
        n = Node()
        n.execute()
    
