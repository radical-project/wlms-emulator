from calculator.exceptions import *
from calculator.components.executor import Executor
import pytest

def test_executor_init():

    executor = Executor(schedule=list())
    assert executor._uid.split('.')[0] == 'executor'
    assert executor._schedule == list()
    assert executor._tte == 0
    assert executor._util == 0

    with pytest.raises(TypeError):
        executor = Executor()
