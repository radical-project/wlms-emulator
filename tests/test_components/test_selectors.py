from calculator.components.selectors.task_selector import Task_Selector
from calculator.components.selectors.core_selector import Core_Selector
from calculator.exceptions import *
import pytest

def test_selectors_init():

    sel = Task_Selector()
    assert sel._uid.split('.')[0] == 'selector'
    assert sel._criteria_options == ['all']
    assert sel._criteria == None

    sel = Core_Selector()
    assert sel._uid.split('.')[0] == 'selector'
    assert sel._criteria_options == ['all']
    assert sel._criteria == None

def test_selectors_select():
    sel = Task_Selector()

    with pytest.raises(CalcValueError):
        sel.select([1,2,3,4,5],3)

    with pytest.raises(CalcValueError):
        sel.criteria = 'l2f'

    sel.criteria = 'all'
    assert sel.select([1,2,3,4,5],3) == [1,2,3,4,5]


    sel = Core_Selector()
    with pytest.raises(CalcValueError):
        sel.select([1,2,3,4,5],3)

    with pytest.raises(CalcValueError):
        sel.criteria = 'l2f'

    sel.criteria = 'all'
    assert sel.select([1,2,3,4,5],3) == [1,2,3,4,5]