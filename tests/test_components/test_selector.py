from calculator.exceptions import *
from calculator.components.selector import Selector

def test_selector_init():

    sel = Selector()
    assert sel._uid.split('.')[0] == 'selector'
    assert sel._criteria_options == ['all']
    assert sel._criteria == 'all'

def test_selector_select():
    sel = Selector()
    assert sel.select([1,2,3,4,5],3) == [1,2,3,4,5]