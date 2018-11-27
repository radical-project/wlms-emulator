from calculator.exceptions import *
from calculator.components.binder import Binder

def test_binder_init():

    bind = Binder()
    assert bind._uid.split('.')[0] == 'binder'
    assert bind._criteria_options == ['rr','tte','util']
    assert bind._criteria == 'tte'
