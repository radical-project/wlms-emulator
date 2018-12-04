import matplotlib as plot
import radical.utils as ru
import pandas as pd

if __name__ == '__main__':
    data = ru.read_json('./profile.executor.0000.json')
    df = pd.DataFrame.from_dict(data)
    print df