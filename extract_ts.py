import pandas as pd
import datetime
import math
from tqdm import tqdm
####LOAD  EVOL AS DATAFRAME
evol = pd.read_csv('DF_EVOLUCION_LIMPIA_COMPLETA.csv')
# Get the index of rows that contain 'SALARIO' in the 'Salary' column
index_to_drop = evol[evol['FECHA_ULTIMA_GESTION'] == 'SALARIO'].index

# Drop the rows using the index
evol.drop(index_to_drop, inplace=True)
#### SET OF IDS
ids = set(evol['IDENTIFICACION'])


def time_series(id):
    assert id in ids, 'The id does not exist in EVOLUCION'
    ex = evol[evol['IDENTIFICACION'] == id].copy()
    ex['FECHA_ULTIMA_GESTION'] = pd.to_datetime(ex['FECHA_ULTIMA_GESTION'])
    ts = ex[['FECHA_ULTIMA_GESTION', 'PAGOS_MES', 'OBLIGACION']]
    ts = ts.dropna(subset=['FECHA_ULTIMA_GESTION'])
    ts.set_index('FECHA_ULTIMA_GESTION', inplace=True)
    ts = ts.sort_values(by='FECHA_ULTIMA_GESTION')

    grouped = ts.groupby('OBLIGACION')
    time_series = {}

    # Loop over the groups and store each time series in the dictionary
    for name, group in grouped:
        time_series[str(name)] = group['PAGOS_MES']
    return time_series


def months(start_date, end_date):
    # extract year and month from date1
    year1 = start_date.year
    month1 = start_date.month

    # extract year and month from date2
    year2 = end_date.year
    month2 = end_date.month
    assert year1 <= year2, 'the time series is not sorted'
    if year1 == year2:
        return month2 - month1
    else:
        return (12 - month1) + month2 + (12 * (year2 - year1 - 1))



def duration(ts):
    max_count = 0
    count = 0
    start = None
    end = None
    prev_date = None
    for i, val in enumerate(ts):
        curr_date = ts.index[i]
        if val != 0:
            # check for consecutive dates
            if prev_date is not None and months(prev_date, curr_date) > 1:
                # dates are not consecutive, reset counter and start/end dates
                if count > max_count:
                    max_count = count
                    max_end = end
                count = 1
                start = None
                end = None
            else:
                count += 1
                if start is None:
                    start = curr_date
                end = curr_date
        else:
            if count > max_count:
                max_count = count
                max_start = start
                max_end = end
            count = 0
            start = None
            end = None
        prev_date = curr_date
    if count > max_count:
        max_count = count
        max_start = start
        max_end = end
    return max_count


dura_dict = {str(cc): max([0]+[duration(t) for t in time_series(cc).values()]) for cc in tqdm(ids, colour='green')}


ts = time_series(13511178)
t = ts['1000000000001829.0']


max_key = max(dura_dict, key=dura_dict.get)
duration(t)