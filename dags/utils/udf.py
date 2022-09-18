from datetime import datetime as dt

###########################################################
# Desired places
# # Maitland airport
maitland_airport = 'maitland_airport'
# # Newcastle Nobbys Signal station
newcastle_nobbys_signal_station = 'newcastle_nobbys_signal_station'
# # Paterson (local)
paterson = 'paterson_(tocal)'
# # Scone airport
scone_airport = 'scone_airport'
# # Williamtown RAAF
williamtown_raaf = 'williamtown_raaf'
places = [maitland_airport, newcastle_nobbys_signal_station,
          paterson, scone_airport, williamtown_raaf]


def process_file_monthly(filename):
    import pandas as pd
    # import numpy as np
    # # Function to keep original column names
    # df = pd.read_csv(
    #     filename, skiprows=9,
    #     encoding='unicode_escape',
    #     header=[0, 1, 2, 3]
    # )
    # coltest = np.array(df.columns).T
    # colnames = []
    # for x in coltest:
    #     colname_list = []
    #     for y in x:
    #         if 'Unnamed' not in y:
    #             colname_list.append(y)
    #         colname = ' '.join(colname_list)
    #     # print(colname)
    #     colnames.append(colname)
    # # Rewrite column names to fit snowflake table name
    colnames = ['station_name', 'date',
                'evapo_transpiration', 'rain',
                'pan_evaporation', 'max_temp', 'min_temp',
                'max_relative_hum', 'min_relative_hum',
                'avg_10m_wind_speed', 'solar_radiation']
    df = pd.read_csv(
        filename, skiprows=13,
        encoding='unicode_escape',
        names=colnames)
    # Drop Total Row
    df.drop(df.index[-1], inplace=True)
    return df


def process_file_daily(filename, ytd: dt):
    import pandas as pd
    colnames = ['station_name', 'date',
                'evapo_transpiration', 'rain',
                'pan_evaporation', 'max_temp', 'min_temp',
                'max_relative_hum', 'min_relative_hum',
                'avg_10m_wind_speed', 'solar_radiation']
    df = pd.read_csv(
        filename, skiprows=13,
        encoding='unicode_escape',
        names=colnames)
    # Drop Total Row
    df.drop(df.index[-1], inplace=True)
    # Get Target Row
    target_row = df[df["date"] == ytd.strftime("%d/%m/%Y")]
    return target_row


def get_month(file_name: str):
    _time = (file_name.split(".")[0].split("-")[1])
    _month = int(_time[4:])
    _year = int(_time[:4])
    return dt(_year, _month, 1)
