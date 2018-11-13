# coding=utf-8

"""
author: Eloi Gabaldon
"""

import pandas as pd
import numpy as np
from datetime import timedelta
from dateutil.relativedelta import relativedelta
import bee_data_cleaning as dc


# haig de buscar el device a reemplazar i per les dates a eliminar les haig de posar a 0
# i haig de extendre les dades (ojo si es accumulative a valor igual que el primer element)
# per evitar que la suma dels devices sigui nan
def meter_replacement(modelling_unit, company, df, mongo_modellingUnits):
    modellingUnit_doc = mongo_modellingUnits.find_one({'modellingUnitId': modelling_unit, "companyId": company})
    devices = modellingUnit_doc['devices']
    for dev in devices:
        if 'replacementDeviceId' in dev:
            deprecatedDeviceId = dev['deviceId']
            replacedByDeviceId = dev['replacementDeviceId']
            dateStart = dev['replacementDate']

            df_1 = df[df.deviceid == deprecatedDeviceId]
            df_1 = df_1[df_1.index < dateStart]

            df_2 = df[df.deviceid == replacedByDeviceId]
            df_2 = df_2[df_2.index >= dateStart]

            df_new = df_2.merge(df_1, how='outer', left_index=True, right_index=True)

            # extend constant values, new consumption values to 0 and accumulative values the same value
            for i in df_new.columns:
                if 'value' in i:
                    v = df_new[i].dropna().unique().tolist()
                    if len(v) >= 1:
                        df_new[i].fillna(0, inplace=True)
                    else:
                        pass
                else:
                    df_new[i].fillna(method='bfill', inplace=True)
                    df_new[i].fillna(method='ffill', inplace=True)

            df_new.reset_index(inplace=True)

            # split df and rename columns
            df1 = pd.DataFrame.from_records({
                'date': df_new['date'],
                'accumulated': df_new['accumulated_x'],
                'value': df_new['value_x'],
                'deviceid': df_new['deviceid_x'],
                'energyType': df_new['energyType_x']
            })
            df1.set_index('date', inplace=True)

            df2 = pd.DataFrame.from_records({
                'date': df_new['date'],
                'accumulated': df_new['accumulated_y'],
                'value': df_new['value_y'],
                'deviceid': df_new['deviceid_y'],
                'energyType': df_new['energyType_y']
            })
            df2.set_index('date', inplace=True)

            df = df[df['deviceid'] != replacedByDeviceId]
            df = df[df['deviceid'] != deprecatedDeviceId]
            df = pd.concat([df, df1, df2])

    return df


def calculate_frequency(dataset):
    if len(dataset.index) > 1:
        return (pd.Series(dataset.index[1:]) - pd.Series(dataset.index[:-1])).value_counts().index[0]
    else:
        return None


def daily_data(df):
    """
    Divide the monthly consumption in days of the month to obtain the natural monthly consumption
    """
    new_ts = []
    new_consumption = []
    ts_ini = 0
    for ts, value in df.iterrows():
        consumption = value.value
        if ts_ini != 0:
            days = (ts - ts_ini).days
            daily_consumption = consumption/days
            for i in range(1,days+1):
                new_ts.append(ts_ini + relativedelta(days=i))
                new_consumption.append(daily_consumption)
        ts_ini = ts

    new_df = pd.DataFrame.from_records({'value':new_consumption, 'date':new_ts}, index='date')
    return new_df


def create_hourly_dataframe(grouped, multiplier, model):
    if model == 'Weekly30Min':
        frequ = 30
        window = 1440
    else:
        frequ = 60
        window = 720
    df_new = None
    for name, group in grouped:
        if name not in multiplier.keys():
            continue
        energy_type_grouped = group.groupby('energyType')
        for energy_type, energy_type_group in energy_type_grouped:
            group_new = energy_type_group.reset_index().drop_duplicates(subset='date', keep='last').set_index('date')
            group_new = group_new.sort_index()
            freq = calculate_frequency(group_new)
            if not freq:
                continue
            day_delta = timedelta(hours=1)
            if freq > day_delta:
                pass
            else:
                # si no existeix algun valor instantani al device ho considero accumulated
                if group_new.value.isnull().all():
                    df_0 = pd.DataFrame(group_new.accumulated * abs(multiplier[name])).resample(str(frequ)+'T').max().\
                        interpolate().diff(1,0).rename(columns={'accumulated': 'value'})
                    df_0.loc[df_0.value < 0] = np.nan  # negative values to nan
                    sign = abs(multiplier[name])/multiplier[name] if multiplier[name] != 0 else 0
                    if df_new is not None:
                        df_new = df_new + df_0 * sign
                    else:
                        df_new = df_0 * sign
                else:
                    if df_new is not None:
                        df_new = df_new + pd.DataFrame(group_new.value * multiplier[name]).\
                            resample(str(frequ)+'T').sum()
                    else:
                        df_new = pd.DataFrame(group_new.value * multiplier[name]).resample(str(frequ)+'T').sum()
    if df_new is not None:
        outliers = dc.detect_znorm_outliers(df_new['value'], 30, mode="global")
        df_new['value'] = dc.clean_series(df_new['value'], outliers)
        outliers = dc.detect_znorm_outliers(df_new['value'], 30, mode="rolling", window=window)
        df_new['value'] = dc.clean_series(df_new['value'], outliers)
        outliers = dc.detect_min_threshold_outliers(df_new['value'], 0)
        df_new['value'] = dc.clean_series(df_new['value'], outliers)
        outliers = dc.detect_max_threshold_outliers(df_new['value'], 100000)
        df_new['value'] = dc.clean_series(df_new['value'], outliers)
        outliers = dc.detect_znorm_outliers(df_new['value'], 30, mode="global")
        df_new['value'] = dc.clean_series(df_new['value'], outliers)
        outliers = dc.detect_znorm_outliers(df_new['value'], 30, mode="rolling", window=window)
        df_new['value'] = dc.clean_series(df_new['value'], outliers)

    return df_new


def create_daily_dataframe(grouped, multiplier):
    freq_month = 1440
    df_new_monthly = None
    df_new_hourly = None
    for name, group in grouped:
        if name not in multiplier.keys():
            continue
        # If the model is monthly, we need to convert the tertiary energy types to daily by dividing and the hourly
        # energy to daily by adding. We will identify the monthly energy type by the frquency of the timestamps.
        energy_type_grouped = group.groupby('energyType')
        for energy_type, energy_type_group in energy_type_grouped:
            group_new = energy_type_group.reset_index().drop_duplicates(subset='date', keep='last').set_index('date')
            group_new = group_new.sort_index()
            freq = calculate_frequency(group_new)
            if not freq:
                continue
            day_delta = timedelta(hours=1)
            if freq > day_delta:  # monthly data
                # si no existeix algun valor instantani al device ho considero accumulated
                if group_new.value.isnull().all():
                    if df_new_monthly is not None:
                        # paso el consum a diari per sumar
                        df_new_monthly = df_new_monthly + pd.DataFrame(group_new.accumulated * multiplier[name]).\
                            resample(str(freq_month)+'T').interpolate().diff(1, 0).\
                            rename(columns={'accumulated': 'value'})
                    else:
                        df_new_monthly = pd.DataFrame(group_new.accumulated * multiplier[name]).\
                            resample(str(freq_month)+'T').interpolate().diff(1, 0).\
                            rename(columns={'accumulated': 'value'})
                else:
                    df_daily = daily_data(pd.DataFrame(group_new.value * multiplier[name]))
                    if df_new_monthly is not None:
                        # paso el consum a diari per sumar
                        df_new_monthly = df_new_monthly + df_daily
                    else:
                        df_new_monthly = df_daily

            else:  # hourly data
                if group_new.value.isnull().all():
                    df_0 = pd.DataFrame(group_new.accumulated * multiplier[name]).resample(str(freq_month)+'T').max().\
                        interpolate().diff(1, 0).rename(columns={'accumulated': 'value'}).clip_lower(0)
                    sign = abs(multiplier[name])/multiplier[name] if multiplier[name] != 0 else 0
                    if df_new_hourly is not None:
                        df_new_hourly = df_new_hourly + df_0 * sign

                    else:
                        df_new_hourly = df_0 * sign
                else:
                    df_daily = pd.DataFrame(group_new.value * multiplier[name])
                    outliers = dc.detect_min_threshold_outliers(df_daily['value'], 0)
                    df_daily['value'] = dc.clean_series(df_daily['value'], outliers)
                    outliers = dc.detect_znorm_outliers(df_daily['value'], 30, mode="global")
                    df_daily['value'] = dc.clean_series(df_daily['value'], outliers)
                    df_daily = df_daily.groupby(pd.Grouper(freq=str(freq_month)+'T')).sum()
                    if df_new_hourly is not None:
                        df_new_hourly = df_new_hourly + df_daily
                    else:
                        df_new_hourly = df_daily
    # concat the 2 dataframes (from monthly and hourly)
    if all([df_new_monthly is not None, df_new_hourly is not None]):
        df_new = df_new_hourly.append(df_new_monthly).reset_index().\
            drop_duplicates(subset="date", keep="first").set_index("date")
    elif df_new_monthly is not None:
        df_new = df_new_monthly
    elif df_new_hourly is not None:
        df_new = df_new_hourly
    else:
        df_new = pd.DataFrame({'date': []})
    if not df_new.empty:
        outliers = dc.detect_min_threshold_outliers(df_new['value'], 0)
        df_new['value'] = dc.clean_series(df_new['value'], outliers)
        outliers = dc.detect_znorm_outliers(df_new['value'], 30, mode="rolling", window=12)
        df_new['value'] = dc.clean_series(df_new['value'], outliers)
    return df_new
