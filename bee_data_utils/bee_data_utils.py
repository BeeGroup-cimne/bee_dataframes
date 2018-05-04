# coding=utf-8
"""
author: Eloi Gabaldon
"""
import pandas as pd
from pymongo import MongoClient
from create_dataframes import create_hourly_dataframe, create_daily_dataframe
import numpy as np
import datetime

class BeeDataConnection(object):
    def __init__(self, db, host, username, password):
        self.db = db
        self.host = host
        self.username = username
        self.password = password
        self.conn = MongoClient(host)
        self.conn[db].authenticate(self.username, self.password)

    def mongo_query_find_one(self, collection, query, *args):
        if args:
            return self.conn[self.db][collection].find_one(query, *args)
        else:
            return self.conn[self.db][collection].find_one(query)

    def mongo_query_find(self, collection, query, *args):
        if args:
            return self.conn[self.db][collection].find(query, *args)
        else:
            return self.conn[self.db][collection].find(query)

    def get_mongo_consumption(self, modelling_unit):
        # get the data from mongo
        dataframe_list = []
        multiplier = {}
        for device_id in modelling_unit['devices']:
            multiplier[device_id['deviceId']] = device_id['multiplier']
            device_data = self.mongo_query_find("raw_data", {"deviceId": device_id['deviceId']})
            dataframe_temp = []
            for dd in device_data:
                if not dd:
                    continue

                if dd['period'] == "INSTANT":
                    value_size = len(dd['values'])
                    device_df = pd.DataFrame.from_records(
                        {
                            "value": dd['values'],
                            "accumulated": [None] * value_size,
                            "date": dd['timestamps'],
                            "deviceid": [str(dd['deviceId'])] * value_size,
                            "energyType": [modelling_unit["energyType"]
                                           if "energyType" in modelling_unit else "unknown"] * value_size
                        })
                else:
                    value_size = len(dd['values'])
                    device_df = pd.DataFrame.from_records(
                        {
                            "value": [None] * value_size,
                            "accumulated": dd['values'],
                            "date": dd['timestamps'],
                            "deviceid": [str(dd['deviceId'])] * value_size,
                            "energyType": [modelling_unit["energyType"]
                                           if "energyType" in modelling_unit else "unknown"] * value_size
                        })
                    device_df =device_df.set_index('date').sort_index()
                    instant = np.diff(device_df.accumulated)
                    device_df.value = np.append(instant, np.nan)
                    device_df = device_df.reset_index()
                dataframe_temp.append(device_df)
            dataframe_list.append(pd.concat(dataframe_temp).drop_duplicates(subset='date', keep='last'))
        dataframe = pd.concat(dataframe_list)
        return dataframe, multiplier

    def obtain_hourly_dataset(self, modelling_unit_id):
        # get the modelling unit
        modelling_unit = self.mongo_query_find_one('modelling_units', {'modellingUnitId': modelling_unit_id, "companyId": 1092915978})
        # get consumption dataframe
        mongo_consumption, multiplier = self.get_mongo_consumption(modelling_unit)
        consumption_dataframe = create_hourly_dataframe(mongo_consumption.groupby('deviceid'),
                                                        multiplier, modelling_unit['baseline']['model'] if 'model' in
                                                        modelling_unit['baseline'] else 'Weekly30Min')
        consumption_dataframe = consumption_dataframe.sort_index()
        return consumption_dataframe

    def obtain_daily_dataset(self, modelling_unit_id):
        # get the modelling unit
        modelling_unit = self.mongo_query_find_one('modelling_units', {'modellingUnitId': modelling_unit_id, "companyId": 1092915978})
        # get consumption dataframe
        mongo_consumption, multiplier = self.get_mongo_consumption(modelling_unit)
        consumption_dataframe = create_daily_dataframe(mongo_consumption.groupby('deviceid'), multiplier)
        consumption_dataframe = consumption_dataframe.sort_index()
        return consumption_dataframe


    def obtain_daily_dataset_file(self, file_passed, modelling_unit_id):
        modelling_unit = self.mongo_query_find_one('modelling_units', {'modellingUnitId': modelling_unit_id, "companyId": 1092915978})
        multiplier = {x['deviceId']: x['multiplier'] for x in modelling_unit['devices']}
        def parse_time(datetime_s):
            return datetime.datetime.fromtimestamp(float(datetime_s))
        mongo_consumption = pd.read_csv(file_passed, sep='\t', names=["deviceid", "date", "value",
                                                                      "accumulated", "energyType"],
                                        parse_dates=['date'], date_parser= parse_time)
        consumption_dataframe = create_daily_dataframe(mongo_consumption.groupby('deviceid'), multiplier)
        consumption_dataframe = consumption_dataframe.sort_index()
        return consumption_dataframe


    def obtain_hourly_dataset_file(self, file_passed, modelling_unit_id):
        modelling_unit = self.mongo_query_find_one('modelling_units', {'modellingUnitId': modelling_unit_id, "companyId": 1092915978})
        multiplier = {x['deviceId']: x['multiplier'] for x in modelling_unit['devices']}
        def parse_time(datetime_s):
            return datetime.datetime.fromtimestamp(float(datetime_s))
        mongo_consumption = pd.read_csv(file_passed, sep='\t', names=["deviceid", "date", "value",
                                                                      "accumulated", "energyType"],
                                        parse_dates=['date'], date_parser= parse_time)
        consumption_dataframe = create_hourly_dataframe(mongo_consumption.groupby('deviceid'),
                                                        multiplier, modelling_unit['baseline']['model'] if 'model' in
                                                        modelling_unit['baseline'] else 'Weekly30Min')
        consumption_dataframe = consumption_dataframe.sort_index()
        return consumption_dataframe


    def obtain_weather_dataset(self, modelling_unit_id):
        # get the modelling unit
        modelling_unit = self.mongo_query_find_one('modelling_units', {'modellingUnitId': modelling_unit_id})
        # get the station
        station = str(modelling_unit['stationId']) if 'stationId' in modelling_unit else None

        station_doc = self.mongo_query_find_one("stations_measures", {'stationId': station},
                                                {'values': True, 'timestamps':True, })
        if not station_doc:
            raise Exception("Could not find the weather station")
        # create temperature dataframe
        temps = []
        for ts, t in zip(station_doc['timestamps'], station_doc['values']):
            val = {'date': ts, 'temperature': t}
            temps.append(val)

        tdf = pd.DataFrame.from_records(temps, index='date', columns=['temperature', 'date'])
        tdf = tdf.sort_index()
        return tdf
