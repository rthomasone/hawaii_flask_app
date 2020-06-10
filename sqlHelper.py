import datetime as dt
from dateutil.relativedelta import *
import numpy as np
import pandas as pd
from sqlalchemy import create_engine


class SQLHelper():

    def __init__(self):
        self.connection_string = "sqlite:///Resources/hawaii.sqlite"
        self.engine = create_engine(self.connection_string)


    #return all stations
    def getAllStationDetails(self):
        query = f"""
                SELECT 
                    station as station_ID,
                    name as station_name,
                    latitude,
                    longitude,
                    elevation
                FROM 
                    station
                ORDER BY
                    station_ID ASC;
                """

        conn = self.engine.connect()
        df = pd.read_sql(query, conn)
        conn.close()

        return df


    #return precipation aggregated by date
    def getTotalPrecipOnDate(self):
        query = f"""
                SELECT 
                    date,
                    sum(prcp) as Total_Precipitation
                FROM 
                    measurement
                GROUP BY
                    date
                ORDER BY
                    date ASC;
                """

        conn = self.engine.connect()
        df = pd.read_sql(query, conn)
        conn.close()

        return df


    #return previous year of temperature data for most active station
    #date must be string & format is Y-M-D (e.g. 2016-08-24)
    #station USC00519281 WAIHEE 837.5, HI US
    #HARD-CODED SOLUTION
    def getTempForMostActive(self):
        query = f"""
                SELECT
                    m.id as Measurement_ID, 
                    m.station as Station_ID,
                    s.name as Station_Name,
                    m.date as Date,
                    m.tobs as Temperature
                FROM 
                    "measurement" m
                    JOIN "station" s on m.station = s.station
                WHERE
                    m.station = 'USC00519281'
                    AND date > '2016-08-23'
                ORDER BY
                    date DESC;
                """

        conn = self.engine.connect()
        df = pd.read_sql(query, conn)
        conn.close()

        return df


    #return previous year of temperature data for most active station
    #date must be string & format is Y-M-D (e.g. 2016-08-24)
    #station USC00519281 WAIHEE 837.5, HI US
    #RELATIVE DATE
    def getTempForMostActiveRelative(self):
        query = f"""
                SELECT
                    m.id as Measurement_ID,
                    m.station as Station_ID,
                    s.name as Station_Name,
                    m.date as Date,
                    m.tobs as Temperature
                FROM 
                    "measurement" m
                    JOIN "station" s on m.station = s.station
                WHERE
                    m.station = 'USC00519281'
                ORDER BY
                    date DESC;
                """

        conn = self.engine.connect()
        df = pd.read_sql(query, conn)
        conn.close()

        df = pd.DataFrame(df)
        # Convert Date Column to Timestamp
        df['Date'] = pd.to_datetime(df['Date'])
        #Set Date as Index
        df.set_index('Date', inplace=True)
        #jsonified dataframe does not return index
        #creating duplicate date column
        df['Date_1'] = df.index
        df['Date_1'] = df['Date_1'].astype(str)

        #Filter for relative 12 months
        start_date = df.index.max()
        end_date = df.index.max() - relativedelta(months=12)
        mask = (df.index > end_date) & (df.index <= start_date)
        last12_df = df.loc[mask]
        last12_df.head()

        return last12_df


    #return temperature by date (must be a string)
    #date format is Y-M-D (e.g. 2016-08-24)
    def getTempByDate(self, date):
        query = f"""
                SELECT 
                    date,
                    min(tobs) as min_temp,
                    max(tobs) as max_temp,
                    avg(tobs) as avg_temp
                FROM 
                    measurement
                WHERE
                    date = '{date}';
                """

        conn = self.engine.connect()
        df = pd.read_sql(query, conn)
        conn.close()

        return df

    #return temperature by date range (must be a string)
    #date must be a string
    #date format is Y-M-D (e.g. 2016-08-24)
    def getTempByDateRange(self, start, end):
        query = f"""
                SELECT 
                    min(tobs) as min_temp,
                    max(tobs) as max_temp,
                    avg(tobs) as avg_temp
                FROM 
                    measurement
                WHERE
                    date >= '{start}'
                    AND date < '{end}';
                """

        conn = self.engine.connect()
        df = pd.read_sql(query, conn)
        conn.close()

        return df