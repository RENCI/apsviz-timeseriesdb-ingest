import os
import psycopg
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

def getForecastStationData(station_name, timemark, data_source):
    try:
        # Create connection to database, set autocommit, and get cursor
        with psycopg.connect(dbname=os.environ['APSVIZ_GAUGES_DB_DATABASE'], user=os.environ['APSVIZ_GAUGES_DB_USERNAME'], 
                             host=os.environ['APSVIZ_GAUGES_DB_HOST'], port=os.environ['APSVIZ_GAUGES_DB_PORT'], 
                             password=os.environ['APSVIZ_GAUGES_DB_PASSWORD']) as conn:
            cur = conn.cursor()

            # Run query
            cur.execute("""SELECT * FROM get_forecast_timeseries_station_data(_station_name := %(stationname)s,
                        _timemark := %(timemark)s, _data_source := %(datasource)s)""", 
                        {'stationname':station_name, 'timemark': timemark, 'datasource': data_source})

            # convert query output to Pandas dataframe
            df = pd.DataFrame.from_dict(cur.fetchall()[0][0], orient='columns')
            #data = cur.fetchall()
            
            # Close cursor and database connection
            cur.close()
            conn.close()

            # Return Pandas dataframe
            return(df)
            #return(data)

    # If exception log error
    except (Exception, psycopg.DatabaseError) as error:
        print(error)

def getObsStationData(station_name, start_date, end_date, nowcast_source):
    try:
        # Create connection to database, set autocommit, and get cursor
        with psycopg.connect(dbname=os.environ['APSVIZ_GAUGES_DB_DATABASE'], user=os.environ['APSVIZ_GAUGES_DB_USERNAME'], 
                             host=os.environ['APSVIZ_GAUGES_DB_HOST'], port=os.environ['APSVIZ_GAUGES_DB_PORT'], 
                             password=os.environ['APSVIZ_GAUGES_DB_PASSWORD']) as conn:
            cur = conn.cursor()

            # Run query
            cur.execute("""SELECT * FROM get_obs_timeseries_station_data(_station_name := %(stationname)s, 
                           _start_date := %(startdate)s, _end_date := %(enddate)s,
                           _nowcast_source := %(nowcastsource)s)""", 
                        {'stationname':station_name, 'startdate': start_date, 'enddate': end_date,
                          'nowcastsource': nowcast_source})

            # convert query output to Pandas dataframe
            df = pd.DataFrame.from_dict(cur.fetchall()[0][0], orient='columns')
            
            # Close cursor and database connection
            cur.close()
            conn.close()

            # Return Pandas dataframe
            return(df)

    # If exception log error
    except (Exception, psycopg.DatabaseError) as error:
        print(error)

def processStationData(station_name, timemark, data_source):
    # THESE STEPS COULD STILL BE DONE WITH PYTHON
    # derive start_date from timemark
    fdatetime = datetime.fromisoformat(" ".join(timemark.split('T'))[0:-1])
    DAYS = timedelta(4)
    start_date = "T".join(str(fdatetime - DAYS).split(' '))+'Z'

    # get nowcast data_source from forecast data_source
    nowcast_source = 'NOWCAST_'+"_".join(data_source.split('_')[1:])

    # THESE STEPS NEED TO BE DONE WITH PLPGSQL
    # get forecast data
    dffc = getForecastStationData(station_name, timemark, data_source)
    
    # get end_date from last datetime in forecast data
    end_date = dffc['time_stamp'].iloc[-1]
    
    # get obs and nowcast data
    dfobs = getObsStationData(station_name, start_date, end_date, nowcast_source)
    
    # drop empty colomns
    empty_cols = [col for col in dfobs.columns if dfobs[col].isnull().all()]
    dfobs.drop(empty_cols, axis=1,inplace=True)
    
    # replace any None values with np.nan, in both DataFrames
    dffc.fillna(value=np.nan)
    dfobs.fillna(value=np.nan)
    
    # convert all values after timemark to nan, in obs data, except in the time_stamp, 
    # and tidal_predictions columns
    for col in dfobs.columns:
        if (col != 'time_stamp') and (col != 'tidal_predictions'):
            dfobs.loc[dfobs.time_stamp >= timemark, col] = np.nan
        else:
            continue

    # merge the obs DataFrame with the forecast Dataframe
    df = dfobs.merge(dffc, on='time_stamp', how='outer')
    
    # change forecast, and nowcast column names
    forecast_column_name = "".join(data_source.split('.')).lower()
    nowcast_column_name = "".join(nowcast_source.split('.')).lower()
    df.rename(columns={forecast_column_name: 'forecast_water_level', nowcast_column_name: 'nowcast_water_level'}, 
              inplace=True)
    
    # return DataFrame
    #return(df)
    return(df.to_csv(index=False))

station_name = '8651370'
timemark = '2023-04-23T12:00:00Z'
data_source = 'NAMFORECAST_NCSC_SAB_V1.23'
#df = processStationData(station_name, timemark, data_source)
#df.head()
output = processStationData(station_name, timemark, data_source)
print(output)
