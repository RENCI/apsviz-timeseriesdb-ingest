#!/usr/bin/env python
# coding: utf-8

# Import python modules
import argparse
import os
import sys
import psycopg
import pandas as pd
from datetime import datetime, timedelta
from shapely.geometry import Point
from shapely import wkb, wkt
from loguru import logger

# Flatten list to one layer
def flatten(l):
    return [item for sublist in l for item in sublist]

def getObsStations(beginDate, endDate, inputLocationType):
    ''' Returns DataFrame containing station names queried from the drf_retain_obs_station table,
        which overlaps with a begin date, and end date.
        Parameters  
            beginDate: data time
                The begin date to use in the query.
            endDate: data time
                The end date to use in the query.
        Returns 
            DataFrame
    '''                     
                
    try:        
        # Create connection to database and get cursor
        conn = psycopg.connect(dbname=os.environ['APSVIZ_GAUGES_DB_DATABASE'], 
                               user=os.environ['APSVIZ_GAUGES_DB_USERNAME'], 
                               host=os.environ['APSVIZ_GAUGES_DB_HOST'], 
                               port=os.environ['APSVIZ_GAUGES_DB_PORT'], 
                               password=os.environ['APSVIZ_GAUGES_DB_PASSWORD'])
        cur = conn.cursor()
                
        # Run query 
        cur.execute("""SELECT DISTINCT station_name,data_source,source_name,source_archive,gauge_owner,location_type
                       FROM drf_retain_obs_station 
                       WHERE location_type = %(locationtype)s AND (begin_date, end_date) 
                       OVERLAPS (%(begindate)s::DATE, %(enddate)s::DATE)
                       ORDER BY station_name""", 
                    {'locationtype': inputLocationType, 'begindate': beginDate, 'enddate': endDate})

        # convert query output to Pandas dataframe
        df = pd.DataFrame(cur.fetchall(), columns=['station_name','data_source','source_name','source_archive',
                                                   'gauge_owner','location_type'])       

        # Close cursor and database connection
        cur.close()
        conn.close()

        # return DataFrame
        return(df)

    # If exception log error
    except (Exception, psycopg.DatabaseError) as error:
        logger.info(error)

def getADCIRCStations(timeMark):
    ''' Returns DataFrame containing station names queried from the drf_apsviz_station  table.
        Parameters  
            timeMark: data time
                The timeMark or start time of the model to use in the query.
        Returns 
            DataFrame
    '''                     
                
    try:        
        # Create connection to database and get cursor
        conn = psycopg.connect(dbname=os.environ['APSVIZ_GAUGES_DB_DATABASE'], 
                               user=os.environ['APSVIZ_GAUGES_DB_USERNAME'], 
                               host=os.environ['APSVIZ_GAUGES_DB_HOST'], 
                               port=os.environ['APSVIZ_GAUGES_DB_PORT'], 
                               password=os.environ['APSVIZ_GAUGES_DB_PASSWORD'])
        cur = conn.cursor()
                
        # Run query 
        cur.execute("""SELECT DISTINCT station_name 
                       FROM drf_apsviz_station 
                       WHERE timemark =  %(timemark)s
                       ORDER BY station_name""", 
                    {'timemark': timeMark})

        # convert query output to Pandas dataframe
        df = pd.DataFrame(cur.fetchall(), columns=['station_name'])       

        # Close cursor and database connection
        cur.close()
        conn.close()

        # return DataFrame
        return(df)

    # If exception log error
    except (Exception, psycopg.DatabaseError) as error:
        logger.info(error)

def getGaugeStationInfo(stationNames):
    ''' Returns DataFrame containing variables from the drf_gauge_station table. It takes a list of station 
        names as input.
        Parameters
            stationNames: list
                List of station names
        Returns
            DataFrame
    '''

    try:
        # Create connection to database and get cursor
        conn = psycopg.connect(dbname=os.environ['APSVIZ_GAUGES_DB_DATABASE'], 
                               user=os.environ['APSVIZ_GAUGES_DB_USERNAME'], 
                               host=os.environ['APSVIZ_GAUGES_DB_HOST'], 
                               port=os.environ['APSVIZ_GAUGES_DB_PORT'], 
                               password=os.environ['APSVIZ_GAUGES_DB_PASSWORD'])
        cur = conn.cursor()
        
        # Run query
        cur.execute("""SELECT station_name, lat, lon, tz, gauge_owner, location_name, country, state, county, geom
                       FROM drf_gauge_station
                       WHERE station_name = ANY(%(station_names)s)""", {'station_names': stationNames})

        # convert query output to Pandas dataframe
        df = pd.DataFrame(cur.fetchall(), columns=['station_name', 'lat', 'lon', 'tz', 'gauge_owner', 
                                                   'location_name', 'country', 'state', 'county', 'geom'])

        # Close cursor and database connection
        cur.close()
        conn.close()

        # return DataFrame
        return(df)

    # If exception log error
    except (Exception, psycopg.DatabaseError) as error:
        logger.info(error)

def addApsVizStationFileMeta(harvestDir, ingestDir, inputFilename, timeMark, modelRunID, inputDataSource, inputSourceName,
                             inputSourceArchive, inputLocationType, gridName, csvURL):
    ''' Returns a csv file that containes station location data for the drf_apsviz_station table. The function adds a 
        timemark, that it gets from the input file name. The timemark values can be used to uniquely query an ADCIRC 
        forecast model run. It also adds a model_run_id, data_source, source_archive, grid_name, and csv_url. The 
        model_run_id specifies what model run the data is from, and the csvurl is the URL used to access that data 
        from the apsViz interface.
        Parameters
            harvestDir: string
                Directory path to harvest data files
            ingestDir: string
                Directory path to ingest data files, created from the harvest files
            inputFilename: string
                The name of the input file
            timeMark: datatime 
                Date and time of the beginning of the model run for forecast runs, and end of the model run for 
                nowcast runs.
            modelRunID: string
                Unique identifier of a model run. It combines the instance_id, and uid from asgs_dashboard db
            inputDataSource: string
                Unique identifier of data source (e.g., river_gauge, tidal_predictions, air_barameter, 
                wind_anemometer,
                NAMFORECAST_NCSC_SAB_V1.23...). Used by ingestSourceMeta, and ingestData.
            inputSourceName: string
                Organization that owns original source data (e.g., ncem, ndbc, noaa, adcirc...). Used by ingestSourceMeta, 
                and ingestData.
            inputSourceArchive: string
                Where the original data source is archived (e.g., contrails, ndbc, noaa, renci...)
            inputLocationType: string
                Gauge location type (COASTAL, TIDAL, or RIVERS). Used by ingestSourceMeta.
            gridName: string
                Name of grid being used in model run (e.g., ed95d, hsofs NCSC_SAB_v1.23...)
            csvURL:
                URL to SQL function that queries db and returns CSV file of data
        Returns
            CSV file
    '''

    # Create station meta filename from the input file name.
    apsviz_station_meta_filename = 'adcirc_'+"_".join(inputFilename.split('_')[1:])

    # Read input file, convert column name to lower case, rename station column to station_name, convert its data
    # type to string, and add timemark, model_run_id, and csvurl columns
    dfADCRICMeta = pd.read_csv(harvestDir+apsviz_station_meta_filename)
    dfADCRICMeta.columns= dfADCRICMeta.columns.str.lower()
    dfADCRICMeta = dfADCRICMeta.rename(columns={'station': 'station_name'})
    dfADCRICMeta = dfADCRICMeta.astype({"station_name": str})
    dfADCIRCStations = dfADCRICMeta["station_name"].to_frame()

    # Get station meta from drf_gauge_station for all of the stations that have ADCRIC data
    dfADCIRCOut = getGaugeStationInfo(dfADCIRCStations["station_name"].values.tolist())

    # Add new columns
    dfADCIRCOut.insert(0,'timemark', '')
    dfADCIRCOut.insert(0,'model_run_id', '')
    dfADCIRCOut.insert(0,'data_source', '')
    dfADCIRCOut.insert(0,'source_name','')
    dfADCIRCOut.insert(0,'source_archive', '')
    dfADCIRCOut.insert(0,'location_type','')
    dfADCIRCOut.insert(0,'grid_name', '')
    dfADCIRCOut.insert(0,'csvurl', '')

    # Reorder columns
    dfADCIRCOut = dfADCIRCOut.loc[:, ["station_name","lat","lon","tz","gauge_owner","location_name","country",
                                      "state","county","geom","timemark","model_run_id","data_source","source_name",
                                      "source_archive","location_type","grid_name","csvurl"]]
 
     # Add model_run_id, timemark, and csvurl values to specifies columns in DataFrame
    timemark = "T".join(timeMark.split(' ')).split('+')[0]+'Z'
    dfADCIRCOut['timemark'] = timemark
    dfADCIRCOut['model_run_id'] = modelRunID
    dfADCIRCOut['data_source'] = inputDataSource
    dfADCIRCOut['source_name'] = inputSourceName
    dfADCIRCOut['source_archive'] = inputSourceArchive
    dfADCIRCOut['location_type'] = inputLocationType
    dfADCIRCOut['grid_name'] = gridName

    # Derive begin_date and end_date from timeMark for use in getting the obs station data
    time_mark = pd.to_datetime(timeMark)
    begin_date = time_mark - timedelta(days=1.5)
    end_date = time_mark

    # Get Obs stations that overlap with begin date and end date derived from timeMark
    dfObs = getObsStations(begin_date, end_date, inputLocationType)

    # Check if dataframe is not empty
    if not dfObs.empty:
        # Remove rows containing tidal_predictions, wind_anemometer, and air_barameter. 
        dfObs.drop(dfObs.loc[dfObs['data_source']=='tidal_predictions'].index, inplace=True)
        dfObs.drop(dfObs.loc[dfObs['data_source']=='wind_anemometer'].index, inplace=True)
        dfObs.drop(dfObs.loc[dfObs['data_source']=='air_barometer'].index, inplace=True)

        # Remove any duplicate stations if there are any
        dfObs.drop_duplicates(subset=['station_name'], inplace=True)

        # Extract Obs stations with ADCIRC stations removing duplicates
        dfObsStations = dfObs["station_name"].to_frame()
        dfObsStationSubset = dfObsStations[~dfObsStations.apply(tuple,1).isin(dfADCIRCStations.apply(tuple,1))]
        
        # Subset dfObs by only including stations from dfObsStationSubset
        dfObs = dfObs.loc[dfObs['station_name'].isin(flatten(dfObsStationSubset.values.tolist()))]
        
        # Remove gauge_owner colume from dfObs
        dfObs = dfObs.drop('gauge_owner', axis=1)
        
        # Merger dfObs with DateFrame obtained from drf_guauge_station, which has extra meta-data
        dfObsOut = pd.merge(dfObs, getGaugeStationInfo(dfObsStationSubset["station_name"].values.tolist()), 
                            on="station_name")
        
        # Add new columns
        dfObsOut.insert(0,'timemark', '')
        dfObsOut.insert(0,'model_run_id', '')
        dfObsOut.insert(0,'grid_name', '')
        dfObsOut.insert(0,'csvurl', '')
        
        # Add model_run_id, timemark, and csvurl values to specifies columns in DataFrame
        dfObsOut['timemark'] = timemark
        dfObsOut['model_run_id'] = modelRunID
        dfObsOut['grid_name'] = gridName

        # Reorder columns
        dfObsOut = dfObsOut.loc[:, ["station_name","lat","lon","tz","gauge_owner","location_name","country",
                                    "state","county","geom","timemark","model_run_id","data_source",
                                    "source_name","source_archive","location_type","grid_name","csvurl"]]
        
        # Concatinate dfADCIRCOut with dfObsOut
        dfOut = pd.concat([dfADCIRCOut, dfObsOut], ignore_index=True, sort=False)

    else:
        logger.info('There are no Obs stations with the date range of '+str(begin_date)+' to '+str(end_date))
        dfOut = dfADCIRCOut

    # Create csvURL and add it to DataFrame
    for index, row in dfOut.iterrows():
        csvURL = os.environ['UI_DATA_URL']+'/get_station_data?station_name='+row['station_name']+'&time_mark='+timemark+'&data_source='+inputDataSource
        dfOut.at[index,'csvurl'] = csvURL

    # Write DataFrame to CSV file
    dfOut.to_csv(ingestDir+'meta_copy_'+apsviz_station_meta_filename, index=False, header=False)

# Main program function takes args as input, which contains the  ingestDir, inputDataSource, inputSourceName, and inputSourceArchive values.
@logger.catch
def main(args):
    ''' Main program function takes args as input, starts logger, runs createFileList, and writes output to CSV file.
        The CSV file will be ingest into table drf_apsviz_station_file_meta during runHarvestFile() is run in runIngest.py
        Parameters
            args: dictionary 
                contains the parameters listed below
            harvestDir: string
                Directory path to harvest data files
            ingestDir: string
                Directory path to ingest data files, created from the harvest files
            inputFilename: string
                The name of the input file
            timeMark: datatime
                Date and time of the beginning of the model run for forecast runs, and end of the model run for nowcast runs.
            modelRunID: string
                Unique identifier of a model run. It combines the instance_id, and uid from asgs_dashboard db
            inputDataSource: string
                Unique identifier of data source (e.g., river_gauge, tidal_predictions, air_barameter, wind_anemometer,
                NAMFORECAST_NCSC_SAB_V1.23...). Used by ingestSourceMeta, and ingestData.
            inputSourceName: string
                Organization that owns original source data (e.g., ncem, ndbc, noaa, adcirc...). Used by ingestSourceMeta, 
                and ingestData.
            inputSourceArchive: string
                Where the original data source is archived (e.g., contrails, ndbc, noaa, renci...)
            inputLocationType: string
                Gauge location type (COASTAL, TIDAL, or RIVERS). Used by ingestSourceMeta.
            gridName: string 
                Name of grid being used in model run (e.g., ed95d, hsofs NCSC_SAB_v1.23...)
            csvURL:
                URL to SQL function that queries db and returns CSV file of data
        Returns
            CSV file
    '''

    # Add logger
    logger.remove()
    log_path = os.path.join(os.getenv('LOG_PATH', os.path.join(os.path.dirname(__file__), 'logs')), '')
    logger.add(log_path+'createIngestApsVizStationData.log', level='DEBUG')
    logger.add(sys.stdout, level="DEBUG")
    logger.add(sys.stderr, level="ERROR")

    # Extract args variables
    harvestDir = os.path.join(args.harvestDir, '')
    ingestDir = os.path.join(args.ingestDir, '')
    inputFilename = args.inputFilename 
    timeMark = args.timeMark
    modelRunID = args.modelRunID
    inputDataSource = args.inputDataSource
    inputSourceName = args.inputSourceName
    inputSourceArchive = args.inputSourceArchive
    inputLocationType = args.inputLocationType
    gridName = args.gridName
    csvURL = args.csvURL
        
    logger.info('Start processing data from '+harvestDir+inputFilename+', with output directory '+ingestDir+', model run ID '+
                modelRunID+', timemark '+timeMark+', and csvURL '+csvURL+'.')
    addApsVizStationFileMeta(harvestDir, ingestDir, inputFilename, timeMark, modelRunID, inputDataSource, inputSourceName, inputSourceArchive, inputLocationType, gridName, csvURL)
    logger.info('Finished processing data from '+harvestDir+inputFilename+', with output directory '+ingestDir+', model run ID '+
                modelRunID+', timemark '+timeMark+', and csvURL '+csvURL+'.')
 
# Run main function takes harvestDir, ingestDir, inputFilename, and timeMark as input.
if __name__ == "__main__": 
    ''' Takes argparse inputs and passes theme to the main function
        Parameters
            harvestDir: string
                Directory path to harvest data files
            ingestDir: string
                Directory path to ingest data files, created from the harvest files
            inputFilename: string
                The name of the input file
            timeMark: datatime
                Date and time of the beginning of the model run for forecast runs, and end of the model run for nowcast runs.
            modelRunID: string
                Unique identifier of a model run. It combines the instance_id, and uid from asgs_dashboard db
            inputDataSource: string
                Unique identifier of data source (e.g., river_gauge, tidal_predictions, air_barameter, wind_anemometer,
                NAMFORECAST_NCSC_SAB_V1.23...). Used by ingestSourceMeta, and ingestData.
            inputSourceName: string
                Organization that owns original source data (e.g., ncem, ndbc, noaa, adcirc...). Used by ingestSourceMeta, 
                and ingestData.
            inputSourceArchive: string
                Where the original data source is archived (e.g., contrails, ndbc, noaa, renci...)
            inputLocationType: string
                Gauge location type (COASTAL, TIDAL, or RIVERS). Used by ingestSourceMeta.
            gridName: string 
                Name of grid being used in model run (e.g., ed95d, hsofs NCSC_SAB_v1.23...) 
            csvURL:
                URL to SQL function that queries db and returns CSV file of data
        Returns
            None
    '''

    parser = argparse.ArgumentParser()

    # Optional argument which requires a parameter (eg. -d test)
    parser.add_argument("--harvestDIR", "--harvestDir", help="Input directory path", action="store", dest="harvestDir", required=True)
    parser.add_argument("--ingestDIR", "--ingestDir", help="Output directory path", action="store", dest="ingestDir", required=True)
    parser.add_argument("--inputFilename", help="Input file name containing meta data on apsViz stations", action="store", dest="inputFilename", required=True)
    parser.add_argument("--timeMark", help="Time model run started", action="store", dest="timeMark", required=True)
    parser.add_argument("--modelRunID", help="Model run ID for model run", action="store", dest="modelRunID", required=True)
    parser.add_argument("--inputDataSource", help="Input data source to be processed", action="store", dest="inputDataSource", required=True)
    parser.add_argument("--inputSourceName", help="Input source name to be processed", action="store", dest="inputSourceName", choices=['adcirc','ncem','noaa','ndbc'], required=True)
    parser.add_argument("--inputSourceArchive", help="Input source archive name", action="store", dest="inputSourceArchive", required=True)
    parser.add_argument("--inputLocationType", help="Input location type to be processed", action="store", dest="inputLocationType", required=True)
    parser.add_argument("--gridName", help="Name of grid being used in model run", action="store", dest="gridName", required=True)
    parser.add_argument("--csvURL", help="URL to SQL function that will retrieve a csv file", action="store", dest="csvURL", required=True)

    args = parser.parse_args() 
    main(args)

