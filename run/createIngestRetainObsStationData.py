#!/usr/bin/env python
# coding: utf-8

# Import python modules
import argparse
import os
import sys
import psycopg
import pandas as pd
from shapely.geometry import Point
from shapely import wkb, wkt
from loguru import logger

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

# look into using **kwargs here, and eventually other places where it make sense.
def addObsStationFileMeta(harvestDir, ingestDir, inputFilename, timeMark, beginDate, endDate, inputDataSource, inputSourceName, inputSourceArchive, inputLocationType):
    ''' Returns a csv file that containes station location data for the drf_retain_obs_station table. The function adds
        a timemark, that it gets from the input file name. The timemark values can be used to uniquely query an ADCIRC 
        forecast model run. It also adds a data_source, and source_archive. 
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
            beginDate: datatime
                Date and time of the first record in data file.
            endDate: datatime
                Date and time of the last record in data file.
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
        Returns
            CSV file
    '''

    # Read input file, convert column name to lower case, rename station column to station_name, convert its data
    # type to string, and add timemark, model_run_id, and csvurl columns
    dfObsStations = pd.read_csv(harvestDir+inputFilename)
    dfObsStations.columns= dfObsStations.columns.str.lower()
    dfObsStations = dfObsStations.rename(columns={'station': 'station_name'})
    dfObsStations = dfObsStations.astype({"station_name": str})
    
    # Get stations from drf_gauge_station for station names in dfApsVizStations
    df = getGaugeStationInfo(dfObsStations["station_name"].values.tolist())

    # Add new columns
    df.insert(0,'timemark', '')
    df.insert(0,'begin_date', '')
    df.insert(0,'end_date', '')
    df.insert(0,'data_source', '')
    df.insert(0,'source_name', '')
    df.insert(0,'source_archive', '')
    df.insert(0,'location_type', '')

    # Reorder columns
    df = df.loc[:, ["station_name","lat","lon","location_name","tz","gauge_owner","country","state","county","geom","timemark",
                    "begin_date","end_date","data_source","source_name","source_archive","location_type"]]
 
    # Add model_run_id, timemark, and csvurl values to specifies columns in DataFrame
    timemark = "T".join(timeMark.split(' ')).split('+')[0]+'Z'
    df['timemark'] = timemark
    df['begin_date'] = beginDate
    df['end_date'] = endDate
    df['data_source'] = inputDataSource
    df['source_name'] = inputSourceName
    df['source_archive'] = inputSourceArchive
    df['location_type'] = inputLocationType

    # Write DataFrame to CSV file
    df.to_csv(ingestDir+'obs_station_data_copy_'+inputFilename, index=False, header=False)

# Main program function takes args as input, which contains the  ingestDir, inputDataSource, inputSourceName, and inputSourceArchive values.
@logger.catch
def main(args):
    ''' Main program function takes args as input, starts logger, runs createFileList, and writes output to CSV file.
        The CSV file will be ingest into table drf_apsviz_station_file_meta during runHarvestFile() is run in runObsIngest.py
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
            beginDate: datatime
                Date and time of the first record in data file.
            endDate: datatime
                Date and time of the last record in data file.
            inputDataSource: string
                Unique identifier of data source (e.g., river_gauge, tidal_predictions, air_barameter, wind_anemometer,
                NAMFORECAST_NCSC_SAB_V1.23...). Used by ingestSourceMeta, and ingestData.
            inputSourceName: string
                Organization that owns original source data (e.g., ncem, ndbc, noaa, adcirc...). Used by ingestSourceMeta,
                and ingestData.
            inputSourceArchive: string
                Where the original data source is archived (e.g., contrails, ndbc, noaa, renci...)
                URL to SQL function that queries db and returns CSV file of data
            inputLocationType: string
                Gauge location type (COASTAL, TIDAL, or RIVERS). Used by ingestSourceMeta.
        Returns
            CSV file
    '''

    # Add logger
    logger.remove()
    log_path = os.path.join(os.getenv('LOG_PATH', os.path.join(os.path.dirname(__file__), 'logs')), '')
    logger.add(log_path+'createIngestRetainObsStationData.log', level='DEBUG')
    logger.add(sys.stdout, level="DEBUG")
    logger.add(sys.stderr, level="ERROR")

    # Extract args variables
    harvestDir = os.path.join(args.harvestDir, '')
    ingestDir = os.path.join(args.ingestDir, '')
    inputFilename = args.inputFilename 
    timeMark = args.timeMark
    beginDate = args.beginDate
    endDate = args.endDate
    inputDataSource = args.inputDataSource
    inputSourceName = args.inputSourceName
    inputSourceArchive = args.inputSourceArchive
    inputLocationType = args.inputLocationType
        
    logger.info('Start processing data from '+harvestDir+inputFilename+', with output directory '+ingestDir+', timemark '+timeMark+', and location type '+inputLocationType+'.')
    addObsStationFileMeta(harvestDir, ingestDir, inputFilename, timeMark, beginDate, endDate, inputDataSource, inputSourceName, inputSourceArchive, inputLocationType)
    logger.info('Finished processing data from '+harvestDir+inputFilename+', with output directory '+ingestDir+', timemark '+timeMark+', and location type '+inputLocationType+'.')
 
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
            beginDate: datatime
                Date and time of the first record in data file.
            endDate: datatime
                Date and time of the last record in data file.
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
        Returns
            None
    '''

    parser = argparse.ArgumentParser()

    # Optional argument which requires a parameter (eg. -d test)
    parser.add_argument("--harvestDIR", "--harvestDir", help="Input directory path", action="store", dest="harvestDir", required=True)
    parser.add_argument("--ingestDIR", "--ingestDir", help="Output directory path", action="store", dest="ingestDir", required=True)
    parser.add_argument("--inputFilename", help="Input file name containing meta data on apsViz stations", action="store", dest="inputFilename", required=True)
    parser.add_argument("--timeMark", help="Time model run started", action="store", dest="timeMark", required=True)
    parser.add_argument("--beginDate", help="Time of first record in data file", action="store", dest="beginDate", required=True)
    parser.add_argument("--endDate", help="Time of last record in data file", action="store", dest="endDate", required=True)
    parser.add_argument("--inputDataSource", help="Input data source to be processed", action="store", dest="inputDataSource", required=True)
    parser.add_argument("--inputSourceName", help="Input source name to be processed", action="store", dest="inputSourceName", required=True)
    parser.add_argument("--inputSourceArchive", help="Input source archive name", action="store", dest="inputSourceArchive", required=True) 
    parser.add_argument("--inputLocationType", help="Input location type to be processed", action="store", dest="inputLocationType", required=True)

    args = parser.parse_args() 
    main(args)

