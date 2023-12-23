#!/usr/bin/env python
# coding: utf-8

# Import python modules
import argparse
import psycopg
import sys
import os
import pandas as pd
from loguru import logger

def getStationID(locationType):
    ''' Returns a DataFrame containing a list of station ids and station names, based on the location type (COASTAL, TIDAL or RIVERS), 
        from table drf_gauge_station.
        Parameters
            locationType: string
                gauge location type (COASTAL, TIDAL, or RIVERS) 
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
        cur.execute("""SELECT station_id, station_name FROM drf_gauge_station
                       WHERE location_type = %(location_type)s
                       ORDER BY station_name""", 
                    {'location_type': locationType})
       
        # convert query output to Pandas dataframe 
        df = pd.DataFrame(cur.fetchall(), columns=['station_id', 'station_name'])

        # Close cursor and database connection
        cur.close()
        conn.close()

        # Return Pandas dataframe
        return(df)

    # If exception log error
    except (Exception, psycopg.DatabaseError) as error:
        logger.info(error)

def addMeta(ingestPath, inputDataSource, inputSourceName, inputSourceArchive, inputSourceInstance, inputForcingMetaclass, inputUnits, inputLocationType):
    ''' Returns a CSV file that containes source information specific to station IDs that have been extracted from the drf_gauge_station table.
        The function adds additional source information (data source, source name, source archive, data units) to the station IDs. This 
        information is latter ingested into table drf_model_source by running the ingestModelSourceData() function in ingetTask.py
        Parameters
            ingestPath: string
                Directory path to ingest data files, created from the harvest files, modelRunID subdirectory is included in this path
            inputDataSource: string
                Unique identifier of data source (e.g., river_gauge, tidal_predictions, air_barameter, wind_anemometer, NAMFORECAST_NCSC_SAB_V1.23...)
            inputSourceName: string
                Organization that owns original source data (e.g., ncem, ndbc, noaa, adcirc...)
            inputSourceArchive: string
                Where the original data source is archived (e.g., contrails, ndbc, noaa, renci...)
            inputSourceInstance: string
                Source instance, such as ncsc123_gfs_sb55.01. Used by ingestSourceMeta, and ingestData.
            inputForcingMetaclass: string
                ADCIRC model forcing class, such as synoptic or tropical. Used by ingestSourceMeta, and ingestData.
            inputUnits: string
                Units of data (e.g., m (meters), m^3ps (meter cubed per second), mps (meters per second), and mb (millibars)
            inputLocationType: string
                gauge location type (COASTAL, TIDAL, or RIVERS)
        Returns
            CSV file
    '''

    df = getStationID(inputLocationType)

    df['data_source'] = inputDataSource
    df['source_name'] = inputSourceName
    df['source_archive'] = inputSourceArchive
    df['source_instance'] = inputSourceInstance
    df['forcing_metaclass'] = inputForcingMetaclass
    df['units'] = inputUnits

    # Drop station_name from DataFrame 
    df.drop(columns=['station_name'], inplace=True)

    # Reorder column name and update indeces 
    newColsOrder = ['station_id','data_source','source_name','source_archive','source_instance','forcing_metaclass','units']
    df=df.reindex(columns=newColsOrder)

    # Write dataframe to csv file 
    outputFile = 'source_'+inputSourceName+'_stationdata_'+inputSourceArchive+'_'+inputLocationType+'_'+inputDataSource+'_meta.csv'
    df.to_csv(ingestPath+outputFile, index=False, header=False)

# Main program function takes args as input, which contains the ingestPath, and outputFile values.
@logger.catch
def main(args):
    ''' Main program function takes args as input, starts logger, runs addMeta(), which writes output to CSV file.
        The CSV file will be ingest into table drf_model_source when ingestModelSourceData() function is run in ingetTask.py
        Parameters
            args: dictionary
                contains the parameters listed below
            ingestPath: string
                Directory path to ingest data files, created from the harvest files, modelRunID subdirectory is included in this path
            inputDataSource: string
                Unique identifier of data source (e.g., river_gauge, tidal_predictions, air_barameter, wind_anemometer, NAMFORECAST_NCSC_SAB_V1.23...)
            inputSourceName: string
                Organization that owns original source data (e.g., ncem, ndbc, noaa, adcirc...)
            inputSourceArchive: string
                Where the original data source is archived (e.g., contrails, ndbc, noaa, renci...)
            inputSourceInstance: string
                Source instance, such as ncsc123_gfs_sb55.01. Used by ingestSourceMeta, and ingestData.
            inputForcingMetaclass: string
                ADCIRC model forcing class, such as synoptic or tropical. Used by ingestSourceMeta, and ingestData.
            inputUnits: string
                Units of data (e.g., m (meters), m^3ps (meter cubed per second), mps (meters per second), and mb (millibars)
            inputLocationType: string
                gauge location type (COASTAL, TIDAL, or RIVERS)
        Returns
            CSV file
    '''

    # Add logger
    logger.remove()
    log_path = os.path.join(os.getenv('LOG_PATH', os.path.join(os.path.dirname(__file__), 'logs')), '')
    logger.add(log_path+'createIngestModelSourceMeta.log', level='DEBUG')
    logger.add(sys.stdout, level="DEBUG")
    logger.add(sys.stderr, level="ERROR")

    # Extract args variables
    ingestPath = os.path.join(args.ingestPath, '')
    inputDataSource = args.inputDataSource
    inputSourceName = args.inputSourceName
    inputSourceArchive = args.inputSourceArchive
    inputSourceInstance = args.inputSourceInstance
    inputForcingMetaclass = args.inputForcingMetaclass
    inputUnits = args.inputUnits
    inputLocationType = args.inputLocationType

    if not os.path.exists(ingestPath):
        os.mkdir(ingestPath)
        logger.info("Directory %s created!" % ingestPath)
    else:
        logger.info("Directory %s already exists" % ingestPath)

    logger.info('Start processing source data for data source '+inputDataSource+', with source name '+inputSourceName+', source archive '+inputSourceArchive+', and location type '+inputLocationType+'.')

    # Run addMeta function
    addMeta(ingestPath, inputDataSource, inputSourceName, inputSourceArchive, inputSourceInstance, inputForcingMetaclass, inputUnits, inputLocationType)
    logger.info('Finished processing source data for file data source '+inputDataSource+', with source name '+inputSourceName+', source archive '+inputSourceArchive+', and location type '+inputLocationType+'.')

# Run main function takes ingestPath, and outputFile as input.
if __name__ == "__main__":
    ''' Takes argparse inputs and passes theme to the main function
        Parameters
            ingestPath: string
                Directory path to ingest data files, created from the harvest files, modelRunID subdirectory is included in this path
            inputDataSource: string
                Unique identifier of data source (e.g., river_gauge, tidal_predictions, air_barameter, wind_anemometer, NAMFORECAST_NCSC_SAB_V1.23...)
            inputSourceName: string
                Organization that owns original source data (e.g., ncem, ndbc, noaa, adcirc...)
            inputSourceArchive: string
                Where the original data source is archived (e.g., contrails, ndbc, noaa, renci...)
            inputSourceInstance: string
                Source instance, such as ncsc123_gfs_sb55.01. Used by ingestSourceMeta, and ingestData.
            inputForcingMetaclass: string
                ADCIRC model forcing class, such as synoptic or tropical. Used by ingestSourceMeta, and ingestData.
            inputUnits: string
                Units of data (e.g., m (meters), m^3ps (meter cubed per second), mps (meters per second), and mb (millibars)
            inputLocationType: string
                Gauge location type (COASTAL, TIDAL, or RIVERS)
        Returns
            None
    '''         

    parser = argparse.ArgumentParser()

    # Optional argument which requires a parameter (eg. -d test)
    parser.add_argument("--ingestPath", "--ingestPath", help="Ingest directory path, including the modelRunID", action="store", dest="ingestPath", required=True)
    parser.add_argument("--inputDataSource", help="Input data source name", action="store", dest="inputDataSource", required=True)
    parser.add_argument("--inputSourceName", help="Input source name", action="store", dest="inputSourceName", choices=['adcirc','noaa','ndbc','ncem'], required=True)
    parser.add_argument("--inputSourceArchive", help="Input source archive name", action="store", dest="inputSourceArchive", required=True) 
    parser.add_argument("--inputSourceInstance", help="Input source variables", action="store", dest="inputSourceInstance", required=True)
    parser.add_argument("--inputForcingMetaclass", help="Input forcing metaclass", action="store", dest="inputForcingMetaclass", required=True)
    parser.add_argument("--inputUnits", help="Input units", action="store", dest="inputUnits", required=True)
    parser.add_argument("--inputLocationType", help="Input location type", action="store", dest="inputLocationType", required=True)

    # Parse input arguments
    args = parser.parse_args()

    # Run main
    main(args)

