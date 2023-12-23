#!/usr/bin/env python
# coding: utf-8

# Import python modules
import argparse
import os
import sys
import glob
import re
import psycopg
import pandas as pd
import numpy as np
from loguru import logger

def getFileMetaTimemark(inputFile, inputSourceInstance, inputForcingMetaclass):
    ''' Returns DataFrame containing a timemark value, from the table drf_havest_model_file_meta.
        Parameters
            inputFile: string
                Input file name for the file to get the timemark for.
            inputSourceInstance: string
                Source instance, such as ncsc123_gfs_sb55.01.
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
        cur.execute("""SELECT file_name, source_instance, forcing_metaclass, timemark
                       FROM drf_harvest_model_file_meta
                       WHERE file_name = %(inputfile)s AND source_instance = %s(sourceinstance)s
                       AND forcing_metaclass = %(forcingmetaclass)s
                       ORDER BY timemark""",
                    {'inputfile': inputFile, 'sourceinstance': inputSourceInstance, 'forcingmetaclass': inputForcingMetaclass})

        # convert query output to Pandas DataFrame
        df = pd.DataFrame(cur.fetchall(), columns=['file_name', 'source_instance', 'forcing_metaclass', 'timemark'])

        # Close cursor and database connection
        cur.close()
        conn.close()

        return(df)

    # If exception log error
    except (Exception, psycopg.DatabaseError) as error:
        logger.info(error)


def getInputFiles(inputDataSource, inputSourceName, inputSourceArchive, inputSourceInstance, inputForcingMetaclass):
    ''' Returns DataFrame containing a list of filenames, from the table drf_havest_model_file_meta, that have not been ingested yet.
        Parameters
            inputDataSource: string
                Unique identifier of data source (e.g., river_gauge, tidal_predictions, air_barameter, wind_anemometer, NAMFORECAST_NCSC_SAB_V1.23...)
            inputSourceName: string
                Organization that owns original source data (e.g., ncem, ndbc, noaa, adcirc...)
            inputSourceArchive: string
                Where the original data source is archived (e.g., contrails, ndbc, noaa, renci...)
            inputSourceInstance: string
                Source instance, such as ncsc123_gfs_sb55.01.
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
        cur.execute("""SELECT dir_path, file_name, source_instance
                       FROM drf_harvest_model_file_meta 
                       WHERE data_source = %(datasource)s AND source_name = %(sourcename)s AND
                       source_archive = %(sourcearchive)s AND source_instance = %(sourceinstance)s 
                       forcing_metaclass = %(forcingmetaclass)s AND ingested = False
                       ORDER BY data_date_time""",
                    {'datasource': inputDataSource, 'sourcename': inputSourceName, 
                     'sourcearchive': inputSourceArchive, 'sourceinstance': inputSourceInstance,
                     'forcingmetaclass': inputForcingMetaclass})

        # convert query output to Pandas DataFrame
        df = pd.DataFrame(cur.fetchall(), columns=['dir_path','file_name','source_instance'])

        # Close cursor and database connection
        cur.close()
        conn.close()

        return(df)

    # If exception log error
    except (Exception, psycopg.DatabaseError) as error:
        logger.info(error)

def getSourceID(inputDataSource, inputSourceName, inputSourceArchive, inputSourceInstance, inputForcingMetaclass, station_list):
    ''' Returns DataFrame containing source_id(s) for model data from the drf_model_source table in the apsviz_gauges database.
        Parameters
            inputDataSource: string
                Unique identifier of data source (e.g., river_gauge, tidal_predictions, air_barameter, wind_anemometer, NAMFORECAST_NCSC_SAB_V1.23...)
            inputSourceName: string
                Organization that owns original source data (e.g., ncem, ndbc, noaa, adcirc...)
            inputSourceArchive: string
                Where the original data source is archived (e.g., contrails, ndbc, noaa, renci...)
            inputSourceInstance: string
                Source instance, such as ncsc123_gfs_sb55.01.
            station_list: list
                List of stations to get source ids for
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
        cur.execute("""SELECT s.source_id AS source_id, g.station_id AS station_id, g.station_name AS station_name, s.data_source AS data_source, 
                              s.source_name AS source_name, s.source_archive AS source_archive, s.source_instance AS source_instance
                       FROM drf_gauge_station g 
                       INNER JOIN drf_model_source s ON s.station_id=g.station_id
                       WHERE data_source = %(datasource)s AND source_name = %(sourcename)s AND
                         source_archive = %(sourcearchive)s AND source_instance = %(sourceinstance)s AND 
                         AND forcing_metaclass = %(forcingmetaclass)s AND station_name = ANY(%(stationlist)s) 
                       ORDER BY station_name""",
                    {'datasource': inputDataSource, 'sourcename': inputSourceName, 'sourcearchive': inputSourceArchive, 
                     'sourceinstance': inputSourceInstance, 'forcingmetaclass': inputForcingMetaclass, 'stationlist': station_list})

        # convert query output to Pandas dataframe
        dfstations = pd.DataFrame(cur.fetchall(), columns=['source_id','station_id','station_name','data_source','source_name','source_archive','source_instance'])
   
        # Close cursor and database connection 
        cur.close()
        conn.close()

        # Return Pandas dataframe 
        return(dfstations)

    # If exception log error
    except (Exception, psycopg.DatabaseError) as error:
        logger.info(error)

# ADCIRC forecast model run.
def addMeta(harvestDir, ingestPath, inputFile, inputDataSource, inputSourceName, inputSourceArchive, inputSourceInstance, inputForcingMetaclass):
    ''' Returns CSV file that containes gauge data. The function uses the getSourceID function above to get a list of existing source
        ids that it includes in the gauge data to enable joining the gauge data (drf_model_data) table with  gauge source (drf_model_source)
        table. The function adds a timemark, that it gets from the input file name. The timemark values can be used to uniquely query an
        ADCIRC forecast model run.
        Parameters
            harvestDir: string
                Directory path to harvest data files
            ingestPath: string
                Directory path to ingest data files, created from the harvest files, modelRunID subdirectory is included in this
                path.
            inputFile: string
                Input file name
            inputDataSource: string
                Unique identifier of data source (e.g., river_gauge, tidal_predictions, air_barameter, wind_anemometer, NAMFORECAST_NCSC_SAB_V1.23...)
            inputSourceName: string
                Organization that owns original source data (e.g., ncem, ndbc, noaa, adcirc...)
            inputSourceArchive: string
                Where the original data source is archived (e.g., contrails, ndbc, noaa, renci...)
            inputSourceInstance: string
                Source instance, such as ncsc123_gfs_sb55.01.
            inputForcingMetaclass: string
                ADCIRC model forcing class, such as synoptic or tropical.
        Returns
            CSV file 
    '''

    # Read input file, convert column name to lower case, rename station column to station_name, convert its data 
    # type to string, and add timemark and source_id columns
    df = pd.read_csv(harvestDir+inputFile)
    df.columns= df.columns.str.lower()
    df = df.rename(columns={'station': 'station_name'})
    df = df.astype({"station_name": str})
    df.insert(0,'timemark', '')
    df.insert(0,'source_id', '')
   
    # Extract list of stations from dataframe for querying the database, and get source_archive name from filename.
    station_list = [sorted([str(x) for x in df['station_name'].unique().tolist()])]

    # Run getSourceID function to get the source_id(s)
    dfstations = getSourceID(inputDataSource, inputSourceName, inputSourceArchive, inputSourceInstance, inputForcingMetaclass, station_list)

    # Get the timemark from the the data filename NEED TO DEAL WITH HURRICANE AND ENSEMBLES
    datetimes = re.findall(r'(\d+-\d+-\d+T\d+:\d+:\d+)',inputFile)
    if inputSourceName == 'adcirc':
        # Get timemark from the drf_harvest_model_file_meta table
        dftimemark = getFileMetaTimemark(inputFile)
        if re.search('forecast', inputDataSource.lower()):
            # If the inputDataSource has forecast in its name get the first datetime in the filename
            df['timemark'] = dftimemark['timemark'].values[0]
        elif re.search('nowcast', inputDataSource.lower()):
            # If the inputDataSource has nowcast in its name get the third datetime in the filename
            # if it is a synoptic run, and the second datetime in the filename if it is a hurricane run
            if inputForcingMetaclass == 'tropical':
                # Hurricane run
                df['timemark'] = dftimemark['timemark'].values[0]
            else:
                # Synoptic run
                df['timemark'] = dftimemark['timemark'].values[0]
        else:
            # If the inputDataSource is a hurricane for ensemble  get the second datetime in the filename
            df['timemark'] = dftimemark['timemark'].values[0]
    else:
        # If the inputDataSource does not have forecast or nowcast in its name then this should never happen!
        logger.info('The filename '+inputFile+' with instance name '+inputSourceInstance+' is not an ADCIRC file!')

    # Add source id(s) to dataframe 
    for index, row in dfstations.iterrows():
        df.loc[df['station_name'] == row['station_name'], 'source_id'] = row['source_id']

    # Drop station_name column from dataframe
    df.drop(columns=['station_name'], inplace=True)

    # Write dataframe to csv file
    df.to_csv(ingestPath+'data_copy_'+inputFile, index=False, header=False)

def processData(ingestPath, inputDataSource, inputSourceName, inputSourceArchive, inputSourceInstance, inputForcingMetaclass):
    ''' Runs getInputFiles, and then addMeta 
        Parameters
            ingestPath: string
                Directory path to ingest data files, created from the harvest files, modelRunID subdirectory is included in this
                path.
            inputDataSource: string
                Unique identifier of data source (e.g., river_gauge, tidal_predictions, air_barameter, wind_anemometer, NAMFORECAST_NCSC_SAB_V1.23...)
            inputSourceName: string
                Organization that owns original source data (e.g., ncem, ndbc, noaa, adcirc...)
            inputSourceArchive: string
                Where the original data source is archived (e.g., contrails, ndbc, noaa, renci...)
            inputSourceInstance: string
                Source instance, such as ncsc123_gfs_sb55.01. Used by getInputFiles, and addMeta.
            inputForcingMetaclass: string
                ADCIRC model forcing class, such as synoptic or tropical. Used by addMeta
        Returns
            None, runs getInputFiles(), and then addMeta() functions
    '''

    dfDirFiles = getInputFiles(inputDataSource, inputSourceName, inputSourceArchive, inputSourceInstance, inputForcingMetaclass) 
 
    for index, row in dfDirFiles.iterrows():
        harvestDir = row[0]
        inputFile = row[1] 

        addMeta(harvestDir, ingestPath, inputFile, inputDataSource, inputSourceName, inputSourceArchive, inputSourceInstance, inputForcingMetaclass)

@logger.catch
def main(args):
    ''' Main program function takes args as input, starts logger, runs processData, 
        Parameters
            args: dictionary
                contains the parameters listed below
            ingestPath: string
                Directory path to ingest data files, created from the harvest files, modelRunID subdirectory is included in this
                path.
            inputDataSource: string
                Unique identifier of data source (e.g., river_gauge, tidal_predictions, air_barameter, wind_anemometer, NAMFORECAST_NCSC_SAB_V1.23...)
            inputSourceName: string
                Organization that owns original source data (e.g., ncem, ndbc, noaa, adcirc...)
            inputSourceArchive: string
                Where the original data source is archived (e.g., contrails, ndbc, noaa, renci...)
            inputSourceInstance: string
                Source instance, such as ncsc123_gfs_sb55.01. Used by getFileMetaTimemark, getInputFiles, getSourceID, addMeta, and processData.
            inputForcingMetaclass: string
                ADCIRC model forcing class, such as synoptic or tropical. Used by addMeta, and processData.
        Returns
            None, runs processData() function
    '''
    # Add logger
    logger.remove()
    log_path = os.path.join(os.getenv('LOG_PATH', os.path.join(os.path.dirname(__file__), 'logs')), '')
    logger.add(log_path+'createIngestModelData.log', level='DEBUG')
    logger.add(sys.stdout, level="DEBUG")
    logger.add(sys.stderr, level="ERROR")

    # Extract args variables
    ingestPath = os.path.join(args.ingestPath, '')
    inputDataSource = args.inputDataSource
    inputSourceName = args.inputSourceName
    inputSourceArchive = args.inputSourceArchive
    inputSourceInstance = args.inputSourceInstance
    inputForcingMetaclass = args.inputForcingMetaclass


    logger.info('Start processing data from data source '+inputDataSource+', with source name '+inputSourceName+', from source archive '+inputSourceArchive+'.')
    processData(ingestPath, inputDataSource, inputSourceName, inputSourceArchive, inputSourceInstance, inputForcingMetaclass) 
    logger.info('Finished processing data from data source '+inputDataSource+', with source name '+inputSourceName+', from source archive '+inputSourceArchive+'.')

# Run main function takes ingestPath, inputDataSource, inputSourceName, inputSourceArchiv as input.
if __name__ == "__main__":
    ''' Takes argparse inputs and passes theme to the main function
        Parameters
            ingestPath: string
                Directory path to ingest data files, created from the harvest files, modelRunID subdirectory is included in this
                path.
            inputDataSource: string
                Unique identifier of data source (e.g., river_gauge, tidal_predictions, air_barameter, wind_anemometer, NAMFORECAST_NCSC_SAB_V1.23...)
            inputSourceName: string
                Organization that owns original source data (e.g., ncem, ndbc, noaa, adcirc...)
            inputSourceArchive: string
                Where the original data source is archived (e.g., contrails, ndbc, noaa, renci...)
            inputSourceInstance: string
                Source instance, such as ncsc123_gfs_sb55.01. Used by getFileMetaTimemark, getInputFiles, getSourceID, addMeta, and processData.
            inputForcingMetaclass: string
                ADCIRC model forcing class, such as synoptic or tropical. Used by addMeta, and processData.
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

    args = parser.parse_args()
    main(args)

