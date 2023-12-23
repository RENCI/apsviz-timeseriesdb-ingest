#!/usr/bin/env python
# coding: utf-8

# Import python modules
import argparse
import glob
import sys
import os
import re
import datetime
import psycopg
import pandas as pd
import numpy as np
from pathlib import Path
from loguru import logger

def getOldApsVizStationFiles(inputDataSource, inputSourceName, inputSourceArchive, inputSourceInstance, inputForcingMetaclass, modelRunID, inputLocationType):
    ''' Returns a DataFrame containing a list of files, from table drf_apsviz_station_file_meta, that have been ingested.
        Parameters
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
            modelRunID: string
                Unique identifier of a model run. It combines the instance_id, and uid from asgs_dashboard db
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
        cur.execute("""SELECT file_id, dir_path, file_name, data_date_time, data_source, source_name, source_archive, grid_name, model_run_id, timemark, location_type, csvurl, ingested 
                       FROM drf_apsviz_station_file_meta
                       WHERE data_source = %(datasource)s AND source_name = %(sourcename)s AND
                       source_archive = %(sourcearchive)s AND source_instance = %(sourceinstance)s AND forcing_metaclass = %(forcingmetaclass)s AND 
                       ingested = True AND model_run_id = %(modelrunid)s AND location_type = %(locationtype)s""", 
                    {'datasource': inputDataSource, 'sourcename': inputSourceName, 'sourcearchive': inputSourceArchive, 'sourceinstance': inputSourceInstance, 
                     'forcingmetaclass': inputForcingMetaclass, 'modelrunid': modelRunID, 'locationtype': inputLocationType})
          
        # convert query output to Pandas dataframe 
        df = pd.DataFrame(cur.fetchall(), columns=['file_id', 'dir_path', 'file_name', 'data_date_time', 'data_source', 'source_name', 'source_archive', 'source_instance', 
                                                   'forcing_metaclass ', 'grid_name', 'model_run_id', 'timemark', 'location_type', 'csvurl', 'ingested'])

        # Close cursor and database connection
        cur.close()
        conn.close()

        # Return DataFrame
        return(df)

    # If exception log error    
    except (Exception, psycopg.DatabaseError) as error:
        logger.info(error)

def createFileList(harvestPath,inputFilename,dataDateTime,inputDataSource,inputSourceName,inputSourceArchive,inputSourceInstance,inputForcingMetaclass,gridName,modelRunID,timeMark,inputLocationType,csvURL):
    ''' Returns a DataFrame containing a list of files, with meta-data, to be ingested in to table drf_apsviz_station_file_meta. It also returns
        first_time, and last_time used for cross checking.
        Parameters
            harvestPath: string
                Directory path to harvest data files
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
            inputFilename: string
                The name of the input file
            gridName: string
                Name of grid being used in model run (e.g., ed95d, hsofs NCSC_SAB_v1.23...)
            modelRunID: string
                Unique identifier of a model run. It combines the instance_id, and uid from asgs_dashboard db
            timeMark: datatime
                Date and time of the beginning of the model run for forecast runs, and end of the model run for nowcast runs.
            inputLocationType: string
                Gauge location type (COASTAL, TIDAL, or RIVERS). Used by ingestSourceMeta.
            csvURL:
                URL to SQL function that queries db and returns CSV file of data
            dataDateTime:
                Date and time that represents the first date time in the input data file.
        Returns
            DataFrame, first_time, last_time
    '''

    # Define outputList variable
    outputList = []

    # Define ingested as False
    ingested = False

    # Append variables to output list.
    outputList.append([harvestPath,inputFilename,dataDateTime,inputDataSource,inputSourceName,inputSourceArchive,inputSourceInstance,inputForcingMetaclass,
                       gridName,modelRunID,timeMark,inputLocationType,csvURL,ingested])

    # Convert outputList to a DataFrame
    dfnew = pd.DataFrame(outputList, columns=['dir_path','file_name','data_date_time','data_source','source_name','source_archve','source_instance','forcing_metaclass',
                                              'grid_name','model_run_id','timemark','location_type','csv_url','ingested'])

    # Get DataFrame of existing list of files, in the database, that have been ingested.
    dfold = getOldApsVizStationFiles(inputDataSource, inputSourceName, inputSourceArchive, inputSourceInstance, inputForcingMetaclass, modelRunID, inputLocationType)

    # Create DataFrame of list of current files that are not already ingested in table drf_harvest_obs_file_meta and drf_harvest_model_file_meta.
    df = dfnew.loc[~dfnew['file_name'].isin(dfold['file_name'])]

    if len(df.values) == 0:
        logger.info('No new files for data source '+inputDataSource+', with source name '+inputSourceName+', from the '+inputSourceArchive+' archive, with model run ID '+
                    modelRunID+' and location type'+inputLocationType+'.')
        first_time = np.nan
        last_time = np.nan
    else:
        logger.info('There are '+str(len(df.values))+' new files for data source '+inputDataSource+', with source name '+inputSourceName+', from the '+
                    inputSourceArchive+' archive, with model run ID '+modelRunID+' and location type'+inputLocationType+'.')
        # Get first time, and last time from the list of files. This will be used in the filename, to enable checking for time overlap in files
        first_time = df['data_date_time'].iloc[0]
        last_time = df['data_date_time'].iloc[-1] 

    # Return DataFrame first time, and last time
    return(df, first_time, last_time)

@logger.catch
def main(args):
    ''' Main program function takes args as input, starts logger, runs createFileList, and writes output to CSV file. 
        The CSV file will be ingest into table drf_apsviz_station_file_meta during runHarvestFile() is run in runModelIngest.py
        Parameters
            args: dictionary 
                contains the parameters listed below
            harvestPath: string
                Directory path to harvest data files
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
                Source instance, such as ncsc123_gfs_sb55.01. Used by ingestSourceMeta, and ingestData.
            inputForcingMetaclass: string
                ADCIRC model forcing class, such as synoptic or tropical. Used by ingestSourceMeta, and ingestData.
            inputFilename: string
                The name of the input file
            gridName: string
                Name of grid being used in model run (e.g., ed95d, hsofs NCSC_SAB_v1.23...)
            modelRunID: string
                Unique identifier of a model run. It combines the instance_id, and uid from asgs_dashboard db
            timeMark: datatime
                Date and time of the beginning of the model run for forecast runs, and end of the model run for nowcast runs.
            inputLocationType: string
                Gauge location type (COASTAL, TIDAL, or RIVERS). Used by ingestSourceMeta.
            csvURL:
                URL to SQL function that queries db and returns CSV file of data
            dataDateTime:
                Date and time that represents the first date time in the input data file.
        Returns
            CSV file 
    '''

    # Add logger
    logger.remove()
    log_path = os.path.join(os.getenv('LOG_PATH', os.path.join(os.path.dirname(__file__), 'logs')), '')
    logger.add(log_path+'createApsVizStationFileMeta.log', level='DEBUG')
    logger.add(sys.stdout, level="DEBUG")
    logger.add(sys.stderr, level="ERROR")

    # Extract args variables
    harvestPath = os.path.join(args.harvestPath, '')
    ingestPath = os.path.join(args.ingestPath, '')
    inputDataSource = args.inputDataSource
    inputSourceName = args.inputSourceName
    inputSourceArchive = args.inputSourceArchive
    inputSourceInstance = args.inputSourceInstance
    inputForcingMetaclass = args.inputForcingMetaclass
    inputFilename = args.inputFilename
    gridName = args.gridName
    modelRunID = args.modelRunID
    timeMark = args.timeMark
    inputLocationType = args.inputLocationType
    csvURL = args.csvURL
    dataDateTime = args.dataDateTime

    logger.info('Start processing source station data for source '+inputDataSource+', source name '+inputSourceName+', and source archive '+inputSourceArchive+', with filename prefix '+inputFilename+'.')

    # Get DataFrame file list, and time variables by running the createFileList function
    df, first_time, last_time = createFileList(harvestPath,inputFilename,dataDateTime,inputDataSource,inputSourceName,inputSourceArchive,inputSourceInstance,inputForcingMetaclass,gridName,modelRunID,timeMark,inputLocationType,csvURL)

    if pd.isnull(first_time) and pd.isnull(last_time):
        logger.info('No new files for station meta data source '+inputDataSource+', source name '+inputSourceName+', source archive '+inputSourceArchive+', and location type '+inputLocationType+'.')
    else:
        # Get current date    
        current_date = datetime.date.today()

        # Create output file name
        outputFile = 'harvest_meta_files_'+inputFilename

        # Write DataFrame containing list of files to a csv file
        df.to_csv(ingestPath+outputFile, index=False, header=False)
        logger.info('Finished processing source station meta data for file '+inputFilename+' with model run ID '+modelRunID+'.')

if __name__ == "__main__":
    ''' Takes argparse inputs and passes theme to the main function
        Parameters
            harvestPath: string
                Directory path to harvest data files
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
                Source instance, such as ncsc123_gfs_sb55.01. Used by ingestSourceMeta, and ingestData.
            inputForcingMetaclass: string
                ADCIRC model forcing class, such as synoptic or tropical. Used by ingestSourceMeta, and ingestData.
            inputFilename: string
                The name of the input file
            gridName: string
                Name of grid being used in model run (e.g., ed95d, hsofs NCSC_SAB_v1.23...) 
            modelRunID: string
                Unique identifier of a model run. It combines the instance_id, and uid from asgs_dashboard db
            timeMark: datatime
                Date and time of the beginning of the model run for forecast runs, and end of the model run for nowcast runs.
            inputLocationType: string
                Gauge location type (COASTAL, TIDAL, or RIVERS). Used by ingestSourceMeta.
            csvURL:
                URL to SQL function that queries db and returns CSV file of data
            dataDateTime:
                Date and time that represents the first date time in the input data file.
        Returns
            None
    '''
    parser = argparse.ArgumentParser()

    # Optional argument which requires a parameter (eg. -d test)
    parser.add_argument("--harvestDIR", "--harvestPath", help="Input directory path", action="store", dest="harvestPath", required=True)    
    parser.add_argument("--ingestPath", "--ingestPath", help="Ingest directory path, including the modelRunID", action="store", dest="ingestPath", required=True)
    parser.add_argument("--inputDataSource", help="Input data source name", action="store", dest="inputDataSource", required=True)
    parser.add_argument("--inputSourceName", help="Input source name", action="store", dest="inputSourceName", choices=['adcirc','noaa','ndbc','ncem'], required=True)
    parser.add_argument("--inputSourceArchive", help="Input source archive name", action="store", dest="inputSourceArchive", required=True)
    parser.add_argument("--inputSourceInstance", help="Input source variables", action="store", dest="inputSourceInstance", required=True)
    parser.add_argument("--inputForcingMetaclass", help="Input forcing metaclass", action="store", dest="inputForcingMetaclass", required=True)
    parser.add_argument("--inputFilename", help="Input file name", action="store", dest="inputFilename", required=True)
    parser.add_argument("--gridName", help="Name of grid being used in model run", action="store", dest="gridName", required=True)
    parser.add_argument("--modelRunID", help="Input model run ID", action="store", dest="modelRunID", required=True)
    parser.add_argument("--timeMark", help="Timemark value for model run ID", action="store", dest="timeMark", required=True)
    parser.add_argument("--inputLocationType", help="Input location type to be processed", action="store", dest="inputLocationType", required=True)
    parser.add_argument("--csvURL", help="Input URL to get CSV output file", action="store", dest="csvURL", required=True)
    parser.add_argument("--dataDateTime", help="Data date time from input harvest file", action="store", dest="dataDateTime", required=True)

    # Parse input arguments
    args = parser.parse_args()

    # Run main
    main(args)

