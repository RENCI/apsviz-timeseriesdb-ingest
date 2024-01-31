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

def getFileDateTime(inputFile):
    ''' Returns a DataFrame containing a list of directory paths, and files, from table drf_harvest_obs_file_meta, and weather they have been ingested.
        Parameters
            inputFile: string
                Name of input file
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
        cur.execute("""SELECT dir_path, file_name, ingested, overlap_past_file_date_time
                       FROM drf_harvest_obs_file_meta
                       WHERE file_name = %(input_file)s
                       ORDER BY file_name""",
                    {'input_file': inputFile})

        # convert query output to Pandas dataframe
        df = pd.DataFrame(cur.fetchall(), columns=['dir_path', 'file_name', 'ingested', 'overlap_past_file_date_time'])

        # Close cursor and database connection
        cur.close()
        conn.close()

        # Return DataFrame
        return(df)

    # If exception log error
    except (Exception, psycopg.DatabaseError) as error:
        logger.info(error)

def getOldHarvestFiles(inputDataSource, inputSourceName, inputSourceArchive, inputSourceVariable):
    ''' Returns a DataFrame containing a list of files, from table drf_harvest_obs_file_meta, with specified data source, source name,
        and source_archive that have been ingested.
        Parameters
            inputDataSource: string
                Unique identifier of data source (e.g., river_gauge, tidal_predictions, air_barameter, wind_anemometer, NAMFORECAST_NCSC_SAB_V1.23...)
            inputSourceName: string
                Organization that owns original source data (e.g., ncem, ndbc, noaa, adcirc...)
            inputSourceArchive: string
                Where the original data source is archived (e.g., contrails, ndbc, noaa, renci...)
            inputSourceVariable: string
                The variable that is being ingested (e.g., water_level, air_pressure, stream_elevation, wave_height...)
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
        cur.execute("""SELECT file_id, dir_path, file_name, processing_datetime, data_date_time, data_begin_time, data_end_time, data_source, source_name, 
                              source_archive, source_variable, location_type, timemark, ingested, overlap_past_file_date_time
                       FROM drf_harvest_obs_file_meta
                       WHERE data_source = %(datasource)s AND source_name = %(sourcename)s AND
                       source_archive = %(sourcearchive)s and source_variable = %(sourcevariable)s AND ingested = True""", 
                    {'datasource': inputDataSource, 'sourcename': inputSourceName, 'sourcearchive': inputSourceArchive, 
                     'sourcevariable': inputSourceVariable})
       
        # convert query output to Pandas dataframe 
        df = pd.DataFrame(cur.fetchall(), columns=['file_id', 'dir_path', 'file_name', 'processing_datetime', 'data_date_time', 'data_begin_time', 
                                                   'data_end_time', 'data_source', 'source_name', 'source_archive', 'source_variable', 'location_type', 
                                                   'timemark', 'ingested', 'overlap_past_file_date_time'])

        # Close cursor and database connection
        cur.close()
        conn.close()

        # Return DataFrame
        return(df)

    # If exception log error    
    except (Exception, psycopg.DatabaseError) as error:
        logger.info(error)

# This function takes as input the harvest directory path, data source, source name, source archive, and a file name prefix.
# It uses them to create a file list that is then ingested into the drf_harvest_obs_file_meta table, and used to ingest the
# data files. This function also returns first_time, and last_time which are used in cross checking the data.
def createFileList(harvestDir, inputDataSource, inputSourceName, inputSourceArchive, inputSourceVariable, 
                   inputFilenamePrefix, inputLocationType):
    ''' Returns a DataFrame containing a list of files, with meta-data, to be ingested in to table drf_harvest_obs_file_meta. It also returns
        first_time, and last_time used for cross checking.
        Parameters
            harvestDir: string
                Directory path to harvest data files
            ingestDir: string
                Directory path to ingest data files, created from the harvest files
            inputDataSource: string
                Unique identifier of data source (e.g., river_gauge, tidal_predictions, air_barameter, wind_anemometer...)
            inputSourceName: string
                Organization that owns original source data (e.g., ncem, ndbc, noaa...)
            inputSourceArchive: string
                Where the original data source is archived (e.g., contrails, ndbc, noaa...)
            inputSourceVariable: string
                The variable that is being ingested (e.g., water_level, air_pressure, stream_elevation, wave_height...)
            inputFilenamePrefix: string
                Prefix filename to data files that are being ingested. The prefix is used to search for the data files, using glob.
            inputLocationType: string
                The location type of the stations (e.g., tidal, coastal, ocean, river)
        Returns
            DataFrame, first_time, last_time
    '''

    # Search for files in the harvestDir that have inputDataset name in them, and generate a list of files found
    dirInputFiles = glob.glob(harvestDir+inputFilenamePrefix+"*.csv")

    # Define outputList variable
    outputList = []

    # Loop through dirOutputFiles, generate new variables and add them to outputList
    for dirInputFile in dirInputFiles:
        dir_path = dirInputFile.split(inputFilenamePrefix)[0]
        file_name = Path(dirInputFile).parts[-1] 

        datetimes = re.findall(r'(\d+-\d+-\d+T\d+:\d+:\d+)',file_name)
        timemark = datetimes[0]
        data_date_time = datetimes[0]
        processing_datetime = datetime.datetime.today().isoformat().split('.')[0]

        df = pd.read_csv(dirInputFile)
        data_begin_time = df['TIME'].min()
        data_end_time = df['TIME'].max()

        # This step checks to see if begin_time, and end_time or null, and if they are it marks the file as being ingested
        # This step was added to deal with some erroneous runs, and may be removed in the future. NO LONGER NEED THIS!
        if pd.isnull(data_begin_time) and pd.isnull(data_end_time):
            ingested = 'True'
        else:
            ingested = 'False'

        overlap_past_file_date_time = 'False'

        outputList.append([dir_path,file_name,processing_datetime,data_date_time,data_begin_time,data_end_time,inputDataSource,inputSourceName,
                           inputSourceArchive,inputSourceVariable,inputLocationType,timemark,ingested,overlap_past_file_date_time]) 

    # Convert outputList to a DataFrame
    dfnew = pd.DataFrame(outputList, columns=['dir_path', 'file_name', 'processing_datetime', 'data_date_time', 'data_begin_time', 'data_end_time', 
                                              'data_source', 'source_name', 'source_archve', 'source_variable', 'location_type', 'timemark', 
                                              'ingested','overlap_past_file_date_time'])

    # Get DataFrame of existing list of files, in the database, that have been ingested.
    dfold = getOldHarvestFiles(inputDataSource, inputSourceName, inputSourceArchive, inputSourceVariable)

    # Create DataFrame of list of current files that are not already ingested in table drf_harvest_obs_file_meta.
    df = dfnew.loc[~dfnew['file_name'].isin(dfold['file_name'])]

    if len(df.values) == 0:
        logger.info('No new files for data source '+inputDataSource+', with source name '+inputSourceName+', from the '+
                    inputSourceArchive+' archive, and the '+inputSourceVariable+' variable')
        first_time = np.nan
        last_time = np.nan
    else:
        logger.info('There are '+str(len(df.values))+' new files for data source '+inputDataSource+', with source name '+inputSourceName+
                    ', from the '+inputSourceArchive+' archive, and the '+inputSourceVariable+' variable')
        # Get first time, and last time from the list of files. This will be used in the filename, to enable checking for time overlap in files
        first_time = df['data_date_time'].iloc[0]
        last_time = df['data_date_time'].iloc[-1] 

    # Return DataFrame first time, and last time
    return(df, first_time, last_time)

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
            inputDataSource: string
                Unique identifier of data source (e.g., river_gauge, tidal_predictions, air_barameter, wind_anemometer...)
            inputSourceName: string
                Organization that owns original source data (e.g., ncem, ndbc, noaa...)
            inputSourceArchive: string
                Where the original data source is archived (e.g., contrails, ndbc, noaa...)
            inputSourceVariable: string
                The variable that is being ingested (e.g., water_level, air_pressure, stream_elevation, wave_height...)
            inputFilenamePrefix: string
                Prefix filename to data files that are being ingested. The prefix is used to search for the data files, using glob.
            inputLocationType: string
                The location type of the stations (e.g., tidal, coastal, ocean, river)
        Returns
            CSV file
    '''
    # Add logger
    logger.remove()
    log_path = os.path.join(os.getenv('LOG_PATH', os.path.join(os.path.dirname(__file__), 'logs')), '')
    logger.add(log_path+'createHarvestObsFileMeta.log', level='DEBUG')
    logger.add(sys.stdout, level="DEBUG")
    logger.add(sys.stderr, level="ERROR")

    # Extract args variables
    harvestDir = os.path.join(args.harvestDir, '')
    ingestDir = os.path.join(args.ingestDir, '')
    inputDataSource = args.inputDataSource
    inputSourceName = args.inputSourceName
    inputSourceArchive = args.inputSourceArchive
    inputSourceVariable = args.inputSourceVariable
    inputFilenamePrefix = args.inputFilenamePrefix
    inputLocationType = args.inputLocationType

    logger.info('Start processing source data for data source '+inputDataSource+', source name '+inputSourceName+', source archive '+
                inputSourceArchive+', and source variable '+inputSourceVariable+', with filename prefix '+inputFilenamePrefix+
                ', and location type '+inputLocationType+'.')

    # Get DataFrame file list, and time variables by running the createFileList function
    df, first_time, last_time = createFileList(harvestDir, inputDataSource, inputSourceName, inputSourceArchive, inputSourceVariable, 
                                               inputFilenamePrefix, inputLocationType)

    if pd.isnull(first_time) and pd.isnull(last_time):
        logger.info('No new files for data source '+inputDataSource+', source name '+inputSourceName+', source archive '+inputSourceArchive+
                    ', source variable '+inputSourceVariable+', and location type '+inputLocationType+'.')
    else:
        # Get current date    
        current_date = datetime.date.today().strftime("%b-%d-%Y")

        # Create output file name
        if inputSourceName == 'adcirc':
            outputFile = 'harvest_data_files_'+inputFilenamePrefix+'_'+first_time.strip()+'_'+last_time.strip()+'_'+current_date+'.csv'
        else:
            outputFile = 'harvest_data_files_'+inputSourceArchive+'_stationdata_'+inputDataSource+'_'+inputFilenamePrefix+'_'+first_time.strip()+'_'+last_time.strip()+'_'+current_date+'.csv'

        # Write DataFrame containing list of files to a csv file
        df.to_csv(ingestDir+outputFile, index=False, header=False)
        logger.info('Finished processing source data for data source '+inputDataSource+', source name '+inputSourceName+', source archive '+
                    inputSourceArchive+', and source variable '+inputSourceVariable+', with data filename prefix '+inputFilenamePrefix+
                    ', and location type '+inputLocationType+'.')

if __name__ == "__main__":
    ''' Takes argparse inputs and passes theme to the main function
        Parameters
            harvestDir: string
                Directory path to harvest data files
            ingestDir: string
                Directory path to ingest data files, created from the harvest files
            inputDataSource: string
                Unique identifier of data source (e.g., river_gauge, tidal_predictions, air_barameter, wind_anemometer...)
            inputSourceName: string
                Organization that owns original source data (e.g., ncem, ndbc, noaa...)
            inputSourceArchive: string
                Where the original data source is archived (e.g., contrails, ndbc, noaa...)
            inputSourceVariable: string
                The variable that is being ingested (e.g., water_level, air_pressure, stream_elevation, wave_height...)
            inputFilenamePrefix: string
                Prefix filename to data files that are being ingested. The prefix is used to search for the data files, using glob.
            inputLocationType: string
                The location type of the stations (e.g., tidal, coastal, ocean, river)
        Returns
            None
    '''

    parser = argparse.ArgumentParser()

    # Optional argument which requires a parameter (eg. -d test)
    parser.add_argument("--harvestDIR", "--harvestDir", help="Input directory path", action="store", dest="harvestDir", required=True)    
    parser.add_argument("--ingestDIR", "--ingestDir", help="Output directory path", action="store", dest="ingestDir", required=True)
    parser.add_argument("--inputDataSource", help="Input data source name", action="store", dest="inputDataSource", required=True)
    parser.add_argument("--inputSourceName", help="Input source name", action="store", dest="inputSourceName", choices=['adcirc','noaa','ndbc','ncem'], required=True)
    parser.add_argument("--inputSourceArchive", help="Input source archive name", action="store", dest="inputSourceArchive", required=True)
    parser.add_argument("--inputSourceVariable", help="Input source variable name", action="store", dest="inputSourceVariable", required=True)
    parser.add_argument("--inputFilenamePrefix", help="Input data filename prefix", action="store", dest="inputFilenamePrefix", required=True)
    parser.add_argument("--inputLocationType", help="Input location type name", action="store", dest="inputLocationType", required=True)

    # Parse input arguments
    args = parser.parse_args()

    # Run main
    main(args)

