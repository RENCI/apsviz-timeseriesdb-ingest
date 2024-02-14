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

def getOldRetainObsStationFiles(inputDataSource, inputSourceName, inputSourceArchive, inputLocationType):
    ''' Returns a DataFrame containing a list of files, from table drf_apsviz_station_file_meta, that have been ingested.
        Parameters
            inputDataSource: string
                Unique identifier of data source (e.g., river_gauge, tidal_predictions, air_barameter, wind_anemometer, NAMFORECAST_NCSC_SAB_V1.23...) 
            inputSourceName: string
                Organization that owns original source data (e.g., ncem, ndbc, noaa, adcirc...)
            inputSourceArchive: string
                Where the original data source is archived (e.g., contrails, ndbc, noaa, renci...)
            inputLocationType: string
                Gauge location type (COASTAL, TIDAL, or RIVERS). Used by ingestSourceMeta.
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
        cur.execute("""SELECT file_id, dir_path, file_name, data_source, source_name, source_archive, location_type, timemark, begin_date, end_date, ingested
                       FROM drf_retain_obs_station_file_meta
                       WHERE data_source = %(datasource)s AND source_name = %(sourcename)s AND
                       source_archive = %(sourcearchive)s AND location_type = %(locationtype)s AND ingested = True""", 
                    {'datasource': inputDataSource, 'sourcename': inputSourceName, 'sourcearchive': inputSourceArchive, 'locationtype': inputLocationType})
          
        # convert query output to Pandas dataframe 
        df = pd.DataFrame(cur.fetchall(), columns=['file_id', 'dir_path', 'file_name', 'data_source', 'source_name', 'source_archive', 'location_type', 
                                                   'timemark', 'begin_date','end_date', 'ingested'])

        # Close cursor and database connection
        cur.close()
        conn.close()

        # Return DataFrame
        return(df)

    # If exception log error    
    except (Exception, psycopg.DatabaseError) as error:
        logger.exception(error)

def createFileList(harvestDir,ingestDir,inputDataSource,inputSourceName,inputSourceArchive,inputLocationType,inputFilenamePrefix):
    ''' Returns a DataFrame containing a list of files, with meta-data, to be ingested in to table drf_retain_obs_station_file_meta. It also returns
        first_time, and last_time used for cross checking.
        Parameters
            harvestDir: string
                Directory path to harvest data files
            ingestDir: string
                Directory path to ingest data files, created from the harvest files
            inputDataSource: string
                Unique identifier of data source (e.g., river_gauge, tidal_predictions, air_barameter, wind_anemometer, NAMFORECAST_NCSC_SAB_V1.23...)
            inputSourceName: string
                Organization that owns original source data (e.g., ncem, ndbc, noaa, adcirc...)
            inputSourceArchive: string
                Where the original data source is archived (e.g., contrails, ndbc, noaa, renci...)
            inputLocationType: string
                Gauge location type (COASTAL, TIDAL, or RIVERS). Used by ingestSourceMeta.
            inputFilenamePrefix: string
                Input file name prefix
        Returns
            DataFrame, first_time, last_time
    '''

    # Search for files in the harvestDir that have inputDataset name in them, and generate a list of files found
    dirInputFiles = glob.glob(harvestDir+inputFilenamePrefix+"*.csv")

    if len(dirInputFiles) > 0:
        # Define outputList variable
        outputList = []

        # Define ingested as False
        ingested = False

        # Define timeMarkList to output timeMarks into
        timeMarkList = []

        # Loop through dirOutputFiles, generate new variables and add them to outputList
        for dirInputFile in dirInputFiles:
            dir_path = dirInputFile.split(inputFilenamePrefix)[0]
            file_name = Path(dirInputFile).parts[-1] 

            logger.info('Process file: '+file_name)
                       
            datetimes = re.findall(r'(\d+-\d+-\d+T\d+:\d+:\d+)',file_name)
            timeMark = datetimes[0]
            if len(timeMark) == 0:
                logger.info('Something is wrong for the timeMake from file: '+file_name)
            else:
                timeMarkList.append([timeMark])

            dirInputDataFile = "_".join(dirInputFile.split('_meta_'))
            logger.info('Get begin date, and end date from data file: '+dirInputDataFile)
            df = pd.read_csv(dirInputDataFile)
            beginDate = df['TIME'].min()
            endDate = df['TIME'].max()

            outputList.append([dir_path,file_name,inputDataSource,inputSourceName,inputSourceArchive,inputLocationType,timeMark,beginDate,endDate,ingested])

        # Convert outputList to a DataFrame
        dfnew = pd.DataFrame(outputList, columns=['dir_path','file_name','data_source','source_name','source_archve','location_type','timemark','begin_date','end_date','ingested'])

        # Get DataFrame of existing list of files, in the database, that have been ingested.
        dfold = getOldRetainObsStationFiles(inputDataSource, inputSourceName, inputSourceArchive, inputLocationType)

        # Create DataFrame of list of current files that are not already ingested in table drf_harvest_obs_file_meta.
        df = dfnew.loc[~dfnew['file_name'].isin(dfold['file_name'])]

        # Check to see if there are any files 
        if len(df.values) == 0:
            logger.info('No new files for data source '+inputDataSource+', with location type '+inputLocationType+', source name '+inputSourceName+', from the '+inputSourceArchive+' archive')
        else:
            logger.info('There are '+str(len(df.values))+' new files for data source '+inputDataSource+', with location type '+inputLocationType+', source name '+inputSourceName+', from the '+inputSourceArchive+' archive')

        # Return DataFrame first time, and last time
        return(df, timeMarkList)
    else:
        logger.info('There were no files with prefix: '+inputFilenamePrefix)
        df = 'NODATA'
        timeMarkList = 'NOTIMEMARKS'
        return(df,timeMarkList)

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
                Unique identifier of data source (e.g., river_gauge, tidal_predictions, air_barameter, wind_anemometer, NAMFORECAST_NCSC_SAB_V1.23...)
            inputSourceName: string
                Organization that owns original source data (e.g., ncem, ndbc, noaa, adcirc...)
            inputSourceArchive: string
                Where the original data source is archived (e.g., contrails, ndbc, noaa, renci...)
            inputLocationType: string
                Gauge location type (COASTAL, TIDAL, or RIVERS). Used by ingestSourceMeta.
            inputFilenamePrefix: string
                Input file name prefix
        Returns
            CSV file 
    '''

    # Add logger
    logger.remove()
    log_path = os.path.join(os.getenv('LOG_PATH', os.path.join(os.path.dirname(__file__), 'logs')), '')
    logger.add(log_path+'createRetainObsStationFileMeta.log', level='DEBUG')
    logger.add(sys.stdout, level="DEBUG")
    logger.add(sys.stderr, level="ERROR")

    # Extract args variables
    harvestDir = os.path.join(args.harvestDir, '')
    ingestDir = os.path.join(args.ingestDir, '')
    inputDataSource = args.inputDataSource
    inputSourceName = args.inputSourceName
    inputSourceArchive = args.inputSourceArchive
    inputLocationType = args.inputLocationType
    inputFilenamePrefix = args.inputFilenamePrefix

    logger.info('Start processing source station data for source '+inputDataSource+', source name '+inputSourceName+', and source archive '+inputSourceArchive+', with filename prefix '+inputFilenamePrefix+'.')

    # Get DataFrame file list, and time variables by running the createFileList function
    df, timeMarkList = createFileList(harvestDir,ingestDir,inputDataSource,inputSourceName,inputSourceArchive,inputLocationType,inputFilenamePrefix)

    if timeMarkList == 'NOTIMEMARKS':
        logger.info('No data or timeMark for file name prefix'+inputFilenamePrefix)
    else:
        logger.info('createFileList returned '+str(len(timeMarkList))+' timeMarks')

        # Get current date   
        current_date = datetime.date.today()

        # Create output file name
        outputFile = 'retain_obs_meta_files_'+inputFilenamePrefix+'_'+timeMarkList[0][0]+'_'+current_date.strftime("%b-%d-%Y")+'.csv'

        # Write DataFrame containing list of files to a csv file
        df.to_csv(ingestDir+outputFile, index=False, header=False)
        logger.info('Finished processing source station meta data for file '+outputFile+'.')

if __name__ == "__main__":
    ''' Takes argparse inputs and passes theme to the main function
        Parameters
            harvestDir: string
                Directory path to harvest data files
            ingestDir: string
                Directory path to ingest data files, created from the harvest files
            inputDataSource: string
                Unique identifier of data source (e.g., river_gauge, tidal_predictions, air_barameter, wind_anemometer, NAMFORECAST_NCSC_SAB_V1.23...)
            inputSourceName: string
                Organization that owns original source data (e.g., ncem, ndbc, noaa, adcirc...)
            inputSourceArchive: string
                Where the original data source is archived (e.g., contrails, ndbc, noaa, renci...)
            inputLocationType: string
                Gauge location type (COASTAL, TIDAL, or RIVERS). Used by ingestSourceMeta.
            inputFilenamePrefix: string
                Input file name prefix
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
    parser.add_argument("--inputLocationType", help="Input location type to be processed", action="store", dest="inputLocationType", required=True)
    parser.add_argument("--inputFilenamePrefix", help="Input file name prefix", action="store", dest="inputFilenamePrefix", required=True)

    # Parse input arguments
    args = parser.parse_args()

    # Run main
    main(args)

